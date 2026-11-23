# Databricks notebook source
# MAGIC %md
# MAGIC # T2b — HTTP-API PSS throughput (PR18 target state; rec #7 parallelism)
# MAGIC
# MAGIC The hosted CGIC flood API is replaced by an **in-process stub** (A5): their `CGICHomeModelAPI` request building and
# MAGIC `parse_response` run verbatim, only the round trip is a sleep of `latency_ms` (default 49 ms = VM 48.7 s / 1k rows).
# MAGIC
# MAGIC | variant | execution code |
# MAGIC |---|---|
# MAGIC | `upstream` | `api/execution.py::score_with_model_api` as uploaded (no repartition, no retry, 8 threads/partition) |
# MAGIC | `scratch` | scratch notebook version (`repartition(max_workers*2)`, retry/backoff) — verbatim logic |
# MAGIC | `tuned_p{P}_t{T}` | Databricks proposal: `P` partitions (tied to cores), `T` threads/task, persistent pool |

# COMMAND ----------

dbutils.widgets.text("catalog", "catalog_8as5lj_bvv7nu")
dbutils.widgets.text("schema", "charm_poc")
dbutils.widgets.text("compute", "{}")
dbutils.widgets.text("job_run_id", "")
dbutils.widgets.text("task_run_id", "")
dbutils.widgets.text("total_cores", "8")              # worker cores of the job cluster (orchestrator passes it)
dbutils.widgets.text("latency_ms", "49")
dbutils.widgets.text("sizes", "1000,10000,100000")
dbutils.widgets.text("variants", "upstream,scratch,tuned")
dbutils.widgets.text("tuned_partition_factors", "2,4")  # partitions = total_cores * factor
dbutils.widgets.text("tuned_threads", "8,32")
dbutils.widgets.text("budget_s", "900")
dbutils.widgets.text("fault_test", "true")
dbutils.widgets.text("save_scored_1m", "false")

import json, os
os.environ.setdefault("CHARM_DATA_ROOT", f"/Volumes/{dbutils.widgets.get('catalog')}/{dbutils.widgets.get('schema')}/raw/")  # before importing their package (PATHS is module-level)
from charm_bench.bench import Recorder, materialize
from charm_bench.http_stub import StubbedCGICHomeModelAPI
from charm_bench.prep import book_table, project, pss_input
from charm_bench.variants import scratch_score_with_model_api, tuned_score_with_model_api
from cgl_dbx_flood_pipeline.api.execution import score_with_model_api as upstream_score   # VERBATIM

W = {k: dbutils.widgets.get(k) for k in ("catalog", "schema", "total_cores", "latency_ms", "sizes", "variants",
                                          "tuned_partition_factors", "tuned_threads", "budget_s", "fault_test", "save_scored_1m")}
CORES, LAT = int(W["total_cores"]), float(W["latency_ms"])
rec = Recorder(spark, f"{W['catalog']}.{W['schema']}.benchmark_results", compute=json.loads(dbutils.widgets.get("compute")),
               job_run_id=dbutils.widgets.get("job_run_id"), task_run_id=dbutils.widgets.get("task_run_id"),
               workspace="azure-classic-fevm", code_fidelity="verbatim+stub")
sizes = [int(s) for s in W["sizes"].split(",")]

def build_variants():
    out = []
    for v in W["variants"].split(","):
        if v == "upstream":
            out.append(("upstream", {}, lambda df, api: upstream_score(df, index_col="item_id", effective_date="2026-06-30", model_api=api)))
        elif v == "scratch":
            out.append(("scratch", {}, lambda df, api: scratch_score_with_model_api(df, index_col="item_id", effective_date="2026-06-30", model_api=api)))
        elif v == "tuned":
            for f in [int(x) for x in W["tuned_partition_factors"].split(",")]:
                for t in [int(x) for x in W["tuned_threads"].split(",")]:
                    p = CORES * f
                    out.append((f"tuned_p{p}_t{t}", {"num_partitions": p, "threads_per_task": t},
                                lambda df, api, p=p, t=t: tuned_score_with_model_api(df, index_col="item_id", effective_date="2026-06-30",
                                                                                      model_api=api, num_partitions=p, threads_per_task=t)))
    return out

# COMMAND ----------

api = StubbedCGICHomeModelAPI(latency_ms=LAT)
results = {}
for name, vparams, fn in build_variants():
    measured = []
    for n in sizes:
        if measured and project(measured, n) > float(W["budget_s"]):
            print(f"{name}: skip n={n} (projected {project(measured, n):.0f}s)"); break
        df = pss_input(spark.table(book_table(W["catalog"], W["schema"], n)))
        rows = df.count()
        try:
            with rec.time("T2b", name, "score_api", rows, recs="#7", params={**vparams, "latency_ms": LAT, "n_book": n, "total_cores": CORES}):
                materialize(fn(df, api))
        except Exception as e:  # noqa: BLE001 — incompatibility is the finding (row already recorded ok=false)
            print(name, "FAILED:", type(e).__name__, str(e)[:200]); break
        measured.append((rows, rec.last_seconds))
    for n in ([s for s in sizes if s not in [m[0] for m in measured]] + [1_000_000]) if measured else []:
        rec.add("T2b", name, "score_api", n, project(measured, n), recs="#7", measure="extrapolated",
                params={**vparams, "latency_ms": LAT, "from_points": measured}, notes="affine fit on last 2 measured sizes")
    results[name] = measured

# COMMAND ----------

# MAGIC %md ### Robustness: 0.5% injected HTTP 503s (upstream has no retry; scratch/tuned retry)

# COMMAND ----------

if W["fault_test"] == "true":
    faulty = StubbedCGICHomeModelAPI(latency_ms=LAT, fault_rate=0.005)
    df = pss_input(spark.table(book_table(W["catalog"], W["schema"], 10_000)))
    for name, vparams, fn in build_variants()[:3]:
        try:
            with rec.time("T2b_fault", name, "score_api_with_0.5pct_503", df.count(), recs="#7", params={**vparams, "fault_rate": 0.005}):
                out = fn(df, faulty)
                materialize(out)
                if "scoring_error" in out.columns:
                    errs = out.filter("scoring_error IS NOT NULL").count()
                    print(name, "rows with scoring_error after retries:", errs)
        except Exception as e:  # noqa: BLE001 — failure is the finding
            print(name, "FAILED:", type(e).__name__, str(e)[:200])

# COMMAND ----------

if W["save_scored_1m"] == "true":  # PSS-scored 1M book reused by T4/T5 (stub values)
    df = pss_input(spark.table(book_table(W["catalog"], W["schema"], 1_000_000)))
    best = tuned_score_with_model_api(df, index_col="item_id", effective_date="2026-06-30", model_api=api,
                                      num_partitions=CORES * 4, threads_per_task=32)
    with rec.time("T2b", "tuned_save_1m", "score_api_and_write_delta", df.count(), recs="#7"):
        best.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{W['catalog']}.{W['schema']}.pss_scored_1m")

dbutils.notebook.exit(json.dumps(results))
