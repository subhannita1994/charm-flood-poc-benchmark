# Databricks notebook source
# MAGIC %md
# MAGIC # T2a — Local-library PSS throughput (their benchmarked path; recs #1, #5, #7)
# MAGIC
# MAGIC Calls **their `score_with_pss` verbatim** (`geomap_scorer/score_pss.py`) against the `flood_model_serving`
# MAGIC stand-in (A4). The compute shape and the asset location come from the job's cluster spec / env:
# MAGIC `FMS_ASSETS_ROOT` = UC Volume (FUSE) or local disk staged by init script.
# MAGIC Sizes run ascending; any size projected to exceed `budget_s` is **not run** and is written as `extrapolated` (A8).

# COMMAND ----------

dbutils.widgets.text("catalog", "catalog_8as5lj_bvv7nu")
dbutils.widgets.text("schema", "charm_poc")
dbutils.widgets.text("compute", "{}")
dbutils.widgets.text("job_run_id", "")
dbutils.widgets.text("task_run_id", "")
dbutils.widgets.text("variant", "A_fuse_defaults")
dbutils.widgets.text("sizes", "10,100,1000,10000")
dbutils.widgets.text("extrapolate_to", "1000000")
dbutils.widgets.text("budget_s", "900")
dbutils.widgets.text("endpoint_scope", "worker")        # upstream default
dbutils.widgets.text("partitions_per_core", "2")       # upstream default
dbutils.widgets.text("max_threads_per_task", "16")     # upstream default
dbutils.widgets.text("max_records_per_batch", "1000")  # upstream default
dbutils.widgets.text("warm_repeat", "true")

import json, os
os.environ.setdefault("CHARM_DATA_ROOT", f"/Volumes/{dbutils.widgets.get('catalog')}/{dbutils.widgets.get('schema')}/raw/")  # before importing their package (PATHS is module-level), os
from loguru import logger
from charm_bench.bench import Recorder, materialize
from charm_bench.prep import book_table, project, pss_input
from cgl_dbx_flood_pipeline.geomap_scorer.score_pss import score_with_pss   # VERBATIM upstream

W = {k: dbutils.widgets.get(k) for k in ("catalog", "schema", "variant", "sizes", "extrapolate_to", "budget_s",
                                          "endpoint_scope", "partitions_per_core", "max_threads_per_task",
                                          "max_records_per_batch", "warm_repeat")}
compute = json.loads(dbutils.widgets.get("compute")); compute["FMS_ASSETS_ROOT"] = os.environ.get("FMS_ASSETS_ROOT", "")
rec = Recorder(spark, f"{W['catalog']}.{W['schema']}.benchmark_results", compute=compute,
               job_run_id=dbutils.widgets.get("job_run_id"), task_run_id=dbutils.widgets.get("task_run_id"),
               workspace="azure-classic-fevm", code_fidelity="verbatim+standin")
captured = []
logger.add(lambda m: captured.append(str(m)), level="INFO")
params = {k: W[k] for k in ("endpoint_scope", "partitions_per_core", "max_threads_per_task", "max_records_per_batch")}
print(W, compute)

# COMMAND ----------

def run_once(n: int, repeat: int) -> float:
    df = pss_input(spark.table(book_table(W["catalog"], W["schema"], n)))
    rows = df.count()
    with rec.time("T2a", W["variant"], "score_with_pss", rows, recs="#1,#5,#7",
                  params={**params, "n_book": n, "repeat": repeat}, notes="cold" if repeat == 0 else "warm workers") as r:
        out = score_with_pss(df, index_col="item_id", cie="CGIC", product="home", pss_endpoint_version="v1_1",
                             max_records_per_batch=int(W["max_records_per_batch"]),
                             partitions_per_core=int(W["partitions_per_core"]),
                             endpoint_scope=W["endpoint_scope"], max_threads_per_task=int(W["max_threads_per_task"]))
        materialize(out)
        r["notes"] += " | " + next((c.strip() for c in reversed(captured) if "threads_per_task" in c), "")[-300:]
    return rec.last_seconds

sizes = [int(s) for s in W["sizes"].split(",")]
budget = float(W["budget_s"]); measured = []
for i, n in enumerate(sizes):
    proj = project(measured, n) if measured else 0.0
    if proj > budget:
        print(f"skip n={n}: projected {proj:.0f}s > budget {budget:.0f}s")
        break
    try:
        t = run_once(n, 0); measured.append((n, t))
    except Exception as e:  # noqa: BLE001 — e.g. serverless blocks the arrow conf their code sets
        print("FAILED:", type(e).__name__, str(e)[:300])
        dbutils.notebook.exit(json.dumps({"failed": f"{type(e).__name__}: {str(e)[:300]}"}))
    if i == 0 and W["warm_repeat"] == "true":
        run_once(n, 1)  # same size again: workers + endpoints already initialised

# COMMAND ----------

# Extrapolations for every size not measured, plus the full book (A8)
for n in [s for s in sizes if s not in dict(measured)] + [int(W["extrapolate_to"])]:
    rec.add("T2a", W["variant"], "score_with_pss", n, project(measured, n), recs="#1,#5,#7", measure="extrapolated",
            params={**params, "from_points": measured}, notes="affine fit t=a+b*n on last 2 measured sizes")
rec.flush()
dbutils.notebook.exit(json.dumps({"measured": measured}))
