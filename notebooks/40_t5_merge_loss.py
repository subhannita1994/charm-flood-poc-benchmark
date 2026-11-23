# Databricks notebook source
# MAGIC %md
# MAGIC # T5 — Simulation merge (50K-year event join) + loss aggregation
# MAGIC
# MAGIC | variant | join | code |
# MAGIC |---|---|---|
# MAGIC | `A_upstream` | `FloodSimulationMerge.transform()` — explode 7 RP knots, **broadcast the exposure side**, inner join on (region, RP) | **verbatim** |
# MAGIC | `B_broadcast_events` | same, broadcast hint moved to the (filtered) events side | Databricks proposal |
# MAGIC | `C_aqe_no_hint` | same, no hint — AQE picks | Databricks proposal |
# MAGIC | `D_clustered_events` | C + events read from a liquid-clustered (by `flood_region`) table | Databricks proposal |
# MAGIC
# MAGIC Loss aggregation after the merge is the stand-in `aggregate_losses` (A6) for every variant.
# MAGIC Photon on/off is a property of the job cluster (compute JSON), not of the code.
# MAGIC Input = 1M book with regions from native ST join and **synthetic** PSS damage ratios (stated: values random, sizes real).

# COMMAND ----------

dbutils.widgets.text("catalog", "catalog_8as5lj_bvv7nu")
dbutils.widgets.text("schema", "charm_poc")
dbutils.widgets.text("compute", "{}")
dbutils.widgets.text("job_run_id", "")
dbutils.widgets.text("task_run_id", "")
dbutils.widgets.text("sizes", "100000,1000000")
dbutils.widgets.text("variants", "A_upstream,B_broadcast_events,C_aqe_no_hint,D_clustered_events")
dbutils.widgets.text("budget_s", "1200")

import json, os
C, S = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
os.environ.setdefault("CHARM_DATA_ROOT", f"/Volumes/{C}/{S}/raw/")
from pyspark.sql import functions as F
from charm_bench.bench import Recorder, materialize
from charm_bench.prep import book_table
from charm_bench.variants import aggregate_losses, make_fixed_merge
from cgl_dbx_flood_pipeline.config import GLOBAL_CONFIG, PATHS
from cgl_dbx_flood_pipeline.flood_simulation_merge import FloodSimulationMerge   # VERBATIM

rec = Recorder(spark, f"{C}.{S}.benchmark_results", compute=json.loads(dbutils.widgets.get("compute")),
               job_run_id=dbutils.widgets.get("job_run_id"), task_run_id=dbutils.widgets.get("task_run_id"),
               workspace="azure-classic-fevm", code_fidelity="verbatim+standin")
print(PATHS.fluvial_simulation, PATHS.coastal_simulation)

# COMMAND ----------

INPUT = f"{C}.{S}.merge_input_1m"
if not spark.catalog.tableExists(INPUT):
    pts = spark.table(book_table(C, S, 1_000_000)).filter("latitudeinternal BETWEEN 41 AND 70 AND longitudeinternal BETWEEN -141 AND -52")
    pts.createOrReplaceTempView("b")
    regions = spark.sql(f"""
      WITH p AS (SELECT item_id_m AS item_id, st_point(longitudeinternal, latitudeinternal, 4326) g FROM b),
      f AS (SELECT /*+ BROADCAST(r) */ p.item_id, min(r.NUMBER) fluvial_region FROM p
            LEFT JOIN {C}.{S}.silver_fluvial_regions r ON st_intersects(p.g, st_geomfromwkb(r.geometry_wkb, 4326)) GROUP BY p.item_id),
      c AS (SELECT /*+ BROADCAST(r) */ p.item_id, min(r.NUMBER) coastal_region FROM p
            LEFT JOIN {C}.{S}.silver_coastal_regions r ON st_intersects(p.g, st_geomfromwkb(r.geometry_wkb, 4326)) GROUP BY p.item_id)
      SELECT item_id, fluvial_region, coastal_region FROM f JOIN c USING (item_id)""")
    cols = [F.lit("v1_1").alias("pss_version")]
    for i, rp in enumerate(GLOBAL_CONFIG.return_periods):
        for j, fld in enumerate(("fluvialBuildingDamageRatios", "fluvialContentsDamageRatios", "stormSurgeBuildingDamageRatios",
                                 "stormSurgeContentsDamageRatios")):
            cols.append((F.rand(100 + i * 10 + j) * (0.05 + 0.04 * i)).alias(f"{fld}_{rp}"))
        for j, fld in enumerate(("fluvialDefenseEfficiencies", "stormSurgeDefenseEfficiencies")):
            cols.append(F.when(F.rand(200 + i * 10 + j) < 0.2, F.lit(1.0)).otherwise(F.lit(0.0)).alias(f"{fld}_{rp}"))
    fin = spark.table(book_table(C, S, 1_000_000)).select(F.col("item_id_m").alias("item_id"), "building_value", "content_value",
                                                          "flood_deductible", "flood_limit")
    regions.select("*", *cols).join(fin, "item_id").write.mode("overwrite").saveAsTable(INPUT)
    spark.sql(f"""CREATE OR REPLACE TABLE {C}.{S}.fluvial_events_clustered CLUSTER BY (flood_region) AS
                  SELECT * FROM delta.`{PATHS.fluvial_simulation}`""")
    spark.sql(f"""CREATE OR REPLACE TABLE {C}.{S}.coastal_events_clustered CLUSTER BY (flood_region) AS
                  SELECT * FROM delta.`{PATHS.coastal_simulation}`""")
    spark.sql(f"OPTIMIZE {C}.{S}.fluvial_events_clustered"); spark.sql(f"OPTIMIZE {C}.{S}.coastal_events_clustered")

# COMMAND ----------

def clustered_cls():
    base = make_fixed_merge(FloodSimulationMerge, mode="aqe")
    class ClusteredEventsMerge(base):
        def load_regions(self, flv_delta, costal_delta):
            super().load_regions(flv_delta, costal_delta)  # builds the same filtered/stair-stepped selection
            rebuild = lambda tbl: (spark.table(tbl).filter(F.col("return_period") >= GLOBAL_CONFIG.return_periods[0])
                                   .withColumn("return_period", F.floor(F.col("return_period")).cast("int"))
                                   .select("flood_region", F.col("simulated_year").cast("long").alias("simulated_year"), "event_id",
                                           F.col("return_period").cast("int").alias("return_period_original"),
                                           self.return_period_stair().alias("return_period")))
            self.fluvial_events = rebuild(f"{C}.{S}.fluvial_events_clustered")
            self.coastal_events = rebuild(f"{C}.{S}.coastal_events_clustered")
            return self
    return ClusteredEventsMerge

CLASSES = {"A_upstream": FloodSimulationMerge, "B_broadcast_events": make_fixed_merge(FloodSimulationMerge, "broadcast_events"),
           "C_aqe_no_hint": make_fixed_merge(FloodSimulationMerge, "aqe"), "D_clustered_events": clustered_cls()}

results = {}
for v in dbutils.widgets.get("variants").split(","):
    for n in [int(x) for x in dbutils.widgets.get("sizes").split(",")]:
        inp = spark.table(INPUT).filter(F.col("item_id") < f"CGIC{n:08d}")
        rows = inp.count()
        try:
            with rec.time("T5", v, "merge_plus_loss", rows, recs="#3,#7", params={"n_book": n}) as r:
                m = CLASSES[v](table=inp, as_of_date="2026-06-30", run_name="t5").transform(
                    fluvial_simulation_repo=PATHS.fluvial_simulation, coastal_simulation_repo=PATHS.coastal_simulation)
                losses = aggregate_losses(m.fluvial_table, m.coastal_table, inp)
                materialize(losses)
            results[f"{v}_{n}"] = rec.last_seconds
            with rec.time("T5", v, "merge_only_row_count", rows, recs="#3,#7", params={"n_book": n}) as r:
                r["notes"] = f"fluvial_rows={m.fluvial_table.count()} coastal_rows={m.coastal_table.count()}"
        except Exception as e:  # noqa: BLE001 — e.g. broadcast of the exposure side exceeding limits
            results[f"{v}_{n}"] = f"FAILED {type(e).__name__}: {str(e)[:200]}"
            print(v, n, results[f"{v}_{n}"])
            break
        if rec.last_seconds > float(dbutils.widgets.get("budget_s")):
            break

dbutils.notebook.exit(json.dumps(results, default=str))
