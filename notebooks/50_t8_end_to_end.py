# Databricks notebook source
# MAGIC %md
# MAGIC # T8 — End-to-end pipeline (calibration vs their sheet + tuned target state)
# MAGIC
# MAGIC Stages mirror their benchmark sheet rows: **Score with Preprocessor**, **Score with PSS** (GeomapScorer: regions + PSS),
# MAGIC **Loss scoring** (FloodSimulationMerge + loss stand-in A6).
# MAGIC
# MAGIC * `baseline` — all stages verbatim: shapefiles read from Volume, PSS assets on Volume (cluster env), upstream merge.
# MAGIC * `tuned` — preprocessor verbatim; regions from silver Delta (rec #2); PSS verbatim code with assets staged locally (rec #1,
# MAGIC   cluster init script) and partitions/threads sized to the cluster (rec #7); merge with events broadcast (T5 winner).

# COMMAND ----------

dbutils.widgets.text("catalog", "catalog_8as5lj_bvv7nu")
dbutils.widgets.text("schema", "charm_poc")
dbutils.widgets.text("compute", "{}")
dbutils.widgets.text("job_run_id", "")
dbutils.widgets.text("task_run_id", "")
dbutils.widgets.text("mode", "baseline")
dbutils.widgets.text("sizes", "10,100,1000")
dbutils.widgets.text("budget_s", "900")
dbutils.widgets.text("partitions_per_core", "2")
dbutils.widgets.text("max_threads_per_task", "16")

import json, os
C, S = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
os.environ.setdefault("CHARM_DATA_ROOT", f"/Volumes/{C}/{S}/raw/")
MODE = dbutils.widgets.get("mode")
from pyspark.sql import functions as F
from charm_bench.bench import Recorder, materialize
from charm_bench.prep import book_table, project
from charm_bench.variants import aggregate_losses, make_fixed_merge
from cgl_dbx_flood_pipeline.config import PATHS
from cgl_dbx_flood_pipeline.preprocessor import FloodInForcePreprocessor, FloodInForceHomeCGICRules   # VERBATIM
from cgl_dbx_flood_pipeline.geomap_scorer import GeomapScorer, GeomapScorerCGICHomeRules             # VERBATIM
from cgl_dbx_flood_pipeline.geomap_scorer import geomap_scorer as gs_mod
from cgl_dbx_flood_pipeline.flood_simulation_merge import FloodSimulationMerge                         # VERBATIM
from cgl_dbx_flood_pipeline.sources.cgic_home import HOME_CGIC_CONFIG
try:
    from sedona.spark import SedonaContext
    spark = SedonaContext.create(spark)
except Exception as e:  # noqa: BLE001
    print("Sedona context:", e)

rec = Recorder(spark, f"{C}.{S}.benchmark_results", compute=json.loads(dbutils.widgets.get("compute")) | {"FMS_ASSETS_ROOT": os.environ.get("FMS_ASSETS_ROOT", "")},
               job_run_id=dbutils.widgets.get("job_run_id"), task_run_id=dbutils.widgets.get("task_run_id"),
               workspace="azure-classic-fevm", code_fidelity="verbatim+standin+reconstructed")

if MODE == "tuned":  # rec #7: thread/partition sizing passed to their score_with_pss through its own parameters
    _orig = gs_mod.score_with_pss
    def _sized(*a, **k):
        return _orig(*a, **k, partitions_per_core=int(dbutils.widgets.get("partitions_per_core")),
                     max_threads_per_task=int(dbutils.widgets.get("max_threads_per_task")))
    gs_mod.score_with_pss = _sized

def regions(name, path):
    if MODE == "baseline":
        return path                       # their default: .shp path -> GeoPandas on driver
    return (spark.table(f"{C}.{S}.silver_{name}_regions")
            .select("NUMBER", F.expr("ST_GeomFromWKB(geometry_wkb)").alias("geometry")))

MergeCls = FloodSimulationMerge if MODE == "baseline" else make_fixed_merge(FloodSimulationMerge, "broadcast_events")

# COMMAND ----------

def run(n):
    raw = spark.table(book_table(C, S, n))
    tag = {"n_book": n, "mode": MODE}
    with rec.time("T8", MODE, "1_preprocessor", n, recs="calibration", params=tag):
        pre = FloodInForcePreprocessor(table=raw, rules=FloodInForceHomeCGICRules(), as_of_date="2026-06-30", run_name="t8").transform(
            province_shp_path=PATHS.province_shp_path, custom_deductible=HOME_CGIC_CONFIG.default_deductible)
        pre_tbl = f"{C}.{S}.t8_pre_{MODE}"
        pre.table.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(pre_tbl)
    pre_df = spark.table(pre_tbl)
    with rec.time("T8", MODE, "2_geomap_regions_and_pss", n, recs="#1,#2,#7", params=tag):
        g = GeomapScorer(table=pre_df, rules=GeomapScorerCGICHomeRules(), as_of_date="2026-06-30", pss_version="v1_1").transform(
            fluvial_regions_gdf=regions("fluvial", PATHS.fluvial_regions), coastal_regions_gdf=regions("coastal", PATHS.coastal_regions))
        geo_tbl = f"{C}.{S}.t8_geo_{MODE}"
        g.table.drop("geometry").write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(geo_tbl)
    geo_df = spark.table(geo_tbl)
    with rec.time("T8", MODE, "3_merge_and_loss", n, recs="#3,#7", params=tag):
        m = MergeCls(table=geo_df, as_of_date="2026-06-30", run_name="t8").transform(
            fluvial_simulation_repo=PATHS.fluvial_simulation, coastal_simulation_repo=PATHS.coastal_simulation)
        materialize(aggregate_losses(m.fluvial_table, m.coastal_table, pre_df))
    return sum(r for r in [0])

measured = {s: [] for s in ("1_preprocessor", "2_geomap_regions_and_pss", "3_merge_and_loss")}
for n in [int(x) for x in dbutils.widgets.get("sizes").split(",")]:
    if measured["2_geomap_regions_and_pss"]:
        total = sum(project(v, n) for v in measured.values())
        if total > float(dbutils.widgets.get("budget_s")):
            print(f"skip n={n}: projected {total:.0f}s")
            for stage, pts in measured.items():
                rec.add("T8", MODE, stage, n, project(pts, n), measure="extrapolated", params={"from_points": pts})
            continue
    before = len(rec.rows)
    run(n)
    hist = spark.sql(f"SELECT stage, seconds FROM {C}.{S}.benchmark_results WHERE task_run_id='{rec.task_run_id}' "
                     f"AND test_id='T8' AND n_rows={n} AND measure='measured'").collect()
    for r in hist:
        measured[r.stage].append((n, r.seconds))

for stage, pts in measured.items():
    rec.add("T8", MODE, stage, 1_000_000, project(pts, 1_000_000), measure="extrapolated", params={"from_points": pts})
dbutils.notebook.exit(json.dumps(measured))
