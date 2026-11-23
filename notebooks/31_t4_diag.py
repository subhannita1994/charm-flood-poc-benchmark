# Databricks notebook source
# MAGIC %md T4 diagnostic: are the sub-second no-Sedona-JAR timings real? Does native ST join need Photon?
# COMMAND ----------
import os, time, json, traceback
dbutils.widgets.text("catalog", "catalog_8as5lj_bvv7nu"); dbutils.widgets.text("schema", "charm_poc")
C, S = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
os.environ.setdefault("CHARM_DATA_ROOT", f"/Volumes/{C}/{S}/raw/")
from pyspark.sql import functions as F
from cgl_dbx_flood_pipeline.geomap_scorer.geomap_scorer import GeomapScorer
from cgl_dbx_flood_pipeline.geomap_scorer.geomap_rules_cgic_home import GeomapScorerCGICHomeRules
out = {}
pts = spark.table(f"{C}.{S}.points_1m")
# native ST: Photon effect (100k and 1M)
for lim in (100_000, 1_000_000):
  pts.limit(lim).createOrReplaceTempView("p100k"); t0 = time.perf_counter()
  n = spark.sql(f"""SELECT count(r.NUMBER) FROM (SELECT item_id, st_point(longitude, latitude, 4326) g FROM p100k) p
   LEFT JOIN {C}.{S}.silver_fluvial_regions r ON st_intersects(p.g, st_geomfromwkb(r.geometry_wkb, 4326))""").collect()[0][0]
  out[f"native_{lim}_s"] = time.perf_counter() - t0; out[f"native_{lim}_matches"] = n
  if out[f"native_{lim}_s"] > 900: break
print(json.dumps(out, indent=1, default=str))
dbutils.notebook.exit(json.dumps(out, default=str))
