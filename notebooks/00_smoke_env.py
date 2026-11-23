# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Environment smoke test
# MAGIC Verifies classic compute + Sedona (Maven JARs + PyPI) + geo libs + Databricks native spatial SQL on DBR 17.3.

# COMMAND ----------

import json, os, platform, sys, time

out = {"python": sys.version.split()[0], "cpus": os.cpu_count()}
try:
    out["dbr"] = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")
except Exception:  # serverless (Spark Connect) blocks clusterUsageTags reads
    out["dbr"] = "serverless:" + os.environ.get("DATABRICKS_RUNTIME_VERSION", "?")

def tryit(name, fn):
    t = time.perf_counter()
    try:
        out[name] = {"ok": True, "val": str(fn())[:200], "s": round(time.perf_counter() - t, 2)}
    except Exception as e:  # noqa: BLE001
        out[name] = {"ok": False, "err": f"{type(e).__name__}: {e}"[:400]}

tryit("native_st_point", lambda: spark.sql("SELECT st_astext(st_point(1, 2))").first()[0])
tryit("native_h3", lambda: spark.sql("SELECT h3_longlatash3(-75.7, 45.4, 7)").first()[0])

def _sedona():
    from sedona.spark import SedonaContext
    s = SedonaContext.create(spark)
    return s.sql("SELECT ST_AsText(ST_Point(0, 0))").first()[0]
tryit("sedona", _sedona)

for mod in ("geopandas", "shapely", "rasterio", "pyogrio", "loguru", "typer", "pandas", "pyarrow"):
    tryit(f"import_{mod}", lambda m=mod: __import__(m).__version__)

print(json.dumps(out, indent=1))
dbutils.notebook.exit(json.dumps(out))
