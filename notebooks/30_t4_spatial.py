# Databricks notebook source
# MAGIC %md
# MAGIC # T4 — Point-in-polygon region scoring (recs #2 shapefile->Delta, #3 medallion, #4 bbox filter, #6 engine choice)
# MAGIC
# MAGIC | variant | how fluvial/coastal regions are assigned | code |
# MAGIC |---|---|---|
# MAGIC | `A_shp_geopandas_sedona` | `GeomapScorer.score_flood_regions(<.shp path>)`: GeoPandas reads shapefile on driver (Volume FUSE) -> WKB -> Sedona broadcast `ST_Intersects` + `row_number` | **verbatim** |
# MAGIC | `B_delta_sedona` | same method, polygons passed as a Spark DataFrame read from silver Delta (WKB) | verbatim method, Delta input |
# MAGIC | `C_delta_bbox_sedona` | B + equi-join on 1-degree grid cell of the polygon bbox before `ST_Intersects` | Databricks proposal |
# MAGIC | `D_native_st` | Databricks native `st_*` (DBR 17.3 / serverless) broadcast join | Databricks proposal |
# MAGIC | `E_native_h3` | H3 res-6 tessellation of polygons (once, silver), equi-join on cell + exact `st_contains` only for boundary cells | Databricks proposal |
# MAGIC | `G_geopandas_driver` | `gpd.sjoin` on the driver (their VM approach) | reference |
# MAGIC
# MAGIC Sedona variants need the Sedona JARs (classic only). On serverless the Sedona Python API resolves to native functions; that is tested as `A`/`B` with `engine=serverless`.

# COMMAND ----------

dbutils.widgets.text("catalog", "catalog_8as5lj_bvv7nu")
dbutils.widgets.text("schema", "charm_poc")
dbutils.widgets.text("compute", "{}")
dbutils.widgets.text("job_run_id", "")
dbutils.widgets.text("task_run_id", "")
dbutils.widgets.text("sizes", "1000000,10000000")
dbutils.widgets.text("variants", "A_shp_geopandas_sedona,B_delta_sedona,C_delta_bbox_sedona,D_native_st,E_native_h3,G_geopandas_driver")
dbutils.widgets.text("budget_s", "900")

import json, os
os.environ.setdefault("CHARM_DATA_ROOT", f"/Volumes/{dbutils.widgets.get('catalog')}/{dbutils.widgets.get('schema')}/raw/")  # before importing their package (PATHS is module-level), time
import geopandas as gpd
from pyspark.sql import functions as F
from charm_bench.bench import Recorder, materialize
from charm_bench.prep import book_table, pss_input

C, S = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
RAW = f"/Volumes/{C}/{S}/raw"
FLV_SHP, CST_SHP = f"{RAW}/flood_watersheds/can_flv_regions_v02.shp", f"{RAW}/flood_watersheds/can_cst_regions_v04.shp"
VARIANTS = dbutils.widgets.get("variants").split(",")
SIZES = [int(x) for x in dbutils.widgets.get("sizes").split(",")]
rec = Recorder(spark, f"{C}.{S}.benchmark_results", compute=json.loads(dbutils.widgets.get("compute")),
               job_run_id=dbutils.widgets.get("job_run_id"), task_run_id=dbutils.widgets.get("task_run_id"),
               workspace="azure-classic-fevm", code_fidelity="verbatim")
try:
    from sedona.spark import SedonaContext
    spark = SedonaContext.create(spark)
    SEDONA = spark.sql("SELECT ST_AsText(ST_Point(0,0))").first()[0] == "POINT (0 0)"
except Exception:  # noqa: BLE001
    SEDONA = False
print("Sedona JARs active:", SEDONA)

# COMMAND ----------

# MAGIC %md ### Points (1M = book; 10M = book x10 with jitter) and silver polygon tables (built once, timed as one-off)

# COMMAND ----------

def points(n):
    base = pss_input(spark.table(book_table(C, S, 1_000_000))).select("item_id", "latitude", "longitude")
    if n <= 1_000_000:
        return base
    k = n // 1_000_000
    rep = spark.range(k).withColumnRenamed("id", "r")
    return (base.crossJoin(rep)
            .select(F.concat("item_id", F.lit("_"), "r").alias("item_id"),
                    (F.col("latitude") + (F.rand(1) - 0.5) * 0.02).alias("latitude"),
                    (F.col("longitude") + (F.rand(2) - 0.5) * 0.02).alias("longitude")))

for n in SIZES:
    tbl = f"{C}.{S}.points_{n // 1_000_000}m"
    if not spark.catalog.tableExists(tbl):
        points(n).write.mode("overwrite").saveAsTable(tbl)

def build_silver():
    out = {}
    for name, shp in (("fluvial", FLV_SHP), ("coastal", CST_SHP)):
        tbl = f"{C}.{S}.silver_{name}_regions"
        t0 = time.perf_counter()
        g = gpd.read_file(shp)
        b = g.geometry.bounds
        pdf = g.drop(columns="geometry").assign(geometry_wkb=g.geometry.to_wkb(), xmin=b.minx, ymin=b.miny, xmax=b.maxx, ymax=b.maxy)
        spark.createDataFrame(pdf).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl)
        # H3 res-6 tessellation (native): core cells (fully inside) vs boundary cells (need exact test)
        spark.sql(f"""CREATE OR REPLACE TABLE {tbl}_h3 AS
            SELECT NUMBER, t.cellid AS cell, t.core AS is_core, t.chip AS chip_wkb
            FROM (SELECT NUMBER, explode(h3_tessellateaswkb(geometry_wkb, 6)) AS t FROM {tbl})""")
        out[name] = time.perf_counter() - t0
    return out

if not spark.catalog.tableExists(f"{C}.{S}.silver_coastal_regions_h3"):
    with rec.time("T4", "silver_build", "shapefile_to_delta_and_h3_once", 6000, recs="#2,#3") as r:
        r["notes"] = json.dumps(build_silver())

# COMMAND ----------

from cgl_dbx_flood_pipeline.geomap_scorer.geomap_scorer import GeomapScorer   # VERBATIM
from cgl_dbx_flood_pipeline.geomap_scorer.geomap_rules_cgic_home import GeomapScorerCGICHomeRules

def scorer_for(pts):
    """Instantiate their GeomapScorer on a minimal preprocessed table (province/risk cols are pass-through)."""
    t = (pts.withColumn("province", F.lit("ON")).withColumn("risk_type", F.lit("HOM"))
            .withColumn("dwelling_style", F.lit("1storey")).withColumn("foundation_type", F.lit("Slab")))
    gs = GeomapScorer(table=t, rules=GeomapScorerCGICHomeRules(), as_of_date="2026-06-30", pss_version="v1_1")
    gs.to_geodataframe()
    return gs

def sedona_regions_df(name):
    return (spark.table(f"{C}.{S}.silver_{name}_regions")
            .select("NUMBER", F.expr("ST_GeomFromWKB(geometry_wkb)").alias("geometry")))

def v_A(pts):  # verbatim: shapefile paths
    gs = scorer_for(pts)
    gs.score_flood_regions(FLV_SHP, source_crs="EPSG:4326", risk="flv")
    gs.score_flood_regions(CST_SHP, source_crs="EPSG:4326", risk="cst")
    return gs.table

def v_B(pts):  # verbatim method, Delta input
    gs = scorer_for(pts)
    gs.score_flood_regions(sedona_regions_df("fluvial"), source_crs="EPSG:4326", risk="flv")
    gs.score_flood_regions(sedona_regions_df("coastal"), source_crs="EPSG:4326", risk="cst")
    return gs.table

def _grid_join(pts, name, col_out, fn_contains):
    cell = 1.0
    reg = (spark.table(f"{C}.{S}.silver_{name}_regions")
           .withColumn("gx", F.explode(F.sequence(F.floor(F.col("xmin") / cell).cast("int"), F.floor(F.col("xmax") / cell).cast("int"))))
           .withColumn("gy", F.explode(F.sequence(F.floor(F.col("ymin") / cell).cast("int"), F.floor(F.col("ymax") / cell).cast("int")))))
    p = pts.withColumn("gx", F.floor(F.col("longitude") / cell).cast("int")).withColumn("gy", F.floor(F.col("latitude") / cell).cast("int"))
    j = p.join(F.broadcast(reg), ["gx", "gy"], "left").filter(
        F.col("NUMBER").isNull() | (F.col("longitude").between(F.col("xmin"), F.col("xmax")) & F.col("latitude").between(F.col("ymin"), F.col("ymax"))))
    j = j.filter(F.col("NUMBER").isNull() | fn_contains())
    return j.groupBy("item_id").agg(F.min("NUMBER").alias(col_out))

def v_C(pts):  # bbox/grid pre-filter + Sedona exact test
    c = lambda: F.expr("ST_Intersects(ST_Point(longitude, latitude), ST_GeomFromWKB(geometry_wkb))")
    return pts.select("item_id").join(_grid_join(pts, "fluvial", "fluvial_region", c), "item_id", "left").join(
        _grid_join(pts, "coastal", "coastal_region", c), "item_id", "left")

def v_D(pts):  # native Databricks spatial SQL, broadcast polygons
    pts.createOrReplaceTempView("pts")
    return spark.sql(f"""
        WITH p AS (SELECT item_id, st_point(longitude, latitude, 4326) g FROM pts),
        f AS (SELECT /*+ BROADCAST(r) */ p.item_id, min(r.NUMBER) fluvial_region FROM p
              LEFT JOIN {C}.{S}.silver_fluvial_regions r ON st_intersects(p.g, st_geomfromwkb(r.geometry_wkb, 4326)) GROUP BY p.item_id),
        c AS (SELECT /*+ BROADCAST(r) */ p.item_id, min(r.NUMBER) coastal_region FROM p
              LEFT JOIN {C}.{S}.silver_coastal_regions r ON st_intersects(p.g, st_geomfromwkb(r.geometry_wkb, 4326)) GROUP BY p.item_id)
        SELECT f.item_id, fluvial_region, coastal_region FROM f JOIN c USING (item_id)""")

def v_E(pts):  # H3 equi-join; exact test only on boundary cells
    pts.createOrReplaceTempView("pts")
    q = lambda name, col: f"""
        SELECT p.item_id, min(h.NUMBER) {col} FROM (SELECT item_id, longitude, latitude, h3_longlatash3(longitude, latitude, 6) cell FROM pts) p
        LEFT JOIN {C}.{S}.silver_{name}_regions_h3 h ON p.cell = h.cell
          AND (h.is_core OR st_contains(st_geomfromwkb(h.chip_wkb), st_point(p.longitude, p.latitude)))
        GROUP BY p.item_id"""
    return spark.sql(f"SELECT * FROM ({q('fluvial', 'fluvial_region')}) JOIN ({q('coastal', 'coastal_region')}) USING (item_id)")

def v_G(pts):  # GeoPandas on the driver
    pdf = pts.toPandas()
    g = gpd.GeoDataFrame(pdf, geometry=gpd.points_from_xy(pdf.longitude, pdf.latitude), crs="EPSG:4326")
    f = gpd.sjoin(g, gpd.read_file(FLV_SHP)[["NUMBER", "geometry"]], how="left", predicate="intersects")
    return len(f)

IMPL = {"A_shp_geopandas_sedona": v_A, "B_delta_sedona": v_B, "C_delta_bbox_sedona": v_C, "D_native_st": v_D,
        "E_native_h3": v_E, "G_geopandas_driver": v_G}
NEEDS_SEDONA = {"C_delta_bbox_sedona"}  # A/B run via Sedona API (JAR on classic; native fallback on serverless)

# COMMAND ----------

results = {}
for v in VARIANTS:
    if v in NEEDS_SEDONA and not SEDONA:
        print("skip", v, "(no Sedona JARs)"); continue
    for n in SIZES:
        pts = spark.table(f"{C}.{S}.points_{n // 1_000_000}m")
        try:
            with rec.time("T4", v + ("" if SEDONA or v not in ("A_shp_geopandas_sedona", "B_delta_sedona") else "_native_fallback"),
                          "assign_fluvial_and_coastal_region", n, recs="#2,#4,#6", params={"sedona_jars": SEDONA}):
                out = IMPL[v](pts)
                if not isinstance(out, int):
                    materialize(out)
            results[f"{v}_{n}"] = rec.last_seconds
        except Exception as e:  # noqa: BLE001 — e.g. GeoPandas OOM on 10M is a finding
            results[f"{v}_{n}"] = f"FAILED {type(e).__name__}"
            print(v, n, "FAILED", str(e)[:300])
            if rec.last_seconds > float(dbutils.widgets.get("budget_s")):
                break

# COMMAND ----------

# Parity: region assignments must agree between verbatim A and the fastest proposals on the 1M set
if "A_shp_geopandas_sedona" in VARIANTS and SIZES[0] == 1_000_000:
    pts = spark.table(f"{C}.{S}.points_1m")
    a = v_A(pts).select("item_id", "fluvial_region", "coastal_region")
    for v in [x for x in ("D_native_st", "E_native_h3") if x in VARIANTS]:
        b = IMPL[v](pts).select("item_id", F.col("fluvial_region").alias("f2"), F.col("coastal_region").alias("c2"))
        mism = a.join(b, "item_id").filter("NOT (fluvial_region <=> f2) OR NOT (coastal_region <=> c2)").count()
        rec.add("T4_parity", v, "mismatched_items_vs_A", 1_000_000, float(mism), notes="seconds column holds mismatch COUNT")
        print(v, "mismatches vs verbatim:", mism)

dbutils.notebook.exit(json.dumps(results, default=str))
