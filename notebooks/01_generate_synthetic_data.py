# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Synthetic data for the CHARM flood-scoring benchmark
# MAGIC
# MAGIC Everything here is **synthetic**; nothing comes from Cooperators. Assumptions (also in `ASSUMPTIONS.md`):
# MAGIC
# MAGIC | # | What | Generated as |
# MAGIC |---|---|---|
# MAGIC | A1 | Book of business | 1M CGIC Home policies in the **raw** schema the preprocessor expects (`item_id_m`, `latitudeinternal`, ...). Locations clustered around 40 Canadian population centres. Dirty rows injected at fixed rates so every filter in their preprocessor does work. |
# MAGIC | A2 | Event sets | 50K simulated years x 20 events/yr = 1M events per peril, `return_period = 1/U` (so ~5% survive their `>= 20` filter), region drawn with a heavy-tailed activity weight |
# MAGIC | A3 | Region polygons | 5,000 fluvial + 1,000 coastal Voronoi cells over the Canada bbox, edges densified (`segmentize`) so vertex counts resemble watershed outlines; attribute `NUMBER` |
# MAGIC | A3b | Provinces | Real Statistics Canada 2021 boundary file `lpr_000a21a_e` (public). Falls back to synthetic rectangles if download is blocked |
# MAGIC | A4 | PSS map assets | 2 single rasters (0.005 deg, float32, ~17.8K x 5.8K px), 16 fluvial + 8 storm-surge multi-band depth tiles (7 RP bands, int16), 5 vector layers (20K/5K defenses, 100K/30K flood zones, 2K exclusion), damage-curve CSVs |
# MAGIC
# MAGIC Widgets let the same notebook build both workspaces' copies.

# COMMAND ----------

dbutils.widgets.text("catalog", "catalog_8as5lj_bvv7nu")
dbutils.widgets.text("schema", "charm_poc")
dbutils.widgets.text("n_policies", "1000000")
dbutils.widgets.text("steps", "book,events,regions,provinces,assets")
CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
N_POLICIES = int(dbutils.widgets.get("n_policies"))
STEPS = set(dbutils.widgets.get("steps").split(","))
RAW = f"/Volumes/{CATALOG}/{SCHEMA}/raw"
ASSETS = f"/Volumes/{CATALOG}/{SCHEMA}/assets/flood_model_serving"
SEED = 20261117
BBOX = dict(lon_min=-141.0, lon_max=-52.0, lat_min=41.0, lat_max=70.0)  # populated part of GLOBAL_CONFIG.canada_bbox
print(CATALOG, SCHEMA, N_POLICIES, STEPS)

# COMMAND ----------

import json, os, shutil, time, zipfile, urllib.request
import numpy as np
import pandas as pd
from pyspark.sql import functions as F

timings = {}
def timed(name):
    def deco(fn):
        def run(*a, **k):
            t = time.perf_counter(); r = fn(*a, **k); timings[name] = round(time.perf_counter() - t, 1)
            print(f"{name}: {timings[name]} s"); return r
        return run
    return deco

# COMMAND ----------

# MAGIC %md ## A1 — book of business (raw source schema)

# COMMAND ----------

CITIES = [  # (lat, lon, weight) — rough population weights
    (43.65, -79.38, 62), (45.50, -73.57, 43), (49.28, -123.12, 27), (51.05, -114.07, 15), (53.55, -113.49, 15),
    (45.42, -75.70, 15), (49.90, -97.14, 8), (46.81, -71.21, 8), (43.26, -79.87, 8), (43.45, -80.49, 6),
    (42.98, -81.25, 5), (44.65, -63.57, 5), (48.43, -123.37, 4), (52.13, -106.67, 3), (50.45, -104.61, 3),
    (43.32, -79.80, 3), (46.49, -80.99, 2), (47.56, -52.71, 2), (45.27, -66.06, 1), (46.24, -63.13, 1),
    (49.89, -119.50, 2), (46.35, -72.55, 2), (45.40, -71.89, 2), (48.38, -89.25, 1), (44.23, -76.49, 2),
    (49.17, -122.80, 3), (53.92, -122.75, 1), (50.68, -120.33, 1), (49.69, -112.84, 1), (56.73, -111.38, 1),
    (62.45, -114.37, 0.3), (60.72, -135.05, 0.3), (48.42, -71.07, 1), (46.10, -64.78, 1), (45.96, -66.64, 1),
    (44.38, -79.69, 2), (43.90, -78.86, 3), (42.31, -83.04, 3), (45.63, -72.96, 1), (55.17, -118.80, 1),
]
RISK = {"Homeowner": 60, "Residential": 12, "Tenant": 12, "Condominium": 8, "Seasonal": 4, "Seasonal high value": 1,
        "Mobile Homeowner": 2, "Mobile Residential": 1}
DWELL = ["1 storey", "1.5 storey", "2 storey", "2.5 storey", "3 storey", "Bi-level", "Tri-level", "Bungalow",
         "Single-Wide", "Double-Wide", "Backsplit", "Sidesplit"]
FOUND = ["Concrete", "Concrete blocks", "Crawl space", "Full Basement", "Slab", "Piers", "Wood", "Stone",
         "Preserved wood basement", "Raised Slab", "Other"]
DEDS = [500.0, 1000.0, 1500.0, 2000.0, 2500.0, 5000.0, 10000.0]
CENTROIDS = [(50.08990811, -85.83358805), (53.01757475, -70.92960413), (52.64990765, -60.24440640),
             (55.16720057, -114.51205581), (54.41643638, -105.89182305), (54.56061105, -125.10931540)]

def make_book(n, seed=SEED):
    rng = np.random.default_rng(seed)
    w = np.array([c[2] for c in CITIES]); w = w / w.sum()
    city = rng.choice(len(CITIES), n, p=w)
    spread = rng.gamma(1.5, 0.12, n)  # degrees; urban core + exurbs
    lat = np.array([c[0] for c in CITIES])[city] + rng.normal(0, 1, n) * spread
    lon = np.array([c[1] for c in CITIES])[city] + rng.normal(0, 1, n) * spread * 1.4
    df = pd.DataFrame({
        "item_id_m": [f"CGIC{i:08d}" for i in range(n)],
        "latitudeinternal": lat, "longitudeinternal": lon,
        "riskoption_name": rng.choice(list(RISK), n, p=np.array(list(RISK.values())) / sum(RISK.values())),
        "dwellingstyle_name": rng.choice(DWELL, n), "foundationtype_name": rng.choice(FOUND, n),
        "company": np.where(rng.random(n) < 0.97, "CGIC", "SGIC"),
        "product": np.where(rng.random(n) < 0.93, "Home", "Farm"),
        "flood_coverage": (rng.random(n) < 0.85).astype("int64"),
        "flood_deductible": rng.choice(DEDS, n),
        "floor": rng.choice([0, 1, 2, 3], n, p=[0.55, 0.40, 0.03, 0.02]).astype("int64"),
        "building_value": np.round(rng.lognormal(np.log(380_000), 0.45, n), 0),
        "flood_limit": rng.choice([50_000.0, 100_000.0, 250_000.0, 500_000.0], n, p=[0.3, 0.35, 0.25, 0.1]),
    })
    df["content_value"] = np.round(df["building_value"] * rng.uniform(0.15, 0.45, n), 0)
    # --- dirty rows so every preprocessor filter does real work (rates are assumptions) ---
    k = lambda rate: rng.random(n) < rate
    m = k(0.002); df.loc[m, ["latitudeinternal", "longitudeinternal"]] = np.nan               # missing coords
    m = k(0.004); ci = rng.integers(0, len(CENTROIDS), m.sum())                               # geocoded to province centroid
    df.loc[m, "latitudeinternal"] = [CENTROIDS[i][0] for i in ci]; df.loc[m, "longitudeinternal"] = [CENTROIDS[i][1] for i in ci]
    m = k(0.001); df.loc[m, "latitudeinternal"] = 30.0                                          # outside Canada
    m = k(0.003); df.loc[m, "building_value"] = -1.0                                            # bad TIV
    m = k(0.02);  df.loc[m, "flood_deductible"] = 0.5                                           # decimal deductible (-> 50, invalid)
    m = k(0.01);  df.loc[m, "dwellingstyle_name"] = "Unknown style"                             # unmapped code
    m = k(0.01);  df.loc[m, "foundationtype_name"] = None
    return df

@timed("A1_book")
def build_book():
    pdf = make_book(N_POLICIES)
    sdf = spark.createDataFrame(pdf)
    full = f"{CATALOG}.{SCHEMA}.inforce_book_1m"
    sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full)
    # Fixed-size slices materialised as their own tables, so tests never need `.limit()` (which collapses to 1 partition)
    for label, size in (("10", 10), ("100", 100), ("1k", 1_000), ("10k", 10_000), ("100k", 100_000)):
        spark.table(full).filter(F.col("item_id_m") < f"CGIC{size:08d}").write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.inforce_book_{label}")
    return spark.table(full).count()

if "book" in STEPS:
    print("policies:", build_book())

# COMMAND ----------

# MAGIC %md ## A3 — fluvial / coastal region polygons (shapefiles in bronze `raw` volume)

# COMMAND ----------

import geopandas as gpd
import shapely
from shapely.geometry import box

def voronoi_regions(n, seed, bounds, densify_deg):
    rng = np.random.default_rng(seed)
    pts = shapely.points(rng.uniform(bounds[0], bounds[2], n), rng.uniform(bounds[1], bounds[3], n))
    cells = shapely.voronoi_polygons(shapely.multipoints(pts), extend_to=box(*bounds))
    polys = [shapely.intersection(c, box(*bounds)) for c in shapely.get_parts(cells)]
    polys = [shapely.segmentize(p, densify_deg) for p in polys]
    return gpd.GeoDataFrame({"NUMBER": np.arange(1, len(polys) + 1, dtype="int64")}, geometry=polys, crs="EPSG:4326")

def write_shp(gdf, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    local = f"/tmp/shp_{os.path.basename(path)[:-4]}"; shutil.rmtree(local, ignore_errors=True); os.makedirs(local)
    gdf.to_file(f"{local}/{os.path.basename(path)}")
    for f in os.listdir(local):
        shutil.copy(f"{local}/{f}", os.path.join(os.path.dirname(path), f))

@timed("A3_regions")
def build_regions():
    b = (BBOX["lon_min"], BBOX["lat_min"], BBOX["lon_max"], BBOX["lat_max"])
    flv = voronoi_regions(5000, SEED + 1, b, 0.02)
    cst = voronoi_regions(1000, SEED + 2, b, 0.02)
    write_shp(flv, f"{RAW}/flood_watersheds/can_flv_regions_v02.shp")
    write_shp(cst, f"{RAW}/flood_watersheds/can_cst_regions_v04.shp")
    nv = int(shapely.get_num_coordinates(flv.geometry.values).sum())
    return {"fluvial": len(flv), "coastal": len(cst), "fluvial_vertices": nv}

if "regions" in STEPS:
    print(build_regions())

# COMMAND ----------

# MAGIC %md ## A3b — province boundaries (real StatCan 2021, EPSG:3347, column `PREABBR`)

# COMMAND ----------

PROV_DIR = f"{RAW}/ext_bi_ml_floodgeomap/flood_model_serving/maps/provinces/public/lpr_000a21a_e"
STATCAN = "https://www12.statcan.gc.ca/census-recensement/2021/geo/sip-pis/boundary-limites/files-fichiers/lpr_000a21a_e.zip"

@timed("A3b_provinces")
def build_provinces():
    os.makedirs(PROV_DIR, exist_ok=True)
    try:
        urllib.request.urlretrieve(STATCAN, "/tmp/lpr.zip")
        with zipfile.ZipFile("/tmp/lpr.zip") as z:
            z.extractall("/tmp/lpr")
        for f in os.listdir("/tmp/lpr"):
            if f.startswith("lpr_000a21a_e."):
                shutil.copy(f"/tmp/lpr/{f}", f"{PROV_DIR}/{f}")
        src = "statcan_2021"
    except Exception as e:  # noqa: BLE001 — egress blocked: synthetic rectangles, stated as assumption
        print("StatCan download failed, using synthetic provinces:", e)
        rects = {"BC.": (-139, 48.3, -114, 60), "Alta.": (-120, 49, -110, 60), "Sask.": (-110, 49, -101.4, 60),
                 "Man.": (-101.4, 49, -89, 60), "Ont.": (-95.2, 41.6, -74.3, 56.9), "Que.": (-79.8, 45, -57.1, 62.6),
                 "N.B.": (-69.1, 44.6, -63.8, 48.1), "N.S.": (-66.4, 43.4, -59.7, 47.1), "P.E.I.": (-64.5, 45.9, -62, 47.1),
                 "N.L.": (-67.8, 46.6, -52.6, 60.4), "Y.T.": (-141, 60, -124, 69.7), "N.W.T.": (-136, 60, -102, 78.8),
                 "Nvt.": (-120, 51.6, -61, 83.1)}
        gdf = gpd.GeoDataFrame({"PREABBR": list(rects)}, geometry=[box(*r) for r in rects.values()], crs="EPSG:4326").to_crs(3347)
        write_shp(gdf, f"{PROV_DIR}/lpr_000a21a_e.shp"); src = "synthetic_rectangles"
    g = gpd.read_file(f"{PROV_DIR}/lpr_000a21a_e.shp")
    return {"source": src, "rows": len(g), "crs": str(g.crs), "cols": list(g.columns)[:8],
            "vertices": int(shapely.get_num_coordinates(g.geometry.values).sum())}

if "provinces" in STEPS:
    print(build_provinces())

# COMMAND ----------

# MAGIC %md ## A2 — 50K-year event sets (Delta at the paths `FloodSimulationMerge.load_regions` reads)

# COMMAND ----------

@timed("A2_events")
def build_events(years=50_000, per_year=20):
    out = {}
    for peril, n_regions, seed in (("fluvial", 5000, SEED + 3), ("coastal", 1000, SEED + 4)):
        n = years * per_year
        df = (spark.range(n)
              .withColumn("simulated_year", (F.col("id") / per_year).cast("long") + 1)
              .withColumn("event_id", F.concat(F.lit(peril[0].upper()), F.col("id").cast("string")))
              # heavy-tailed regional activity: region = floor(n_regions * u^2) + 1
              .withColumn("flood_region", (F.floor(F.pow(F.rand(seed), 2) * n_regions) + 1).cast("string"))
              # return period of the event at that region: 1/U  (P(RP>=20) = 5%)
              .withColumn("return_period", (1.0 / (F.rand(seed + 1) * 0.999 + 0.001)).cast("double"))
              .drop("id"))
        # make sure every region has >= 1 event so `assert_region_coverage` passes (A2)
        cover = (spark.range(1, n_regions + 1).select(F.col("id").cast("string").alias("flood_region"))
                 .withColumn("simulated_year", F.lit(0).cast("long"))
                 .withColumn("event_id", F.concat(F.lit(f"{peril[0].upper()}C"), F.col("flood_region")))
                 .withColumn("return_period", F.lit(25.0)))
        df = df.unionByName(cover)
        path = f"{RAW}/simulations2/{peril}_simulation_events"
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.{peril}_simulation_events")
        out[peril] = spark.read.format("delta").load(path).count()
    return out

if "events" in STEPS:
    print(build_events())

# COMMAND ----------

# MAGIC %md ## A4 — PSS map assets for the `flood_model_serving` stand-in

# COMMAND ----------

import rasterio
from rasterio.transform import from_origin
from rasterio.windows import Window

RES = 0.005
W = int((BBOX["lon_max"] - BBOX["lon_min"]) / RES); H = int((BBOX["lat_max"] - BBOX["lat_min"]) / RES)
PROFILE = dict(driver="GTiff", tiled=True, blockxsize=512, blockysize=512, compress="deflate")
RPS = (20, 50, 75, 100, 200, 500, 1500)

def smooth_field(rows, cols, y0, x0, scale, kind):
    yy, xx = np.meshgrid(np.arange(y0, y0 + rows), np.arange(x0, x0 + cols), indexing="ij")
    f = np.sin(xx / 97.0) * np.cos(yy / 131.0) + 0.5 * np.sin((xx + yy) / 37.0) + 0.25 * np.cos(xx / 13.0 - yy / 17.0)
    return ((f + 1.75) * scale).astype(kind)

def write_raster(path, width, height, left, top, bands, fill):
    tmp = f"/tmp/{os.path.basename(path)}"
    with rasterio.open(tmp, "w", width=width, height=height, count=bands, dtype=fill["dtype"], crs="EPSG:4326",
                       transform=from_origin(left, top, RES, RES), **PROFILE) as dst:
        for r0 in range(0, height, 512):
            rows = min(512, height - r0)
            for b in range(bands):
                dst.write(fill["fn"](rows, width, r0, b), b + 1, window=Window(0, r0, width, rows))
    os.makedirs(os.path.dirname(path), exist_ok=True); shutil.copy(tmp, path); os.remove(tmp)

def depth_fn(rows, cols, r0, band):  # cm of water per RP band; ~85% dry cells
    f = smooth_field(rows, cols, r0, 0, 1.0, "float32")
    return np.clip((f - 2.2 + band * 0.12) * 400, 0, 600).astype("int16")

def circles(n, seed, radius, attrs):
    rng = np.random.default_rng(seed)
    w = np.array([c[2] for c in CITIES]); w = w / w.sum(); ci = rng.choice(len(CITIES), n, p=w)
    lat = np.array([c[0] for c in CITIES])[ci] + rng.normal(0, 0.35, n)
    lon = np.array([c[1] for c in CITIES])[ci] + rng.normal(0, 0.5, n)
    geoms = shapely.buffer(shapely.points(lon, lat), rng.uniform(0.3, 1.0, n) * radius, quad_segs=16)
    return gpd.GeoDataFrame({k: v(rng, n) for k, v in attrs.items()}, geometry=geoms, crs="EPSG:4326")

@timed("A4_assets")
def build_assets():
    maps = f"{ASSETS}/maps"
    write_raster(f"{maps}/rasters/distance_to_water.tif", W, H, BBOX["lon_min"], BBOX["lat_max"], 1,
                 {"dtype": "float32", "fn": lambda r, c, r0, b: smooth_field(r, c, r0, 0, 800.0, "float32")})
    write_raster(f"{maps}/rasters/elevation_to_water.tif", W, H, BBOX["lon_min"], BBOX["lat_max"], 1,
                 {"dtype": "float32", "fn": lambda r, c, r0, b: smooth_field(r, c, r0, 7, 12.0, "float32")})
    for name, nx, ny in (("fluvial_depth", 4, 4), ("storm_surge_depth", 4, 2)):
        tw, th = W // nx, H // ny
        for j in range(ny):
            for i in range(nx):
                write_raster(f"{maps}/tiles/{name}/tile_{j * nx + i:02d}.tif", tw, th,
                             BBOX["lon_min"] + i * tw * RES, BBOX["lat_max"] - j * th * RES, len(RPS),
                             {"dtype": "int16", "fn": depth_fn})
    sop = lambda rng, n: rng.choice([20, 50, 100, 200, 500], n).astype("int64")
    part = lambda rng, n: (rng.random(n) < 0.2).astype("int64")
    zone = lambda rng, n: rng.integers(1, 5, n).astype("int64")
    layers = {
        "fluvial_defenses": circles(20_000, SEED + 10, 0.02, {"SOP": sop, "PARTIAL": part}),
        "coastal_defenses": circles(5_000, SEED + 11, 0.02, {"SOP": sop, "PARTIAL": part}),
        "fluvial_flood_zones": circles(100_000, SEED + 12, 0.05, {"ZONE": zone}),
        "coastal_flood_zones": circles(30_000, SEED + 13, 0.05, {"ZONE": zone}),
        "exclusion_zones": circles(2_000, SEED + 14, 0.03, {"ID": lambda rng, n: np.arange(n, dtype="int64")}),
    }
    for name, gdf in layers.items():
        write_shp(gdf, f"{maps}/shapefiles/{name}.shp")
    # damage curves: 48 curves x 21 depth knots (0..5 m)
    dc_dir = f"{ASSETS}/damage_curves"; os.makedirs(dc_dir, exist_ok=True)
    depth = np.linspace(0, 5, 21); rows = []
    for cid in range(1, 49):
        k = 0.4 + (cid % 8) * 0.08
        bdr = 1 - np.exp(-k * depth); cdr = 1 - np.exp(-1.3 * k * depth)
        rows += [(cid, d, round(b, 4), round(c, 4)) for d, b, c in zip(depth, bdr, cdr)]
    pd.DataFrame(rows, columns=["curve_id", "depth_m", "building_dr", "contents_dr"]).to_csv(f"{dc_dir}/home_farm_dc.csv", index=False)
    styles = ["1storey", "1.5storey", "2storey", "2.5storey", "3storey", "Bilevel", "Trilevel", "Bungalow", "SingleWide",
              "DoubleWide", "Backsplit", "Sidesplit"]
    pd.DataFrame({"code": styles, "group": [s.lower()[:6] for s in styles]}).to_csv(f"{dc_dir}/building_style.csv", index=False)
    founds = ["Concrete", "ConcreteBlocks", "CrawlSpace", "FullBasement", "Slab", "Piers", "Wood", "Stone",
              "PresWoodBase", "RaisedSlab", "Other"]
    pd.DataFrame({"code": founds, "group": ["basement" if f in ("Concrete", "ConcreteBlocks", "FullBasement", "Stone", "PresWoodBase") else "nobase" for f in founds]}).to_csv(f"{dc_dir}/foundation_type.csv", index=False)
    risks = ["HOM", "RES", "TEN", "CON", "SEA", "SHV", "MOB", "MRE"]
    pd.DataFrame({"code": risks, "group": ["own" if r in ("HOM", "RES", "SEA", "SHV") else "other" for r in risks]}).to_csv(f"{dc_dir}/risk_type.csv", index=False)
    mp = [(s.lower()[:6], f, r, 1 + (i * 7 + j * 3 + k) % 48) for i, s in enumerate(styles) for j, f in enumerate(["basement", "nobase"]) for k, r in enumerate(["own", "other"])]
    pd.DataFrame(mp, columns=["building_style", "foundation_type", "risk_type", "curve_id"]).to_csv(f"{dc_dir}/home_farm_dc_mapping.csv", index=False)
    sizes = {}
    for root, _, files in os.walk(ASSETS):
        for f in files:
            sizes[os.path.relpath(os.path.join(root, f), ASSETS)] = os.path.getsize(os.path.join(root, f))
    return {"files": len(sizes), "total_mb": round(sum(sizes.values()) / 1e6, 1)}

if "assets" in STEPS:
    print(build_assets())

# COMMAND ----------

summary = {"catalog": CATALOG, "schema": SCHEMA, "n_policies": N_POLICIES, "timings_s": timings}
print(json.dumps(summary, indent=1))
dbutils.notebook.exit(json.dumps(summary))
