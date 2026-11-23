# Databricks notebook source
# MAGIC %md
# MAGIC # T1 — Package import & PSS asset load (rec #1: stage files to local disk)
# MAGIC
# MAGIC Every measurement runs in a **fresh Python subprocess** so nothing is served from the parent's import/page cache
# MAGIC except what the OS itself caches (reported as the "warm" repeat).
# MAGIC
# MAGIC | Variant | Code | PSS assets |
# MAGIC |---|---|---|
# MAGIC | A_fuse | `sys.path` -> `/Workspace/.../src` (their notebook practice) | UC Volume (FUSE) |
# MAGIC | B_local_assets | wheel | copied to local disk with `shutil.copytree` (copy time recorded) |
# MAGIC | C_wheel_fuse_assets | wheel | UC Volume (FUSE) |
# MAGIC | D_local_cached_handles | wheel | local disk + raster handles kept open (proposal for the library owners) |

# COMMAND ----------

dbutils.widgets.text("catalog", "catalog_8as5lj_bvv7nu")
dbutils.widgets.text("schema", "charm_poc")
dbutils.widgets.text("compute", "{}")
dbutils.widgets.text("job_run_id", "")
dbutils.widgets.text("task_run_id", "")
dbutils.widgets.text("n_predict", "200")
dbutils.widgets.text("repeats", "2")
CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
VOL_ASSETS = f"/Volumes/{CATALOG}/{SCHEMA}/assets/flood_model_serving"
# <repo root in the workspace>/src, derived from this notebook's own path (<root>/notebooks/10_t1_...)
_nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
WS_SRC = "/Workspace" + _nb.rsplit("/notebooks/", 1)[0] + "/src"
N_PREDICT, REPEATS = int(dbutils.widgets.get("n_predict")), int(dbutils.widgets.get("repeats"))

import json, os, shutil, subprocess, sys, tempfile, time
from charm_bench.bench import Recorder

rec = Recorder(spark, f"{CATALOG}.{SCHEMA}.benchmark_results", compute=json.loads(dbutils.widgets.get("compute")),
               job_run_id=dbutils.widgets.get("job_run_id"), task_run_id=dbutils.widgets.get("task_run_id"),
               workspace="azure-classic-fevm", code_fidelity="verbatim+standin")

# COMMAND ----------

PROBE = r'''
import json, os, sys, time
t = {}
t0 = time.perf_counter()
if os.environ.get("CHARM_SRC_PATH"):
    sys.path.insert(0, os.environ["CHARM_SRC_PATH"])
import cgl_dbx_flood_pipeline.geomap_scorer.create_pss_endpoint_cgic_home as ep   # their module
t["import_pipeline_s"] = time.perf_counter() - t0
t["pipeline_from"] = os.path.dirname(ep.__file__)
t0 = time.perf_counter()
import flood_model_serving.assets.flex_map.cgic.v1_1.paths as mp
import flood_model_serving.assets.pss.dc.home_farm.cgic.v1.paths as dp
t["import_assets_exists_checks_s"] = time.perf_counter() - t0
t0 = time.perf_counter()
svc = ep.init_cgic_home_farm_pss_service(endpoint_version="v1_1")                  # their builder
t["service_init_s"] = time.perf_counter() - t0
import random
rnd = random.Random(1)
pts = [(rnd.uniform(43.3, 46.0), rnd.uniform(-80.5, -71.0)) for _ in range(int(os.environ["N_PREDICT"]))]
t0 = time.perf_counter(); svc.predict(lat=pts[0][0], long=pts[0][1], building_style="1storey", foundation_type="Slab", risk_type="HOM")
t["first_predict_s"] = time.perf_counter() - t0
t0 = time.perf_counter()
for lat, lon in pts[1:]:
    svc.predict(lat=lat, long=lon, building_style="2storey", foundation_type="FullBasement", risk_type="HOM")
t["predict_ms_avg"] = (time.perf_counter() - t0) / max(1, len(pts) - 1) * 1000
print("RESULT" + json.dumps(t))
'''

def probe(env_extra: dict, use_wheel: bool) -> dict:
    env = {k: v for k, v in os.environ.items()}
    env.update(env_extra); env["N_PREDICT"] = str(N_PREDICT)
    env.pop("CHARM_SRC_PATH", None) if use_wheel else None
    cmd = [sys.executable, "-c", PROBE]
    if not use_wheel:  # hide the wheel so the /Workspace copy is the one imported
        env["CHARM_SRC_PATH"] = WS_SRC
    p = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=3600)
    line = [l for l in p.stdout.splitlines() if l.startswith("RESULT")]
    if not line:
        raise RuntimeError(p.stderr[-2000:])
    return json.loads(line[0][6:])

# COMMAND ----------

local_root = os.path.join("/local_disk0" if os.access("/local_disk0", os.W_OK) else tempfile.gettempdir(), "fms_assets")  # serverless: /local_disk0 read-only
with rec.time("T1", "B_local_assets", "stage_copy_volume_to_local", 0, recs="#1",
              params={"src": VOL_ASSETS, "dst": local_root}) as r:
    shutil.rmtree(local_root, ignore_errors=True)
    shutil.copytree(VOL_ASSETS, local_root)
    size_mb = sum(os.path.getsize(os.path.join(d, f)) for d, _, fs in os.walk(local_root) for f in fs) / 1e6
    r["notes"] = f"copied {size_mb:.0f} MB"

VARIANTS = [
    ("A_fuse", False, {"FMS_ASSETS_ROOT": VOL_ASSETS, "FMS_RASTER_HANDLES": "per_call"}),
    ("C_wheel_fuse_assets", True, {"FMS_ASSETS_ROOT": VOL_ASSETS, "FMS_RASTER_HANDLES": "per_call"}),
    ("B_local_assets", True, {"FMS_ASSETS_ROOT": local_root, "FMS_RASTER_HANDLES": "per_call"}),
    ("D_local_cached_handles", True, {"FMS_ASSETS_ROOT": local_root, "FMS_RASTER_HANDLES": "cached"}),
]
summary = []
for rep in range(REPEATS):
    for name, wheel, env in VARIANTS:
        res = probe(env, wheel)
        res.update(variant=name, repeat=rep); summary.append(res)
        for stage, val, n in (("import_pipeline", res["import_pipeline_s"], 1),
                              ("import_assets_exists_checks", res["import_assets_exists_checks_s"], 1),
                              ("service_init", res["service_init_s"], 1),
                              ("first_predict", res["first_predict_s"], 1),
                              ("predict_steady_state", res["predict_ms_avg"] / 1000 * (N_PREDICT - 1), N_PREDICT - 1)):
            rec.add("T1", name, stage, n, val, recs="#1",
                    params={"repeat": rep, "pipeline_from": res["pipeline_from"], **env},
                    notes="cold" if rep == 0 else "warm (OS page cache may hold FUSE blocks)")

import pandas as pd
df = pd.DataFrame(summary); display(df)
dbutils.notebook.exit(df.to_json(orient="records"))
