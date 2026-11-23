"""Submit the serverless parts of the PoC as one-time job runs and wait for them.

Usage (after scripts/deploy.sh; reads the same env vars as scripts/config.sh):
    python3 scripts/run_serverless.py smoke      # 00_smoke_env: checks libraries + native spatial SQL
    python3 scripts/run_serverless.py data       # 01_generate_synthetic_data: 1M book, events, polygons, PSS maps (~1.1 GB)
    python3 scripts/run_serverless.py tests      # T1, T4, T5, T2b on serverless (the "Serverless" row in the results)

Serverless has no Sedona JARs; their Sedona Python calls resolve to Databricks' native ST_* functions.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time

PROFILE = os.environ.get("CHARM_PROFILE", "charm-azure-classic")
CAT = os.environ.get("CHARM_CATALOG", "catalog_8as5lj_bvv7nu")
SCH = os.environ.get("CHARM_SCHEMA", "charm_poc")
WS = os.environ.get("CHARM_WS_ROOT")  # e.g. /Users/<you>/CHARM PoC/charm_benchmark (config.sh sets it)
WHL = f"/Volumes/{CAT}/{SCH}/assets/wheels/"
WHEELS = [WHL + w for w in ("cgl_dbx_flood_pipeline-0.0.2+charm.poc-py3-none-any.whl",
                            "flood_model_serving-2.0.1+charm.standin-py3-none-any.whl",
                            "charm_bench-0.1.0-py3-none-any.whl")]
TEST_DEPS = WHEELS + ["geopandas", "rasterio", "pyogrio", "loguru", "typer", "apache-sedona==1.9.0"]
DATA_DEPS = ["geopandas", "rasterio", "pyogrio", "shapely>=2.1"]
COMPUTE = json.dumps({"type": "serverless", "env": "4", "label": "serverless_standard"})

# name -> (notebook, params)   tasks run in sequence, each runs even if the previous one failed
SUITES = {
    "smoke": [("smoke", "00_smoke_env", {})],
    "data": [("gen", "01_generate_synthetic_data", {"n_policies": "1000000", "steps": "book,events,regions,provinces,assets"})],
    "tests": [
        ("t1", "10_t1_import_and_assets", {"n_predict": "200", "repeats": "2"}),
        ("t4_native", "30_t4_spatial", {"sizes": "1000000,10000000",
                                        "variants": "A_shp_geopandas_sedona,B_delta_sedona,D_native_st,E_native_h3,G_geopandas_driver"}),
        ("t5", "40_t5_merge_loss", {"sizes": "100000,1000000"}),
        ("t2b", "21_t2b_http_pss", {"sizes": "1000,10000,100000,1000000", "variants": "upstream,scratch,tuned",
                                    "tuned_partition_factors": "8,16", "tuned_threads": "8,32", "total_cores": "8",
                                    "fault_test": "false"}),
    ],
}


def cli(*args: str) -> dict:
    out = subprocess.run(["databricks", *args, "--profile", PROFILE, "-o", "json"], capture_output=True, text=True)
    if out.returncode:
        sys.exit(out.stderr.strip()[:1500])
    return json.loads(out.stdout or "{}")


def submit(suite: str) -> int:
    if not WS:
        sys.exit("CHARM_WS_ROOT not set: run `source scripts/config.sh` first")
    base = {"catalog": CAT, "schema": SCH, "compute": COMPUTE, "job_run_id": "{{job.run_id}}", "task_run_id": "{{task.run_id}}"}
    tasks, prev = [], None
    for key, nb, params in SUITES[suite]:
        t = {"task_key": key, "environment_key": "e", "timeout_seconds": 7200,
             "notebook_task": {"notebook_path": f"{WS}/notebooks/{nb}",
                               "base_parameters": ({"catalog": CAT, "schema": SCH} if suite == "data" else base) | params}}
        if prev:
            t["depends_on"], t["run_if"] = [{"task_key": prev}], "ALL_DONE"
        tasks.append(t)
        prev = key
    deps = DATA_DEPS if suite == "data" else TEST_DEPS
    spec = {"run_name": f"charm_poc_serverless_{suite}", "tasks": tasks,
            "environments": [{"environment_key": "e", "spec": {"environment_version": "4", "dependencies": deps}}]}
    tmp = f"/tmp/charm_serverless_{suite}.json"
    with open(tmp, "w") as f:
        json.dump(spec, f)
    return cli("jobs", "submit", "--json", f"@{tmp}", "--no-wait")["run_id"]


def wait(run_id: int) -> None:
    while True:
        d = cli("jobs", "get-run", str(run_id))
        if d["state"]["life_cycle_state"] in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
            print(d["state"].get("result_state"), {t["task_key"]: t["state"].get("result_state") for t in d["tasks"]})
            print(d.get("run_page_url", ""))
            return
        time.sleep(30)


if __name__ == "__main__":
    for s in sys.argv[1:] or ["tests"]:
        rid = submit(s)
        print(s, "run_id", rid, flush=True)
        wait(rid)
