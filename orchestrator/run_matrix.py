"""Submit the CHARM benchmark matrix as one-time job runs (one submit per compute shape) and wait.

Usage:  python run_matrix.py <shape> [<shape> ...]      (shapes: see SHAPES)   -- profile charm-azure-classic
Each submit has up to two job clusters: `fuse` (PSS assets read from the UC Volume) and `local` (assets staged to
local NVMe by init script, rec #1). Tasks on different clusters run in parallel; tasks on one cluster run in order.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time

# Override with env vars to run in another workspace (see README): CHARM_PROFILE, CHARM_CATALOG, CHARM_SCHEMA, CHARM_WS_ROOT
PROFILE = os.environ.get("CHARM_PROFILE", "charm-azure-classic")
CAT, SCH = os.environ.get("CHARM_CATALOG", "catalog_8as5lj_bvv7nu"), os.environ.get("CHARM_SCHEMA", "charm_poc")
WS = os.environ.get("CHARM_WS_ROOT", "/Users/subhannita.sarcar@databricks.com/CHARM PoC/charm_benchmark")
WHL = f"/Volumes/{CAT}/{SCH}/assets/wheels/"
VOL_ASSETS = f"/Volumes/{CAT}/{SCH}/assets/flood_model_serving"
SEDONA_JARS = [{"maven": {"coordinates": "org.apache.sedona:sedona-spark-shaded-4.0_2.13:1.9.0"}},
               {"maven": {"coordinates": "org.datasyslab:geotools-wrapper:1.9.0-33.5"}}]
LIBS_PY = [*[{"pypi": {"package": p}} for p in ("apache-sedona==1.9.0", "geopandas", "rasterio", "pyogrio", "loguru", "typer")],
        *[{"whl": WHL + w} for w in ("cgl_dbx_flood_pipeline-0.0.2+charm.poc-py3-none-any.whl",
                                     "flood_model_serving-2.0.1+charm.standin-py3-none-any.whl",
                                     "charm_bench-0.1.0-py3-none-any.whl")]]
# `native` cluster: no Sedona JARs, so Databricks native ST_* functions are not shadowed by Sedona's (same names).
LIBS = {"fuse": SEDONA_JARS + LIBS_PY, "local": SEDONA_JARS + LIBS_PY, "native": LIBS_PY}

# name -> (driver, worker, n_workers, photon, spot, worker_cores_total)
SHAPES = {
    "S0_E2ds_v6_single": ("Standard_E2ds_v6", None, 0, False, False, 2),      # their benchmark sheet #1
    "S1_E8ds_v6_single": ("Standard_E8ds_v6", None, 0, False, False, 8),      # their benchmark sheet #2
    "S2_lab_E4as_1xE8as": ("Standard_E4as_v5", "Standard_E8as_v5", 1, False, False, 8),   # their resources/clusters/lab.yml
    "S3_E4as_4xE8as": ("Standard_E4as_v5", "Standard_E8as_v5", 4, False, False, 32),
    "S4_E4as_8xE8as": ("Standard_E4as_v5", "Standard_E8as_v5", 8, False, False, 64),
    "S5_E48ds_v5_single": ("Standard_E48ds_v5", None, 0, False, False, 48),   # "one large VM"
    "S6_E4as_4xE8as_photon": ("Standard_E4as_v5", "Standard_E8as_v5", 4, True, False, 32),
    "S7_E4as_4xE8as_spot": ("Standard_E4as_v5", "Standard_E8as_v5", 4, False, True, 32),
}


def cluster(shape: str, assets: str) -> dict:
    drv, wrk, n, photon, spot, _ = SHAPES[shape]
    c = {"spark_version": "17.3.x-scala2.13", "node_type_id": wrk or drv, "driver_node_type_id": drv,
         "num_workers": n, "data_security_mode": "SINGLE_USER",
         "runtime_engine": "PHOTON" if photon else "STANDARD",
         "azure_attributes": {"availability": "SPOT_WITH_FALLBACK_AZURE" if spot else "ON_DEMAND_AZURE",
                              "first_on_demand": 1, "spot_bid_max_price": -1},
         "spark_env_vars": {"CHARM_DATA_ROOT": f"/Volumes/{CAT}/{SCH}/raw/",
                            "FMS_ASSETS_ROOT": VOL_ASSETS if assets == "fuse" else "/local_disk0/fms_assets",
                            "FMS_STAGE_SRC": VOL_ASSETS},  # read by init/stage_fms_assets.sh
         "custom_tags": {"project": "charm_poc", "shape": shape, "assets": assets}}
    if n == 0:
        c["spark_conf"] = {"spark.master": "local[*]", "spark.databricks.cluster.profile": "singleNode"}
        c["custom_tags"]["ResourceClass"] = "SingleNode"
    if assets in ("local", "native"):
        c["init_scripts"] = [{"workspace": {"destination": f"{WS}/init/stage_fms_assets.sh"}}]
    return c


def compute_json(shape: str, assets: str) -> str:
    drv, wrk, n, photon, spot, cores = SHAPES[shape]
    return json.dumps({"type": "classic", "shape": shape, "driver": drv, "worker": wrk, "workers": n, "photon": photon,
                       "spot": spot, "worker_cores": cores, "assets": "local" if assets == "native" else assets,
                       "sedona_jars": assets != "native"})


T4_SEDONA = "A_shp_geopandas_sedona,B_delta_sedona,C_delta_bbox_sedona,G_geopandas_driver"
T4_NATIVE = "A_shp_geopandas_sedona,B_delta_sedona,D_native_st,E_native_h3"


def plan(shape: str, only: set[str] | None = None) -> list[tuple[str, str, str, dict]]:
    """(task_key, cluster_key, notebook, params). Cluster keys: fuse (Sedona, Volume assets), local (Sedona, local
    assets), native (no Sedona JARs, local assets). Sizes keep every run inside the A8 budget."""
    cores = SHAPES[shape][5]
    if shape in ("S0_E2ds_v6_single", "S1_E8ds_v6_single"):  # calibration against their sheet
        t = [("t8_baseline", "fuse", "50_t8_end_to_end", {"mode": "baseline", "sizes": "10,100,1000", "budget_s": "1200"}),
             ("t1", "local", "10_t1_import_and_assets", {"n_predict": "200", "repeats": "2"}),
             ("t2a_local", "local", "20_t2a_local_pss", {"variant": "B_local_assets", "sizes": "10,100,1000,10000"}),
             ("t8_tuned", "local", "50_t8_end_to_end", {"mode": "tuned", "sizes": "10,100,1000,10000"})]
    else:
        t = [("t2a_fuse", "fuse", "20_t2a_local_pss", {"variant": "A_fuse_defaults", "sizes": "10,100,1000,10000"}),
             ("t2b", "fuse", "21_t2b_http_pss", {"total_cores": str(cores), "sizes": "1000,10000,100000,1000000",
                                                  "tuned_partition_factors": "2,4", "tuned_threads": "8,32",
                                                  "fault_test": "true" if shape == "S3_E4as_4xE8as" else "false"}),
             ("t2a_local", "local", "20_t2a_local_pss", {"variant": "B_local_assets", "sizes": "100,1000,10000,100000"}),
             ("t2a_local_t32", "local", "20_t2a_local_pss", {"variant": "B_local_assets_ppc4_t32", "sizes": "1000,10000,100000",
                                                              "partitions_per_core": "4", "max_threads_per_task": "32", "warm_repeat": "false"}),
             ("t4_sedona", "local", "30_t4_spatial", {"sizes": "1000000,10000000", "variants": T4_SEDONA}),
             ("t8_tuned", "local", "50_t8_end_to_end", {"mode": "tuned", "sizes": "1000,10000,100000"}),
             ("t4_native", "native", "30_t4_spatial", {"sizes": "1000000,10000000", "variants": T4_NATIVE}),
             ("t5", "native", "40_t5_merge_loss", {"sizes": "100000,1000000"}),
             ("t8_tuned_native", "native", "50_t8_end_to_end", {"mode": "tuned", "sizes": "1000,10000,100000"})]
        if shape == "S2_lab_E4as_1xE8as":
            t.append(("t8_baseline", "fuse", "50_t8_end_to_end", {"mode": "baseline", "sizes": "10,100,1000", "budget_s": "1200"}))
        if shape in ("S6_E4as_4xE8as_photon", "S7_E4as_4xE8as_spot"):  # only stages these flags can affect
            t = [x for x in t if x[0] in ("t4_sedona", "t8_tuned", "t4_native", "t5", "t8_tuned_native")]
    return [x for x in t if only is None or x[0] in only]


def submit(shape: str, only: set[str] | None = None) -> int:
    tasks, last_on = [], {}
    steps = plan(shape, only)
    used = sorted({c for _, c, _, _ in steps})
    for key, ck, nb, params in steps:
        base = {"catalog": CAT, "schema": SCH, "compute": compute_json(shape, ck),
                "job_run_id": "{{job.run_id}}", "task_run_id": "{{task.run_id}}", **params}
        task = {"task_key": key, "job_cluster_key": ck, "libraries": LIBS[ck], "timeout_seconds": 7200,
                "notebook_task": {"notebook_path": f"{WS}/notebooks/{nb}", "base_parameters": base}}
        if ck in last_on:
            task["depends_on"] = [{"task_key": last_on[ck]}]
            task["run_if"] = "ALL_DONE"
        last_on[ck] = key
        tasks.append(task)
    settings = {"name": f"charm_poc_{shape}", "tasks": tasks, "max_concurrent_runs": 1,
                "tags": {"project": "charm_poc", "shape": shape},
                "job_clusters": [{"job_cluster_key": ck, "new_cluster": cluster(shape, ck)} for ck in used]}
    job_id = _upsert_job(settings)
    out = _cli(["jobs", "run-now", str(job_id), "--no-wait"])
    return json.loads(out)["run_id"]


def _cli(args: list[str]) -> str:
    out = subprocess.run(["databricks", *args, "--profile", PROFILE, "-o", "json"], capture_output=True, text=True)
    if out.returncode != 0:
        raise RuntimeError(f"databricks {' '.join(args[:2])} failed: {out.stderr.strip()[:1500]}")
    return out.stdout


def _upsert_job(settings: dict) -> int:
    """Persistent job per shape (re-runnable by CHARM); reset settings if it already exists."""
    found = json.loads(_cli(["jobs", "list", "--name", settings["name"]]) or "[]")
    if found:
        job_id = found[0]["job_id"]
        _cli(["jobs", "reset", "--json", json.dumps({"job_id": job_id, "new_settings": settings})])
        return job_id
    return json.loads(_cli(["jobs", "create", "--json", json.dumps(settings)]))["job_id"]


def wait(run_ids: dict[str, int]) -> None:
    pending = dict(run_ids)
    while pending:
        for shape, rid in list(pending.items()):
            d = json.loads(subprocess.run(["databricks", "jobs", "get-run", str(rid), "--profile", PROFILE, "-o", "json"],
                                          capture_output=True, text=True).stdout)
            if d["state"]["life_cycle_state"] in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
                summary = {t["task_key"]: t["state"].get("result_state") for t in d["tasks"]}
                print(time.strftime("%H:%M:%S"), shape, rid, d["state"].get("result_state"), summary, flush=True)
                pending.pop(shape)
        time.sleep(60)


if __name__ == "__main__":
    # python run_matrix.py S3_E4as_4xE8as[:t4_sedona+t5] ...   (optional task subset after ':')
    ids = {}
    for arg in sys.argv[1:] or list(SHAPES):
        shape, _, sub = arg.partition(":")
        ids[shape] = submit(shape, set(sub.split("+")) if sub else None)
    print(json.dumps(ids), flush=True)
    wait(ids)
