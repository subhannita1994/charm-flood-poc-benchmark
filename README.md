# CHARM flood-scoring PoC — Databricks benchmark harness

> **Confidential. Keep this repository private.** `src/cgl_dbx_flood_pipeline/` is a copy of Cooperators' (CHARM team) flood-scoring
> code, shared with Databricks for this PoC. Everything else is Databricks-authored: the harness, notebooks, stand-ins,
> synthetic-data generator and orchestration. All data is synthetic and none of it comes from Cooperators.

This repo tests the eight Databricks recommendations (Oct 15, 2026) on CHARM's own flood-scoring pipeline.
For each recommendation it measures latency and cost per run against their legacy-VM numbers.
It runs end to end on an Azure Databricks workspace (classic job clusters plus serverless jobs) and writes one row per measurement
to a Delta table. CHARM can rerun every test.

* **Latest results:** [`results/poc_tests_results_2026-11-18.md`](results/poc_tests_results_2026-11-18.md), an export of the "PoC Tests" tab of the PoC Google Doc.
* **Assumptions** behind every number (A1–A11): [`ASSUMPTIONS.md`](ASSUMPTIONS.md).
* **Changes made to their code** so it runs, plus code findings: [`PATCHES.md`](PATCHES.md).

---

## 1. What is in the repo

```
charm_benchmark/
├── src/cgl_dbx_flood_pipeline/      Their pipeline code (CHARM upload, 2026-11-17, mid-PR18). Minimal run fixes only, tagged
│                                    `CHARM-POC PATCH` / `CHARM-POC RECONSTRUCTED` (see PATCHES.md). Built as a wheel.
├── src/pyproject.toml               Wheel build for the above (pyproject.upstream.toml = their original, for reference)
├── stand_ins/flood_model_serving_standin/
│                                    Stand-in for their internal PSS library `flood_model_serving` (not shareable). Same module
│                                    paths/classes; reproduces the I/O pattern (A4). Built as a wheel.
├── harness/src/charm_bench/         Our code, built as a wheel:
│     bench.py                       Recorder: timing context manager -> `benchmark_results` table
│     http_stub.py                   In-process stand-in for the hosted CGIC flood API (49 ms/call, A5)
│     variants.py                    The "Databricks proposal" variants (never patched into their code) + loss stand-in (A6)
│     prep.py                        Shared input prep + the A8 extrapolation fit
├── notebooks/                       One notebook per test (Databricks source format), see section 5
├── init/stage_fms_assets.sh         Cluster init script = rec #1 (copy PSS maps from the UC Volume to local NVMe)
├── orchestrator/run_matrix.py       Creates/updates one job per classic compute shape (`charm_poc_<shape>`) and runs it
├── scripts/
│     config.sh                      Profile / catalog / schema / workspace path (env overrides)
│     deploy.sh                      Creates UC objects, builds + uploads wheels, imports notebooks/src/init script
│     run_serverless.py              Smoke test, synthetic data generation, serverless tests
│     sql.sh                         Run a SQL statement on a warehouse (results queries)
├── results/                         Result write-ups (exports of the PoC Tests tab)
├── ASSUMPTIONS.md  PATCHES.md
└── dist/                            Built wheels (git-ignored; produced by deploy.sh)
```

---

## 2. Prerequisites

| Need | Detail |
|---|---|
| Azure Databricks workspace | Unity Catalog enabled; **classic job clusters and serverless jobs** both allowed. The PoC used an FEVM classic workspace in Canada Central (`adb-7405612043532077`). |
| UC permissions | `USE CATALOG` + `CREATE SCHEMA` on an existing catalog (or a schema you own); `CREATE VOLUME`, `CREATE TABLE` in it. |
| System tables | `SELECT` on `system.billing.usage` (cost per run in notebook 90). |
| VM quota (vCPU) | Standard_EADSv5 / EASv5 family: up to **68 vCPU** for S4 (4 + 8 × 8). EDSv5: 48 for S5. EDSv6: 8 for S1. Without S4 quota, skip that shape. |
| Outbound internet from clusters | Maven Central (Sedona JARs) and PyPI (geopandas, rasterio, pyogrio, apache-sedona, loguru, typer). |
| SQL warehouse | Any (serverless starter is fine), for `scripts/sql.sh`. |
| Local tools | Databricks CLI (tested on v1.18), [`uv`](https://docs.astral.sh/uv/) for wheel builds, Python ≥ 3.11, bash. |
| Runtime | DBR **17.3 LTS** (`17.3.x-scala2.13`), Sedona **1.9.0** (the versions CHARM's job uses). Serverless environment version **4**. |

Authenticate the CLI to the workspace with a profile name of your choice:

```bash
databricks auth login --host https://adb-<workspace-id>.<n>.azuredatabricks.net --profile charm-azure-classic
databricks current-user me --profile charm-azure-classic
```

---

## 3. Configure

All scripts read these environment variables. The defaults are the PoC's values, so set them for your workspace:

```bash
export CHARM_PROFILE=charm-azure-classic          # CLI profile
export CHARM_CATALOG=<your_catalog>               # default catalog_8as5lj_bvv7nu
export CHARM_SCHEMA=charm_poc
export CHARM_WAREHOUSE_ID=<warehouse id>          # databricks warehouses list --profile $CHARM_PROFILE
source scripts/config.sh                          # also derives CHARM_WS_ROOT=/Users/<you>/CHARM PoC/charm_benchmark
```

The notebooks take `catalog` / `schema` as widgets, and the scripts pass them through. If you use a different catalog,
`run_matrix.py`, `run_serverless.py` and `deploy.sh` pick it up from these variables.

---

## 4. Set up (one time)

```bash
# 4.1 UC schema + volumes (raw, assets), build 3 wheels, upload them to /Volumes/<cat>/<schema>/assets/wheels/,
#     import notebooks/, src/ and init/ into /Workspace$CHARM_WS_ROOT
./scripts/deploy.sh

# 4.2 Smoke test on serverless: libraries import, native spatial SQL works
python3 scripts/run_serverless.py smoke

# 4.3 Synthetic data (serverless). Writes:
#     tables  inforce_book_{10,100,1k,10k,100k,1m}, fluvial_/coastal_simulation_events (1M events each)
#     volume  raw/   region + province shapefiles, event Delta folders (paths their config expects)
#     volume  assets/flood_model_serving/   ~1.1 GB synthetic PSS maps (rasters, tiles, vector layers, damage curves)
python3 scripts/run_serverless.py data
```

After any code change, rerun `./scripts/deploy.sh` (or `./scripts/deploy.sh wheels` if only package code changed).

What each step sets up, and why:
* **Wheels.** Clusters install all three from the Volume: `cgl_dbx_flood_pipeline` (their code), `flood_model_serving` (stand-in) and `charm_bench` (harness).
  Their code is also imported as files into `/Workspace/.../src`, because T1 variant A measures their notebook practice of importing from `/Workspace`.
* **Init script.** `init/stage_fms_assets.sh` runs on the `local` and `native` job clusters. It copies the maps from `FMS_STAGE_SRC` (the Volume)
  to `/local_disk0/fms_assets`. `FMS_ASSETS_ROOT` then tells the PSS library where to read from. This is rec #1 as pure cluster config.
* **Provinces.** The generator tries the public StatCan 2021 boundary file. If the workspace blocks the download, it falls back to synthetic rectangles (A3b).

---

## 5. Reproduce the tests

### 5.1 Test map

| Notebook | Test | Recommendation | What it compares |
|---|---|---|---|
| `10_t1_import_and_assets` | T1 | #1 stage files locally | Package + PSS maps from the UC Volume vs local disk vs files kept open; each in a fresh Python subprocess |
| `20_t2a_local_pss` | T2a | #1, #5, #7 | Their `score_with_pss` (local library path) with maps on Volume vs local; threads × partitions; shapes |
| `21_t2b_http_pss` | T2b | #7 parallelism | PR18 HTTP-API path: their uploaded execution vs their scratch version vs tuned (partitions × threads), against the stub |
| `30_t4_spatial` | T4 | #2, #4, #6 | Region scoring: shapefile+Sedona (theirs) vs Delta+Sedona vs bbox pre-filter vs native `st_*` vs H3 join vs GeoPandas on driver. Builds the `silver_*_regions[_h3]` and `points_*m` tables used later |
| `31_t4_diag` | T4 diag | #6 | Native ST join with/without Photon at 100K / 1M points (run on S3 native and S6) |
| `40_t5_merge_loss` | T5 | #3, #7 | Simulation merge: their broadcast of the exposure side vs broadcast events vs AQE vs clustered events table |
| `50_t8_end_to_end` | T8 | all | Whole pipeline `baseline` (theirs as-is) vs `tuned` (winners), per stage, matching their benchmark sheet |
| `90_results_summary` | — | cost | Median latency per test/variant (successful attempts only) + DBUs per job run from `system.billing.usage` × Azure list rates |

T6 (code fixes) and T7 (pricing: Jobs vs All-Purpose, spot, Photon, serverless) are not separate notebooks. They come from the
findings in `PATCHES.md`, the S6/S7 shapes and the cost roll-up in notebook 90.

**Order matters once.** `30_t4_spatial` creates the silver region tables that T5 and T8 `tuned` read.
`40_t5_merge_loss` creates `merge_input_1m` and the clustered event tables on first run. Run the serverless suite (5.2) first,
or one classic shape on its own, before launching several shapes in parallel.

### 5.2 Serverless suite

```bash
python3 scripts/run_serverless.py tests      # T1, T4 (1M + 10M points), T5 (100K + 1M), T2b (1K → 1M)
```

On serverless there are no Sedona JARs. Their Sedona Python calls resolve to Databricks' native spatial functions.
Serverless blocks `spark.sql.execution.arrow.maxRecordsPerBatch`, which their `score_with_pss` sets, so their local-PSS path (T2a/T8 PSS) cannot run there.
This is finding #3.

### 5.3 Classic compute matrix

`orchestrator/run_matrix.py` creates (or resets) a persistent job per shape called `charm_poc_<shape>` and runs it.
It waits until every run finishes and prints per-task results. Each job has up to three job clusters, which run in parallel; tasks on one cluster run in sequence:

| Cluster key | Sedona JARs | PSS maps read from |
|---|---|---|
| `fuse` | yes | UC Volume (FUSE), their current setup |
| `local` | yes | local NVMe (init script, rec #1) |
| `native` | **no** | local NVMe; spatial uses native `st_*` |

| Shape | Driver | Workers | Represents |
|---|---|---|---|
| `S0_E2ds_v6_single` | E2ds_v6 | — (single node) | their benchmark sheet tab 1 (calibration) |
| `S1_E8ds_v6_single` | E8ds_v6 | — | their benchmark sheet tab 2 (calibration) |
| `S2_lab_E4as_1xE8as` | E4as_v5 | 1 × E8as_v5 | their current LAB job cluster |
| `S3_E4as_4xE8as` | E4as_v5 | 4 × E8as_v5 | scale-out (rec #7) |
| `S4_E4as_8xE8as` | E4as_v5 | 8 × E8as_v5 | larger scale-out (needs 68 vCPU quota) |
| `S5_E48ds_v5_single` | E48ds_v5 | — | "one large VM" |
| `S6_E4as_4xE8as_photon` | as S3 | as S3 | Photon on |
| `S7_E4as_4xE8as_spot` | as S3 | as S3, spot | spot workers, on-demand driver |

```bash
source scripts/config.sh
python3 orchestrator/run_matrix.py S0_E2ds_v6_single S1_E8ds_v6_single      # calibration shapes
python3 orchestrator/run_matrix.py S2_lab_E4as_1xE8as S3_E4as_4xE8as S5_E48ds_v5_single
python3 orchestrator/run_matrix.py S6_E4as_4xE8as_photon S7_E4as_4xE8as_spot S4_E4as_8xE8as
python3 orchestrator/run_matrix.py                                         # (all shapes at once)
python3 orchestrator/run_matrix.py "S3_E4as_4xE8as:t2b+t5"                 # subset of tasks on one shape
```

Task keys per shape are in `plan()`: `t1`, `t2a_fuse`, `t2a_local`, `t2a_local_t32`, `t2b`, `t4_sedona`, `t4_native`, `t5`,
`t8_baseline`, `t8_tuned`, `t8_tuned_native`. Sizes are chosen so no run exceeds the A8 budget. S6/S7 only run the tasks Photon or spot can affect.
The jobs stay in the workspace, so you can also rerun a single task from the Jobs UI (**Run now with different settings → only selected tasks**).

The T4 diagnostic (`31_t4_diag`) is not in the matrix. Run it from the UI on an S3 `native`-style cluster and on S6 (Photon) to reproduce
the native-ST-needs-Photon result.

### 5.4 Read the results

Every measurement is a row in `<catalog>.<schema>.benchmark_results`:

| Column | Meaning |
|---|---|
| `test_id`, `variant`, `stage`, `n_rows` | what was measured |
| `seconds`, `rows_per_s` | wall-clock time for the stage (cluster start-up is **not** included; it is reported separately, A11) |
| `measure` | `measured`, or `extrapolated` (A8: sizes projected over the budget are not run; time = affine fit `a + b·n` on the last two measured sizes) |
| `code_fidelity` | `verbatim`, `verbatim+standin`, `verbatim+stub`, `verbatim+standin+reconstructed`: how much of the path is their code |
| `compute` | JSON: shape, node types, workers, Photon, spot, asset location, Sedona JARs |
| `ok` | `false` = the attempt failed (the time is time-to-crash). **Always filter on `ok`** |
| `job_run_id`, `task_run_id` | join key to `system.billing.usage` for cost |

Run `notebooks/90_results_summary` (any compute) for median latency per variant and DBUs per run, or query the table directly:

```bash
./scripts/sql.sh "SELECT test_id, coalesce(get_json_object(compute,'$.shape'),'serverless') shape, variant, stage, n_rows, measure,
                  round(percentile(seconds,0.5),1) s, count(*) runs
                  FROM ${CHARM_CATALOG}.${CHARM_SCHEMA}.benchmark_results WHERE ok
                  GROUP BY ALL ORDER BY test_id, shape, variant, stage, n_rows" | column -t -s $'\t'
```

**Cost per run** = DBUs for the job run (`system.billing.usage`) × Azure list rate (Jobs $0.30/DBU, All-Purpose $0.551/DBU as in their sheet)
\+ Azure VM pay-as-you-go $/h (Canada Central list, captured 2026-11-17 in notebook 90) × run hours. DBU/node-hour seen: E2ds_v6 0.57,
E4as_v5 1.32, E8ds_v6 2.28, E8as_v5 2.64, E48ds_v5 15.8; Photon × 2.13. Negotiated discounts are not applied (A10).

---

## 6. Replacing stand-ins with the real thing (to firm up the numbers)

| Stand-in | Replace with | How |
|---|---|---|
| `flood_model_serving` stand-in + synthetic maps (A4) | Their real wheel + maps Volume | Build/upload their wheel instead of `dist/flood_model_serving-*.whl` (edit `LIBS_PY` in `run_matrix.py`), set `FMS_STAGE_SRC` / the Volume path to their maps folder. Rerun T1, T2a, T8 |
| HTTP API stub (A5) | Hosted CGIC flood API | Run a latency + rate-limit probe in LAB. Set `latency_ms` on T2b. Cap the tuned variant's concurrency (partitions × threads) to the API's limit. Note: the stub never returns 429 and the tuned variant only retries 5xx/timeouts, so add 429 + `Retry-After` handling before pointing it at a real API |
| Synthetic book / events / regions (A1–A3) | Their tables and shapefiles | Point `inforce_book_*`, `*_simulation_events` and `CHARM_DATA_ROOT` (raw Volume) at real data; same schemas |
| Loss aggregation stand-in (A6) | Their `LossPipeline` | Not in the upload; swap `charm_bench.variants.aggregate_losses` |

---

## 7. Known issues and gotchas

* **The `native` cluster cannot run their Sedona code.** Their region scoring fails with `'JavaPackage' object is not callable`.
  Those rows are recorded with `ok=false` (sub-second times = time to crash) and must be filtered out. Dropping Sedona on classic needs a code change (finding #9).
* **Native `st_intersects` joins on classic need Photon.** S3 1M points: 1,084 s without Photon vs 2.5 s with Photon (S6).
* **`ConcurrentTransactionException`** (clustering conflict) when two T8 tasks write the same intermediate table at once. Rerun the failed task; run `t8_baseline` and `t8_tuned` sequentially.
* **First cluster start** in a fresh workspace with Maven + PyPI libraries takes ~9 min (534 s), and this is not in stage timings. Pools or serverless remove most of it.
* **`/local_disk0` is read-only on serverless.** T1 falls back to a temp dir there.
* **Their upload is mid-refactor**: it does not import as uploaded. The minimal fixes are in `PATCHES.md` (assumption A9).
* **S4** needs vCPU quota. Request it, or skip that shape.

---

## 8. Provenance

* Their code: `cgic-dbx-flood-scoring-pipeline` upload from CHARM, 2026-11-17 (`src/pyproject.upstream.toml` is their original build file).
* PoC runs: 2026-11-17 → 2026-11-18 on DBR 17.3 LTS and serverless env 4, Azure Canada Central.
* Readout: 2026-11-26, Quebec City.
