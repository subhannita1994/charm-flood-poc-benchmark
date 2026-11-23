# PoC Tests — progress update (Nov 17, 2026, evening)

**Purpose.** Prove (or disprove) the Oct 15 recommendations on CHARM's own flood-scoring code, with numbers we can quote at the Nov 26 readout: "applying recommendation X changed latency/cost from A to B." Three checkpoints: (1) test plan — approved, (2) test results — in progress, (3) target-state patterns — after results.

**Status in one line.** Environment, synthetic data, harness and all test notebooks are built and validated; roughly half of the test matrix has run. The remaining compute setups and the cost roll-up are running and should land by tomorrow (Nov 18).

**Read every number below as preliminary.** It comes from synthetic data and stand-ins for components Cooperators could not share (see Assumptions), and from single runs unless noted. Relative effects (before vs after a change on the same hardware) are the claim, not absolute minutes.

## 1. How the tests are being conducted

### Environment

* **Azure classic FEVM workspace** (adb-7405612043532077), so we test the exact VM types CHARM uses (Standard_E2ds_v6, E8ds_v6, E4as_v5, E8as_v5, E48ds_v5) at Azure prices. The same workspace also runs serverless jobs.
* Data and results: Unity Catalog `catalog_8as5lj_bvv7nu.charm_poc` (table `benchmark_results`, one row per test, variant, stage and size).
* Code and notebooks: `CHARM PoC/charm_benchmark/` in the FEVM workspaces. Each compute setup is a saved job named `charm_poc_<setup>`, so CHARM can rerun it.
* DBR 17.3 LTS, Photon off unless stated, Jobs compute, the same Sedona 1.9.0 version their job uses.

### Code under test

* **Their code, unchanged wherever it runs.** This covers the preprocessor, the GeomapScorer (region scoring and PSS scoring), `score_with_pss`, the PR18 HTTP-API execution and the simulation merge.
* The uploaded snapshot is mid-refactor and does not run as-is. We made the minimum fixes to get it running, each tagged in the code and listed in a changes file:
  * an undefined name in `sources/cgic_home.py`
  * a removed `GEOMETRY_COL` constant
  * a renamed `to_geodataframe`
  * missing `pss_versions.py`, `config_flood_risks.py`, schema/deductible config and event paths
* Performance changes are never patched into their code. They run as separate variants next to it.

### Stand-ins (components we do not have)

* **PSS library (`flood_model_serving`).** Not available: the package and maps sit in a Volume we cannot read. We built a stand-in with the same modules and classes. It reproduces the I/O pattern:
  * file-existence checks at import
  * reading vector layers at service start
  * per-row raster and tile reads plus point-in-polygon lookups, against 1.1 GB of synthetic maps
* **Hosted flood API (PR18 path).** An in-process stub at 49 ms per call (the VM's observed rate: 48.7 s per 1,000 rows). Their request building and response parsing run unchanged.
* **Loss aggregation (`LossPipeline`).** Referenced by their code but not uploaded. Replaced by a simple AAL / max-loss aggregation.

### Synthetic data (assumptions, stated in every result)

| # | Assumption | Value |
| --- | --- | --- |
| A1 | Book of business | 1M CGIC Home policies in their raw source schema, clustered around 40 Canadian cities, with dirty rows injected so every preprocessor filter does work |
| A2 | Event set | 50,000 simulated years × 20 events/year = 1M events per peril (fluvial, coastal) |
| A3 | Region polygons | 5,000 fluvial + 1,000 coastal synthetic watershed polygons (Voronoi cells with densified edges) |
| A3b | Province polygons | Synthetic rectangles (the StatCan download is blocked from the PoC workspace) |
| A4 | PSS maps | 2 rasters, 24 multi-band depth tiles, 5 vector layers, damage curves (~1.1 GB) |
| A8 | No long reproductions | Any run projected over 15 minutes is not executed. It is extrapolated with a straight-line fit (fixed cost plus per-row cost) on the last two measured sizes, and labelled "extrapolated" |

### Test matrix (each test maps to a recommendation)

| Test | Recommendation | What we compare |
| --- | --- | --- |
| T1 Import and asset load | #1 Stage files to local disk | Package and PSS maps read from the UC Volume vs copied to local disk; also files kept open between calls |
| T2a PSS scoring, local library (their benchmarked path) | #1, #5, #7 | Their `score_with_pss` with maps on the Volume vs on local disk; threads and partitions; cluster shapes |
| T2b PSS scoring, HTTP API (PR18 target state) | #7 Scale out and confirm parallelism | Their uploaded execution code vs their scratch-notebook version vs a tuned version (partitions tied to cores, threads tied to API latency) |
| T3 Scale-out vs one big node | #7 | The same tests across 8 compute setups (below) |
| T4 Region scoring (points in polygons) | #2 Shapefiles to Delta, #4 Bounding-box filter, #6 Engine choice | Their code reading the shapefile vs reading Delta; bounding-box pre-filter; native Databricks spatial SQL; H3 index join; GeoPandas on the driver |
| T5 Simulation merge and loss | #3, #7 | Their merge (forces broadcast of the 7M-row exposure side) vs broadcasting the events side vs letting Spark choose vs clustered event table; Photon on/off |
| T6 Code fixes | — | The `rerun_pss` bug and driver-side `.collect()` calls |
| T7 Compute and pricing | Cost | Jobs vs All-Purpose rate; spot workers; Photon; serverless |
| T8 End to end | All | Their full pipeline as-is ("baseline") vs with the winning changes ("tuned"), per stage, matching the rows of their benchmark sheet |

### Compute setups

| Setup | Driver | Workers | Why |
| --- | --- | --- | --- |
| S0 | E2ds_v6 (2 cores, 16 GB), single node | — | Their benchmark sheet, tab 1 |
| S1 | E8ds_v6 (8 cores, 64 GB), single node | — | Their benchmark sheet, tab 2 |
| S2 | E4as_v5 | 1 × E8as_v5 | Their current LAB job cluster |
| S3 | E4as_v5 | 4 × E8as_v5 | Scale-out (rec #7) |
| S4 | E4as_v5 | 8 × E8as_v5 | Scale-out, larger |
| S5 | E48ds_v5 (48 cores, 384 GB), single node | — | "One large VM" comparison |
| S6 | as S3 | as S3 | Photon on |
| S7 | as S3 | as S3, spot | Spot workers, on-demand driver |
| Serverless | — | — | Serverless jobs (no Sedona JARs; native spatial functions) |

Method:
* Same input tables for every setup.
* Cluster start-up is reported separately from compute time.
* Output parity is checked between their code and each faster variant.
* Cost per run will come from the billing system table (DBUs per job run) × Azure list rates, plus Azure VM list prices for Canada Central.

## 2. Results so far

### T1 — Rec #1, stage PSS maps to local disk: **worked (large effect)**

Per-row `predict()` time with their service builder (stand-in maps), measured over 199 calls:

| Setup | Maps on UC Volume | Maps on local disk | Local disk, files kept open |
| --- | --- | --- | --- |
| S0 E2ds_v6 | 1.35 s/row | 16 ms/row (~80x faster) | 2 ms/row |
| S1 E8ds_v6 | 0.82 s/row | 15 ms/row (~55x faster) | 2 ms/row |

* Service start-up: 7.7 s from the Volume vs 1.7 s from local disk.
* The import-time existence checks cost about 0.5 s on the Volume and ~0 locally. Importing their package from a wheel instead of a `/Workspace` path saves ~1.5 s.
* Copying the 1.16 GB of maps from the Volume to local disk takes **8–11 s** (done by a cluster init script, with no code change).
* "Files kept open" means opening each raster once and reusing it, instead of reopening it on every call. It is a further 5–8x but needs a change inside their `flood_model_serving` library, so we will raise it as a library recommendation.

### T2a — PSS scoring with their `score_with_pss` (local library path)

10,000 policies, measured:

| Setup | Maps on UC Volume | Maps on local disk | Change |
| --- | --- | --- | --- |
| S2 Lab (1 × E8as worker) | 332 s | 34.7 s | −90% |
| S3 (4 × E8as workers) | 83.5 s | 10.5 s | −87% |
| S5 one E48ds_v5 node | 107.8 s at 1,000 rows; 10,000 skipped (over budget) | 1.8 s at 1,000 rows | ~60x at 1,000 rows |

Scale-out with maps on local disk (rec #7). 100,000 policies measured; 1M policies extrapolated:

| Setup | Cores doing work | 100K measured | 1M extrapolated |
| --- | --- | --- | --- |
| S2 Lab (1 × E8as) | 8 | 327 s | ~54 min |
| S3 (4 × E8as) | 32 | 89 s | ~14.5 min |
| S5 one E48ds_v5 node | 48 | 81 s | ~13.3 min |

* On the Volume, the big single node is the worst case. Its 48 cores contend on the same FUSE mount: 108 s for 1,000 rows on S5 vs 35 s on S2.
* Raising threads and partitions (4 partitions per core, 32 threads) **did not help** on this path (±1%). Once the maps are local, the stand-in PSS is bound by compute, not I/O.
* Rec #5 (Pandas UDF instead of `mapInPandas`): `score_with_pss` already streams Arrow batches through `mapInPandas`, so the signature change has nothing to gain. We will report this one as "does not apply".

### T2b — PSS scoring through the HTTP API (PR18): **worked (largest effect so far)**

1M policies, stub at 49 ms/call:

| Setup | Their uploaded code | Their scratch-notebook version | Tuned (partitions × threads) |
| --- | --- | --- | --- |
| S2 Lab (1 × E8as) | ~6,455 s, extrapolated (644 s at 100K) | 814 s | 221 s (16 × 32) |
| S3 (4 × E8as) | ~6,457 s, extrapolated | 407 s | 56 s (64 × 32) |
| S5 one E48ds_v5 node | ~6,464 s, extrapolated | 407 s | 38 s (96 × 32) |
| Serverless | ~6,535 s, extrapolated | 558 s | 28.5 s (128 × 32) |

* Their uploaded code takes **the same ~1.8 hours on every cluster**. It never repartitions, so concurrency stays fixed no matter how many cores are added. This is the clearest proof that rec #7 matters.
* Their scratch version is capped at 16 partitions (`repartition(max_workers*2)`), so it stops improving beyond 4 workers.
* The tuned version scales with cores. S3 is **~7x faster than scratch and ~115x faster than their uploaded code** for the 1M book.
* **Caveat.** These rates assume the real API can absorb the concurrency: ~1,700 calls in flight on serverless, ~600 on S3. The tuned version's concurrency must be set from the API's actual rate limit. We will give CHARM a one-cell latency probe to run in LAB.

### T4 — Region scoring, points in polygons (serverless): **recs #2 and #6 worked**

| Variant | 1M points | 10M points |
| --- | --- | --- |
| Their code, reading the shapefile from the Volume | 12.1 s | 13.6 s |
| Their same code, polygons read from Delta (rec #2) | 4.5 s (−63%) | 10.3 s (−24%) |
| H3 index join, exact test only on boundary cells (rec #6) | 3.6 s | 6.5 s (−52%) |
| Native spatial SQL, broadcast join | 15.4 s | 36.9 s |
| GeoPandas on the driver (their VM approach) | 13.6 s | 54.8 s |

* Parity: the native join matches their result exactly. The H3 join differs on 15 of 1M points (0.0015%, points on polygon edges).
* Their Sedona-based code ran on serverless **without the Sedona JARs**, because Databricks' native spatial functions answered the same calls. We are testing on classic whether Sedona can be dropped entirely.
* Classic results with Sedona JARs, and the bounding-box variant (rec #4), are running.

### T5 — Simulation merge (50K-year event join) and loss (serverless): **worked**

1M policies:

| Variant | Merge + loss |
| --- | --- |
| Their code (forces broadcast of the exposure side) | 16.9 s |
| Broadcast the events side | 10.4 s (−38%) |
| No hint, Spark chooses (AQE) | 9.9 s (−41%) |
| Events table clustered by region | 10.0 s (−41%) |

The classic and Photon comparison is running.

### T8 — End to end (calibration and first tuned projection)

* **Calibration on S0** (their E2ds_v6, 10 policies), baseline vs their sheet:

  | Stage | Ours | Their sheet |
  | --- | --- | --- |
  | Preprocessor | 83 s | 19 s |
  | Regions + PSS | 126 s | 11 min |
  | Merge/loss | 13 s | 3 min |

  The shape matches (huge fixed cost at tiny sizes). Absolute PSS time is lower because our stand-in maps are lighter than theirs. The 100- and 1,000-row calibration points are re-running.
* **First tuned projection on S3** (4 × E8as, maps on local disk, polygons from Delta, fixed merge). Measured at 1K, 10K and 100K; 1M extrapolated:

  | Stage | 1M extrapolated |
  | --- | --- |
  | Preprocessor | 47 s |
  | Regions + PSS | 727 s |
  | Merge + loss | 23 s |
  | **Total** | **~13 min**, vs ~1 hour on their VM |

  Cost per run is not yet computed.

## 3. Findings in CHARM's code (to share, not patched)

1. **The PSS reuse cache never works.** `rerun_pss=bool(rerun_pss)`, and `bool("False")` is `True`, so every run re-scores PSS.
2. **`.limit(max_nrow)` collapses the input to one partition** before scoring.
3. **Their PSS scoring cannot run on serverless.** `score_with_pss` and the scratch API code set `spark.sql.execution.arrow.maxRecordsPerBatch`, which serverless blocks.
4. **The uploaded HTTP execution code has no retry and no repartition**, so it does not scale with the cluster (T2b).
5. **The simulation merge broadcasts the large side** (policies × 7 return periods) instead of the events side.
6. **Shapefiles are read with GeoPandas on the driver from the Volume on every run.**
7. **The preprocessor collects one diagnostics row per policy to the driver**, plus several lists of removed IDs.
8. **The snapshot does not import as uploaded** (mid-PR18 refactor). See the fixes listed in section 1.

## 4. Running now and next steps

* Running now:
  * Reruns of T4 (classic, with and without Sedona), T5 and T8 on S0, S1, S2, S3 and S5.
  * Then S6 (Photon) and S7 (spot).
  * Then S4 (8 workers), which waits for vCPU quota.
* Next:
  * Cost per run from billing, at Jobs and All-Purpose rates, vs their VM.
  * Finish the T8 calibration points.
  * A verdict per recommendation (worked / partly / did not work / does not apply).
  * Gate 2 review.
* Then: target-state patterns written into the Recommendations tab, and the TCO for the Nov 26 readout.

## 5. Asks for Cooperators (would firm up the numbers)

* `flood_model_serving` `map_lookup.py`, plus a file listing with sizes of the PSS maps folder (needs read access to their Volume).
* A consistent commit of `cgl_dbx_flood_pipeline` (the upload is mid-refactor), including `config/pss_versions.py`, `config/config_flood_risks.py` and the loss pipeline.
* Real book size and event-set row counts (to replace A1 and A2).
* The flood API's latency and rate limit measured from LAB (one-cell probe we will provide).
