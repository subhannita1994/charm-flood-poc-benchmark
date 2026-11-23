# PoC Tests
**Purpose.** Prove (or disprove) the Oct 15 recommendations on CHARM's own flood-scoring code, with numbers we can quote at the Nov 26 readout: "applying recommendation X changed latency/cost from A to B." Three checkpoints: (1) test plan — approved, (2) test results — in progress, (3) target-state patterns — after results.
**Status TLDR:** All 8 compute setups have run and cost per run is computed. With every tuned change applied, the 1M book scores in **~13 min on S3 (4 × E8as) for ~$1.28** at Jobs rates (~$0.96 with spot workers), vs a projected ~10 hours for their unchanged code on the Lab cluster. The biggest wins are staging the PSS maps to local disk (rec #1, ~10x on their PSS path) and tying partitions and threads to the cluster (rec #7, ~115x on the HTTP-API path). The suspiciously fast region-scoring times seen on classic clusters without Sedona turned out to be failed attempts (their code crashes without the Sedona JARs); they are excluded and no quoted number changes.
**Read every number below as preliminary.** It comes from synthetic data and stand-ins for components Cooperators could not share (see Assumptions), and from single runs unless noted. Relative effects (before vs after a change on the same hardware) are the claim, not absolute minutes.
## 1. How the tests are being conducted
### Environment
* **Azure classic FEVM workspace** (adb-7405612043532077), so we test the exact VM types CHARM uses (Standard_E2ds_v6, E8ds_v6, E4as_v5, E8as_v5, E48ds_v5) at Azure prices. The same workspace also runs serverless jobs.
* Data and results: Unity Catalog `catalog_8as5lj_bvv7nu.charm_poc` (table `benchmark_results`, one row per test, variant, stage and size).
* Code and notebooks: `CHARM PoC/charm_benchmark/` in the FEVM workspaces. Each compute setup is a saved job named `charm_poc_<setup>`, so CHARM can rerun it.
* DBR 17.3 LTS, Photon off unless stated, Jobs compute, the same Sedona 1.9.0 version their job uses.
### Code under test
* **Their code (flood scoring pipeline), unchanged wherever it runs.** This covers the preprocessor, the GeomapScorer (region scoring and PSS scoring), `score_with_pss`, the PR18 HTTP-API execution and the simulation merge.
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

| **#** | **Assumption** | **Value** |
|---|---|---|
| A1 | Book of business | 1M CGIC Home policies in their raw source schema, clustered around 40 Canadian cities, with dirty rows injected so every preprocessor filter does work |
| A2 | Event set | 50,000 simulated years × 20 events/year = 1M events per peril (fluvial, coastal) |
| A3 | Region polygons | 5,000 fluvial + 1,000 coastal synthetic watershed polygons (Voronoi cells with densified edges) |
| A3b | Province polygons | Synthetic rectangles (the StatCan download is blocked from the PoC workspace) |
| A4 | PSS maps | 2 rasters, 24 multi-band depth tiles, 5 vector layers, damage curves (~1.1 GB) |
| A8 | No long reproductions | Any run projected over 15 minutes is not executed. It is extrapolated with a straight-line fit (fixed cost plus per-row cost) on the last two measured sizes, and labelled "extrapolated" |

### Test matrix (each test maps to a recommendation)

| **Test** | **Recommendation** | **What we compare** |
|---|---|---|
| T1 Import and asset load | #1 Stage files to local disk | Package and PSS maps read from the UC Volume vs copied to local disk; also files kept open between calls |
| T2a PSS scoring, local library (their benchmarked path) | #1, #5, #7 | Their score_with_pss with maps on the Volume vs on local disk; threads and partitions; cluster shapes |
| T2b PSS scoring, HTTP API (PR18 target state) | #7 Scale out and confirm parallelism | Their uploaded execution code vs their scratch-notebook version vs a tuned version (partitions tied to cores, threads tied to API latency) |
| T3 Scale-out vs one big node | #7 | The same tests across 8 compute setups (below) |
| T4 Region scoring (points in polygons) | #2 Shapefiles to Delta, #4 Bounding-box filter, #6 Engine choice | Their code reading the shapefile vs reading Delta; bounding-box pre-filter; native Databricks spatial SQL; H3 index join; GeoPandas on the driver |
| T5 Simulation merge and loss | #3, #7 | Their merge (forces broadcast of the 7M-row exposure side) vs broadcasting the events side vs letting Spark choose vs clustered event table; Photon on/off |
| T6 Code fixes | — | The rerun_pss bug and driver-side .collect() calls |
| T7 Compute and pricing | Cost | Jobs vs All-Purpose rate; spot workers; Photon; serverless |
| T8 End to end | All | Their full pipeline as-is ("baseline") vs with the winning changes ("tuned"), per stage, matching the rows of their benchmark sheet |

### Compute setups

| **Setup** | **Driver** | **Workers** | **Why** |
|---|---|---|---|
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
## 2. Results so far - Nov 18
### T1 — Rec #1, stage PSS maps to local disk: worked (large effect)
Per-row `predict()` time with their service builder (stand-in maps), measured over 199 calls:

| **Setup** | **Maps on UC Volume** | **Maps on local disk** | **Local disk, files kept open** |
|---|---|---|---|
| S0 E2ds_v6 | 1.35 s/row | 16 ms/row (~80x faster) | 2 ms/row |
| S1 E8ds_v6 | 0.82 s/row | 15 ms/row (~55x faster) | 2 ms/row |

* Service start-up: 7.7 s from the Volume vs 1.7 s from local disk.
* The import-time existence checks cost about 0.5 s on the Volume and ~0 locally. Importing their package from a wheel instead of a `/Workspace` path saves ~1.5 s.
* Copying the 1.16 GB of maps from the Volume to local disk takes **8–11 s** (done by a cluster init script, with no code change).
* "Files kept open" means opening each raster once and reusing it, instead of reopening it on every call. It is a further 5–8x but needs a change inside their `flood_model_serving` library, so we will raise it as a library recommendation.
### T2a — PSS scoring with their `score_with_pss` (local library path)
10,000 policies, measured:

| **Setup** | **Maps on UC Volume** | **Maps on local disk** | **Change** |
|---|---|---|---|
| S2 Lab (1 × E8as worker) | 332 s | 34.7 s | −90% |
| S3 (4 × E8as workers) | 83.5 s | 10.5 s | −87% |
| S5 one E48ds_v5 node | 107.8 s at 1,000 rows; 10,000 skipped (over budget) | 1.8 s at 1,000 rows | ~60x at 1,000 rows |

Scale-out with maps on local disk (rec #7). 100,000 policies measured; 1M policies extrapolated:

| **Setup** | **Cores doing work** | **100K measured** | **1M extrapolated** |
|---|---|---|---|
| S2 Lab (1 × E8as) | 8 | 327 s | ~54 min |
| S3 (4 × E8as) | 32 | 89 s | ~14.5 min |
| S5 one E48ds_v5 node | 48 | 81 s | ~13.3 min |
| S4 (8 × E8as) | 64 | 69 s | ~11 min |

* On the Volume, the big single node is the worst case. Its 48 cores contend on the same FUSE mount: 108 s for 1,000 rows on S5 vs 35 s on S2.
* Raising threads and partitions (4 partitions per core, 32 threads) **did not help** on S2, S3 or S5 (±2%) but cut S4 (8 workers) from ~11 to ~8.3 min (−24%). Once the maps are local, the stand-in PSS is mostly bound by compute; extra concurrency only pays on the largest cluster.
* Rec #5 (Pandas UDF instead of `mapInPandas`): `score_with_pss` already streams Arrow batches through `mapInPandas`, so the signature change has nothing to gain. We will report this one as "does not apply".
### T2b — PSS scoring through the HTTP API (PR18): worked (largest effect so far)
1M policies, stub at 49 ms/call:

| **Setup** | **Their uploaded code** | **Their scratch-notebook version** | **Tuned (partitions × threads)** |
|---|---|---|---|
| S2 Lab (1 × E8as) | ~6,455 s, extrapolated (644 s at 100K) | 814 s | 221 s (16 × 32) |
| S3 (4 × E8as) | ~6,457 s, extrapolated | 407 s | 56 s (64 × 32) |
| S5 one E48ds_v5 node | ~6,464 s, extrapolated | 407 s | 38 s (96 × 32) |
| Serverless | ~6,535 s, extrapolated | 558 s | 28.5 s (128 × 32) |
| S4 (8 × E8as) | ~6,459 s, extrapolated | 409 s | 29 s (128 × 32) |

* Their uploaded code takes **the same ~1.8 hours on every cluster**. It never repartitions, so concurrency stays fixed no matter how many cores are added. This is the clearest proof that rec #7 matters.
* Their scratch version is capped at 16 partitions (`repartition(max_workers*2)`), so it stops improving beyond 4 workers.
* The tuned version scales with cores: 221 s on S2, 56 s on S3, 29 s on S4. S3 is **~7x faster than scratch and ~115x faster than their uploaded code** for the 1M book.
* **Caveat.** These rates assume the real API can absorb the concurrency: ~1,700 calls in flight on serverless, ~870 on S3. The tuned version's concurrency must be set from the API's actual rate limit. We will give CHARM a one-cell latency probe to run in LAB.
### T4 — Region scoring, points in polygons: recs #2 and #6 worked, #4 partly
On serverless (no Sedona JARs):

| **Variant** | **1M points** | **10M points** |
|---|---|---|
| Their code, reading the shapefile from the Volume | 12.1 s | 13.6 s |
| Their same code, polygons read from Delta (rec #2) | 4.5 s (−63%) | 10.3 s (−24%) |
| H3 index join, exact test only on boundary cells (rec #6) | 3.6 s | 6.5 s (−52%) |
| Native spatial SQL, broadcast join | 15.4 s | 36.9 s |
| GeoPandas on the driver (their VM approach) | 13.6 s | 54.8 s |

* Parity: the native join matches their result exactly. The H3 join differs on 15 of 1M points (0.0015%, points on polygon edges).
* Their Sedona-based code ran on serverless **without the Sedona JARs**, because Databricks' native spatial functions answered the same calls. On classic clusters the same code fails without the JARs (section 3, finding 9), so dropping Sedona on classic needs a small code change.
* On classic with the Sedona JARs (their setup), local maps:

| **Variant** | **S3 (4 × E8as) 1M** | **S3 10M** | **S2 Lab 1M** | **S2 Lab 10M** |
|---|---|---|---|---|
| Their code, shapefile from the Volume | 42.8 s | 29.7 s | 40.7 s | 35.5 s |
| Their same code, polygons from Delta (rec #2) | 20.7 s (−52%) | 19.6 s (−34%) | 22.0 s (−46%) | 28.9 s (−19%) |
| Delta + bounding-box pre-filter (rec #4) | 10.9 s | 54.6 s | 20.5 s | 140.2 s |
| GeoPandas on the driver | 5.3 s | 38.5 s | 6.0 s | 37.4 s |

* Reading polygons from Delta instead of the shapefile (rec #2) cuts 1M points from 43 s to 21 s on S3 (−52%) and from 41 s to 22 s on S2 (−46%).
* The bounding-box pre-filter (rec #4) halves it again at 1M (11 s on S3) but is slower at 10M (55 s vs 20 s), because the grid join multiplies rows. **Partly worked**: the H3 join below does the same job without the blow-up.
* GeoPandas on the driver is fastest at 1M (5 s) and finishes 10M in ~38 s on a 32 GB driver (no out-of-memory here), but it cannot scale out, so it is a fallback, not the target.
* H3 index join (rec #6) on classic does not need Photon: 5.9 s at 1M and 9.7 s at 10M on S3; 3.1 s and 3.7 s with Photon (S6).
* Native spatial SQL on classic: 1M points took 6,698 s on S2 without Photon vs 16.9 s on S6 with Photon. Confirmed on the same 4-worker shape (S3 vs S6), same query and same matches: 1M points take 1,084 s without Photon and 2.5 s with Photon (100K: 135 s vs 16.6 s). Native spatial SQL on classic needs Photon.
### T5 — Simulation merge (50K-year event join) and loss: worked
On serverless, 1M policies:

| **Variant** | **Merge + loss** |
|---|---|
| Their code (forces broadcast of the exposure side) | 16.9 s |
| Broadcast the events side | 10.4 s (−38%) |
| No hint, Spark chooses (AQE) | 9.9 s (−41%) |
| Events table clustered by region | 10.0 s (−41%) |

On classic (1M policies) the fix holds: S2 Lab 21.8 s → 12.4 s (−43%), S3 14.0 s → 9.1 s (−35%), S5 12.7 s → 7.3 s (−43%). Photon (S6) brings their code to 7.4 s and the fixed merge to 6.0 s. Clustering the event table gave no further gain at this size.
### T8 — End to end (calibration, all setups, cost per run)
* **Calibration on S0** (their E2ds_v6, 1,000 policies), baseline vs their sheet:

| **Stage** | **Ours** | **Their sheet** |
|---|---|---|
| Preprocessor | 52–62 s | 43 s |
| Regions + PSS | 6–7 min | 97 min |
| Merge/loss | 17–20 s | 8 min |

  The preprocessor matches their sheet closely. PSS and merge are much faster than theirs because our stand-in maps are lighter. Projections to 1M from S0 are not reliable (fits on 10–1,000 rows are dominated by start-up noise), so S0 is quoted only qualitatively: the baseline adds ~0.3 s per policy in regions + PSS (a 1M book would take days, consistent with their "stopped after 6 h"); the tuned version does 10,000 policies in ~4 min.
* **Tuned run on S3** (4 × E8as, maps on local disk, polygons from Delta, fixed merge). Measured at 1K, 10K and 100K; 1M extrapolated:

| **Stage** | **1M extrapolated** |
|---|---|
| Preprocessor | 47 s |
| Regions + PSS | 727 s |
| Merge + loss | 23 s |
| **Total** | **~13 min, vs ~1 hour on their VM** |

  **All setups, tuned, 1M policies, with cost per run** (1M extrapolated from 100K; S0 and S1 from 10K):

| **Setup** | **Time (1M, extrapolated)** | **Jobs compute** | **All-Purpose rate (their sheet)** |
|---|---|---|---|
| S0 E2ds_v6 (their sheet #1) | ~3 h (rough) | ~$1.03 | ~$1.46 |
| S1 E8ds_v6 (their sheet #2) | ~43 min | $1.01 | $1.42 |
| S2 Lab (1 × E8as) | ~44 min | $1.41 | $2.14 |
| **S3 (4 × E8as)** | **~13 min** | **$1.28** | $1.94 |
| S4 (8 × E8as) | ~10 min | $1.75 | $2.65 |
| S5 one E48ds_v5 node | ~11 min | $1.57 | $2.31 |
| S6 = S3 + Photon | ~11.5 min | $1.89 | $3.10 |
| **S7 = S3 + spot workers** | ~14 min | **~$0.96** | — |

**Faster costs about the same.** Every tuned setup scores 1M policies for roughly $1–2, because more workers finish proportionally sooner. Best value: S3 with spot workers (~14 min, under $1). Photon roughly doubles the DBU rate for ~2 min saved. One large node (S5) is close to scale-out on speed but ~20% more expensive than S3.
**Baseline:** their unchanged code on the Lab cluster (S2) projects to ~10 hours for 1M (rough: measured only up to 1,000 rows), consistent with their "stopped after 6 h" run.
Method: DBUs per node-hour from the billing table on our own runs (E4as_v5 1.32, E8as_v5 2.64, E2ds_v6 0.57, E8ds_v6 2.28, E48ds_v5 15.8; Photon ×2.13), × $0.30/DBU Jobs or $0.551/DBU All-Purpose, plus Azure VM list prices for Canada Central (spot price for S7 workers). Excludes ~5 min of cluster start-up.
## 3. Findings in CHARM's code (to share, not patched)
* **The PSS reuse cache never works.** `rerun_pss=bool(rerun_pss)`, and `bool("False")` is `True`, so every run re-scores PSS.
* **`.limit(max_nrow)` collapses the input to one partition** before scoring.
* **Their PSS scoring cannot run on serverless.** `score_with_pss` and the scratch API code set `spark.sql.execution.arrow.maxRecordsPerBatch`, which serverless blocks.
* **The uploaded HTTP execution code has no retry and no repartition**, so it does not scale with the cluster (T2b).
* **The simulation merge broadcasts the large side** (policies × 7 return periods) instead of the events side.
* **Shapefiles are read with GeoPandas on the driver from the Volume on every run.**
* **The preprocessor collects one diagnostics row per policy to the driver**, plus several lists of removed IDs.
* **The snapshot does not import as uploaded** (mid-PR18 refactor). See the fixes listed in section 1.
* **Dropping Sedona on classic needs a code change.** Their region and geometry code calls Sedona's Python API (`ST_Point`, `ST_Intersects`). On a classic cluster without the Sedona JARs it fails ("'JavaPackage' object is not callable"); on serverless the same calls resolve to Databricks' native functions.
## 4. Running now and next steps
* Running now:
* Nothing running; all planned runs are complete, including the Photon check for native spatial SQL (T4).
* Next:
* TCO vs their VM: needs their VM size, node count and run time for a full run (see section 5).
* A verdict per recommendation (worked / partly / did not work / does not apply).
* Gate 2 review.
* Then: target-state patterns written into the Recommendations tab, and the TCO for the Nov 26 readout.
## 5. Asks for Cooperators (would firm up the numbers)
* `flood_model_serving` `map_lookup.py`, plus a file listing with sizes of the PSS maps folder (needs read access to their Volume).
* A consistent commit of `cgl_dbx_flood_pipeline` (the upload is mid-refactor), including `config/pss_versions.py`, `config/config_flood_risks.py` and the loss pipeline.
* Real book size and event-set row counts (to replace A1 and A2).
* The flood API's latency and rate limit measured from LAB (one-cell probe we will provide).
* Their VM's size, node count and wall-clock time for a full 1M run, for a like-for-like TCO.
