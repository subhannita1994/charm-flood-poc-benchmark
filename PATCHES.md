# Changes to Cooperators' code in `src/cgl_dbx_flood_pipeline` (CHARM PoC copy)

Upstream snapshot: `CHARM PoC/cgic-dbx-flood-scoring-pipeline` (uploaded 2026-11-17, mid-PR18 refactor). That folder is never modified.
Every change below is tagged in code with `CHARM-POC PATCH` (fix to make it run) or `CHARM-POC RECONSTRUCTED` (module/field
referenced by the code but absent from the upload; rebuilt from how the code uses it — assumption **A9**).
Performance *variants* are never patched into this copy; they live in `harness/src/charm_bench/variants.py`.

| File | Kind | Change | Why |
|---|---|---|---|
| `sources/cgic_home.py` | PATCH | `"item_id_m": GLOBAL` -> `"item_id_m": "item_id"` | `GLOBAL` is undefined (NameError on import) |
| `sources/cgic_home.py` | RECONSTRUCTED | `HomeCGICConfig.schema / default_deductible / possible_deductibles / max_deductible` | read by `preprocessor_rules_cgic_home.py`, absent upstream; values are assumptions |
| `config/config_global.py` | RECONSTRUCTED | `GlobalConfig.schema` | read by `geomap_scorer.py` |
| `config/config_flood_risks.py` | RECONSTRUCTED | `RISK_CONFIGS`, `RiskType` | imported by `geomap_scorer.py`, file absent |
| `config/pss_versions.py` | RECONSTRUCTED | `PSSVersionsMapping`, `PSS_VERSIONS_MAP_CGIC_HOME` (v1, v1_1) | imported by `score_pss.py`, file absent; release dates assumed |
| `config/config_unity_catalog_paths.py` | PATCH + RECONSTRUCTED | root from env `CHARM_DATA_ROOT`; `fluvial_events`/`coastal_events` fields | point at PoC volume; fields referenced by `run_pipeline.py` |
| `flood_pipeline/flood_pipeline.py` | PATCH | `GEOMETRY_COL` class constant restored | PR18 removed it; preprocessor/geomap scorer read `self.GEOMETRY_COL` (AttributeError) |
| `flood_pipeline/flood_pipeline.py` | PATCH | `to_geodataframe = _to_geodataframe` alias | PR18 renamed it; preprocessor/geomap scorer still call the old name |

Not reconstructed (not needed by the tests, flagged for Cooperators): `LossPipeline` / `loss_pipeline` module referenced by
`run_pipeline.py` and `__init__.py`; loss aggregation is replaced by stand-in `charm_bench.variants.aggregate_losses` (A6).

## Code findings (reported to CHARM, not patched)
1. `commands/run_cgic_home_flood_pipeline.py`: `rerun_pss=bool(rerun_pss)` -> `bool("False") is True`, so PSS is **always** rescored and the reuse cache never works.
2. Same file: `.limit(max_nrow)` before scoring collapses the input to a single partition.
3. `preprocessor_rules_cgic_home.map_policy_center_codes`: `.collect()` of a diagnostics row for **every** policy to the driver.
4. `flood_simulation_merge.join_simulations`: broadcasts the **exposure** side (n_policies x 7 knots) instead of the events side.
5. `utils/io_utils.resolve_polygon`: region shapefiles read with GeoPandas on the driver from the Volume on every run.
6. `FloodPipeline.set_index`: `distinct().count()` + `count()` + null scan = 3 full passes per stage.
7. Several stages `.collect()` lists of removed item IDs to the driver for logging.
