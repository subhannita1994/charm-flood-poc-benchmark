"""Function to run the flood scoring pipeline."""

from __future__ import annotations

from typing import Literal

from loguru import logger
from pyspark.sql import DataFrame

from .config import PATHS
from .geomap_scorer import GeomapScorer, GeomapScorerRules
from .flood_simulation_merge import LossPipeline
from .preprocessor import FloodInForcePreprocessor, PreprocessorRules
from .utils import read_table, save_json

OutputKeys = Literal[
    "preprocessor",
    "dropped_items",
    "imputed_items",
    "geomap_scorer",
    "filtered_items",
    "log",
    "loss_cache_repo",
]


def run_flood_pipeline(
    table: DataFrame,
    preprocessor_rules: PreprocessorRules,
    geomap_scorer_rules: GeomapScorerRules,
    as_of_date: str,
    run_name: str = "",
    output_paths: dict[OutputKeys, str] | None = None,
    custom_deductible: float | None = None,
    pss_version: str | None = None,
    batch_size: int = 1000,
    rerun_pss: bool = False,
) -> None:
    """Run the flood scoring pipeline for CGIC Home: preprocessing, geomap/PSS scoring, and loss scoring.

    Parameters
    ----------
    table : DataFrame
        Input in-force table.
    preprocessor_rules : PreprocessorRules
        Rules for the preprocessor.
    geomap_scorer_rules : GeomapScorerRules
        Rules for the geomap scorer.
    as_of_date : str
        The reference date for the scoring in the format "YYYY-MM-DD".
    run_name : str
        Name of the pipeline run.
    output_paths : dict[OutputKeys, str] | None
        Dictionary of output paths. Keys can include:
        - "preprocessor": path/table to save the preprocessor output,
        - "dropped_items": path to save the dropped items as a JSON (.json) file,
        - "imputed_items": path to save the imputed items as a JSON (.json) file,
        - "geomap_scorer": path/table to save the geomap scorer output,
        - "filtered_items": path to save the geomap/loss filtered items as a JSON (.json) file,
        - "log": path to save the combined log as a JSON (.json) file,
        - "loss_cache_repo": repository where to save the two loss caches as parquet files.
        Defaults to None: no outputs will be saved.
    custom_deductible : float | None
        Optional override passed to ``impute_deductible``.
    pss_version : str | None
        If None, the PSS version will be determined based on the as_of_date using the default mapping.
    batch_size : int
        The batch size to use for the loss scoring pipeline. Defaults to 1000.
        Recommended values are 500, 1000, or 2000.
    rerun_pss : bool
        When False and a previous "geomap_scorer" output exists, already-scored items
        are skipped and reattached to the final output instead of being rescored.

    Returns
    -------
    None
        This function does not return a value; outputs are saved to the specified paths if provided.
    """

    # Run the flood in-force preprocessor for CGIC Home ---------------------------------------------

    finf_preprocessed = FloodInForcePreprocessor(
        table=table,
        rules=preprocessor_rules,
        as_of_date=as_of_date,
        run_name=run_name,
    ).transform(
        province_shp_path=PATHS.province_shp_path,
        custom_deductible=custom_deductible,
    )

    if output_paths:
        if "preprocessor" in output_paths:
            finf_preprocessed.save_scored_inforce(output_paths["preprocessor"])
            logger.info(f"Preprocessor output saved to: {output_paths['preprocessor']}")
        if "dropped_items" in output_paths:
            finf_preprocessed.save_dropped_items(output_paths["dropped_items"])
        if "imputed_items" in output_paths:
            finf_preprocessed.save_imputed_items(output_paths["imputed_items"])

    # Run the geomap scoring pipeline for CGIC Home ---------------------------------------------

    # Reuse geomap/PSS output from a previous run unless a rerun is explicitly requested.
    previous_output = None
    if (
        not rerun_pss
        and output_paths
        and "geomap_scorer" in output_paths
        and finf_preprocessed.spark is not None
    ):
        try:
            previous_output = read_table(finf_preprocessed.spark, output_paths["geomap_scorer"])
        except Exception:  # noqa: BLE001
            logger.debug(
                f"No previous geomap scorer output found at: {output_paths['geomap_scorer']}"
            )

    # Run the geomap scorer
    finf_scored = GeomapScorer(
        table=finf_preprocessed.table,
        rules=geomap_scorer_rules,
        as_of_date=as_of_date,
        run_name=run_name,
        previous_output=previous_output,
        pss_version=pss_version,
    ).transform(
        fluvial_regions_gdf=PATHS.fluvial_regions,
        coastal_regions_gdf=PATHS.coastal_regions,
    )

    filtered_items = finf_scored.get_filtered_items()

    if output_paths:
        if "geomap_scorer" in output_paths:
            finf_scored.save_scored_inforce(output_paths["geomap_scorer"])
            logger.info(f"Geomap scorer output saved to: {output_paths['geomap_scorer']}")
        if "filtered_items" in output_paths:
            filtered_items.save_json(output_paths["filtered_items"])
            logger.info(f"Filtered items saved to: {output_paths['filtered_items']}")

    # Run the loss scoring pipeline ---------------------------------------------

    scored_losses = LossPipeline(
        table=finf_scored.table,
        as_of_date=as_of_date,
        run_name=run_name,
        batch_size=batch_size,
        cache_output_path=output_paths.get("loss_cache_repo", None) if output_paths else None,
    ).transform(
        fluvial_simulation_repo=PATHS.fluvial_simulation,
        fluvial_events_path=PATHS.fluvial_events,
        coastal_simulation_repo=PATHS.coastal_simulation,
        coastal_events_path=PATHS.coastal_events,
        financial_table=finf_preprocessed.table,
    )

    filtered_items = filtered_items.union(scored_losses.get_filtered_items())

    if output_paths:
        # Loss pipeline outputs
        if "loss_cache_repo" in output_paths:
            logger.info(f"Loss pipeline output saved to: {output_paths['loss_cache_repo']}")

        # Combine logs from all stages
        if "log" in output_paths:
            combined_log = {
                "preprocessor": finf_preprocessed.get_log(),
                "geomap_scorer": finf_scored.get_log(),
                "loss_pipeline": scored_losses.get_log(),
            }
            save_json(combined_log, output_paths["log"])
            logger.info(f"Log saved to: {output_paths['log']}")

    logger.info(f"Pipeline run '{run_name}' as of '{as_of_date}' completed successfully.")
