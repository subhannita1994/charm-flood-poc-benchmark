"""CLI command to run the end-to-end flood scoring pipeline (preprocessor, geomap/PSS, loss scoring)."""

import os
import sys

import typer
from loguru import logger
from pyspark.sql import SparkSession
from sedona.spark import SedonaContext

from cgl_dbx_flood_pipeline import (
    FloodInForceHomeCGICRules,
    GeomapScorerCGICHomeRules,
    OutputKeys,
    run_flood_pipeline,
)
from cgl_dbx_flood_pipeline.sources.cgic_home import HOME_CGIC_CONFIG

app = typer.Typer(help="Run the end-to-end flood scoring pipeline.", no_args_is_help=True)


@app.command("run")
def run_cgic_home_flood_pipeline(
    input_table: str = typer.Option(
        ..., help="Path to Unity Catalog table (catalog.schema.table)."
    ),
    as_of_date: str = typer.Option("2026-06-30", help="In-force effective date (YYYY-MM-DD)."),
    run_name: str = typer.Option(
        "test_run", help="Run identifier (e.g. Pricing_indication, ORSA, Risk_acc, test_run)."
    ),
    output_dir: str = typer.Option(
        default="/Volumes/cgic_mlai_charm_exp/mlai_charm_exp/outputs",
        help="Directory where to save all output files.",
    ),
    geomap_scorer_output_table: str | None = typer.Option(
        None,
        help="Path to Unity Catalog table where to write geomap/PSS-scored results (catalog.schema.table).\
              Skipped if not provided.",
    ),
    pss_version: str = typer.Option(
        "",
        help="PSS model version. Available choices are \"\", 'v1' or 'v1.1' and 'v2_index_100'.\
              If not provided, the default version will be inferred from the as_of_date.",
    ),
    rerun_pss: str = typer.Option(
        "False",
        help="Force rescoring geomap/PSS even if a previous 'geomap_scorer' output exists.\
              True is highly recommended if pss_version changes or if an update is made to flood maps or defenses.",
    ),
    batch_size: str = typer.Option(
        "500",
        help="Batch size for the loss scoring pipeline. Recommended values are 500, 1000, or 2000.",
    ),
    max_nrow: str = typer.Option(
        "10000",
        help="Number of rows to read from the input table. Useful for testing with a subset of the data.",
    ),
) -> None:
    """Run the full CGIC Home flood scoring pipeline.

    Reads ``input_table``, then runs preprocessing, geomap/PSS scoring, and loss
    scoring end-to-end, saving each stage's outputs to the provided paths/tables.

    Parameters
    ----------
    input_table : str
        Fully-qualified Unity Catalog table name to read (catalog.schema.table).
    as_of_date : str
        In-force effective date used in run metadata (YYYY-MM-DD).
    run_name : str
        Human-readable run identifier stored in logs.
    output_dir : str
        Directory where to save all output files.
    geomap_scorer_output_table : str | None
        Path to Unity Catalog table where to write geomap/PSS-scored results. Skipped if not provided.
    pss_version : str
        PSS model version tag. Available choices are \"\", 'v1' or 'v1.1' and 'v2_index_100'.
        If not provided, the default version will be inferred from the as_of_date.
    rerun_pss : bool
        Force rescoring geomap/PSS even if a previous 'geomap_scorer' output exists.
        True is highly recommended if pss_version changes or if an update is made to flood maps or defenses.
    batch_size : int
        Batch size used by the loss scoring pipeline. Recommended values are 500, 1000, or 2000.
    max_nrow : int
        Maximum number of rows to read from the input table. Useful for testing with a subset of the data.
    """
    # Get Spark session and initialize Sedona
    spark: SparkSession | None = SparkSession.getActiveSession()
    if spark is None:
        logger.error("No active Spark session found.")
        sys.exit(1)
    else:
        sedona: SparkSession = SedonaContext.create(spark)
        try:
            sedona.sql("SELECT ST_Point(0, 0)").count()
            sedona.read.format("shapefile").load(
                "/Volumes/cgic_mlai_charm_exp/mlai_charm_exp/flood_watersheds/can_flv_regions_v02.shp"
            ).limit(1).count()
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to initialize Sedona: {}", exc)
            sys.exit(1)
        else:
            logger.info("Sedona initialized successfully.")

    # Read input table
    logger.info("Reading input table: {}", input_table)
    try:
        finf_spark = sedona.read.table(input_table)
        if max_nrow != "" and int(max_nrow) > 0:
            finf_spark = finf_spark.limit(int(max_nrow))
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to read table '{}': {}", input_table, exc)
        sys.exit(1)

    run_name = run_name + "_CGIC_home"
    run_dir = f"{output_dir}/{run_name}"

    table_name_prefix = "cgic_mlai_charm_exp.mlai_charm_exp"

    if geomap_scorer_output_table is None or geomap_scorer_output_table.strip() == "":
        geomap_scorer_output_table = table_name_prefix + ".geomap_scorer_output_table"

    # Build the output paths dictionary from provided options
    if output_dir is not None and output_dir.strip() != "":
        if not os.path.exists(output_dir):
            logger.error("Output directory does not exist: {}", output_dir)
            sys.exit(1)
        if not os.path.exists(run_dir):
            os.makedirs(run_dir)

        candidate_paths: dict[OutputKeys, str] = {
            "preprocessor": table_name_prefix + ".preprocessor_output_table",
            "geomap_scorer": geomap_scorer_output_table,
            "dropped_items": run_dir + "/dropped_items.json",
            "imputed_items": run_dir + "/imputed_items.json",
            "filtered_items": run_dir + "/filtered_items.json",
            "log": run_dir + "/log.json",
            "loss_cache_repo": run_dir + f"/losses_as_of_{as_of_date}",
        }
        output_paths: dict[OutputKeys, str] | None = {
            key: value for key, value in candidate_paths.items() if value
        }
    else:
        output_paths = None

    # Run the end-to-end pipeline
    logger.info("Running flood scoring pipeline — as_of_date={}, run_name={}", as_of_date, run_name)

    run_flood_pipeline(
        table=finf_spark,
        preprocessor_rules=FloodInForceHomeCGICRules(),
        geomap_scorer_rules=GeomapScorerCGICHomeRules(),
        as_of_date=as_of_date,
        run_name=run_name,
        output_paths=output_paths or None,
        custom_deductible=HOME_CGIC_CONFIG.default_deductible,
        pss_version=pss_version,
        batch_size=int(batch_size),
        rerun_pss=bool(rerun_pss),
    )

    logger.info("Flood scoring pipeline completed successfully.")
