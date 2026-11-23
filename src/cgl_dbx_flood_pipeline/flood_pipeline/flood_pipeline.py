"""Shared base class for all flood pipeline components.

This module provides common table I/O, geometry conversion, run logging,
and lightweight progress helpers used by preprocessing and scoring stages.
"""

from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf

from ..config.config_global import GLOBAL_CONFIG
from ..config.config_unity_catalog_paths import PATHS
from ..utils.geo_utils import extract_epsg_number
from ..utils.io_utils import read_table, save_table


class FloodPipeline:
    """Base class for pipeline stages operating on an in-force table.

    This abstract base class provides common functionality for all flood analysis
    pipeline components, including:
    - Loading and saving Spark DataFrames from CSV/Parquet files
    - Managing geospatial coordinates (Sedona geometry)
    - Maintaining run-level audit logs (metadata and metrics)
    - Standardized table I/O and progress logging

    Notes
    -----
    Utility functions are exposed as class-level hooks (``read_table``,
    ``save_table``). Tests or advanced callers can monkeypatch these hooks
    for isolation without changing production code paths.

    The log attribute starts with basic metadata (run_date, as_of_date, run_name)
    and is extended by subclasses with stage-specific metrics.
    """

    # CHARM-POC PATCH: PR18 removed this class constant; preprocessor.py and geomap_scorer.py still read it
    GEOMETRY_COL: str = GLOBAL_CONFIG.geometry_col

    def __init__(
        self,
        table: DataFrame,
        as_of_date: str = "",
        run_name: str = "",
    ):
        """Initialize a pipeline stage with a Spark DataFrame or a file path.

        Parameters
        ----------
        table : DataFrame
            Input data as a Spark ``DataFrame``.
        as_of_date : str
            Portfolio effective date used in run metadata (e.g., "2024-01-15").
        run_name : str
            Human-readable run identifier stored in logs (e.g., "test", "ORSA", or "pricing_indication").
        **kwargs
            Additional keyword arguments forwarded to file readers when a path is provided
            (e.g., schema inference parameters, compression settings).

        Raises
        ------
        ValueError
            If ``table`` is not provided.
        TypeError
            If ``table`` is not a DataFrame.
        """
        # Validate that either a table is provided.
        if table is None:
            raise ValueError("You must provide a table.")
        
        if not isinstance(table, DataFrame):
            raise TypeError("table must be a PySpark DataFrame")

        self.table = table
        self.spark = table.sparkSession

        # Record the index column for the table
        self.index_col: str = GLOBAL_CONFIG.index_col

        # Initialize log with basic metadata; values can be int or str (mixed types)
        self.log: dict[str, object] = {
            "run_name": run_name,
            "run_date": datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M"),
            "as_of_date": as_of_date,
        }

        # Initialize the folder for saving outputs based on the run name
        self.run_name = run_name
        Path(PATHS.outputs_path + self.run_name).mkdir(parents=True, exist_ok=True)


    def set_index(self):
        """Validate and record the id column used to identify rows.

        Spark DataFrames have no row index. This method:
        1. Validates that the configured index column exists in the table
        2. Records the index column name for downstream operations

        The index column is used for tracking items changes,
        so it must be present and unique across the dataset.

        The table itself is not modified by this operation.

        Returns
        -------
        self
            Returns self to enable method chaining.

        Raises
        ------
        RuntimeError
            If the table is not loaded.
        ValueError
            If the configured index column is not found in the table or contains duplicate values.
        """
        column = GLOBAL_CONFIG.index_col

        if column not in self.table.columns:
            raise ValueError(f"Column '{column}' not found in the table. Cannot set index.")

        if self.table.select(column).distinct().count() != self.table.count():
            raise ValueError(f"Column '{column}' contains duplicate values. Cannot set as index.")

        if self.table.filter(F.col(column).isNull() | (F.trim(F.col(column).cast("string")) == "")).limit(1).count() > 0:
            raise ValueError(f"Column '{column}' contains null or blank values.")

        self.index_col = column
        return self

    def _to_geodataframe(self):
        """Build a geometry DataFrame with coordinates for geospatial analysis.

        Creates a new DataFrame containing the index column and a Sedona Point geometry
        constructed from longitude/latitude columns. The geometry is set to the
        configured CRS (Coordinate Reference System).

        This method is typically used before spatial joins or other geospatial operations.

        Returns
        -------
        self
            Returns self to enable method chaining.

        Raises
        ------
        RuntimeError
            If the table is not loaded or is empty.
        """
        if self.table.isEmpty():
            raise RuntimeError(
                "In-force table is empty. Please provide at least one row before calling to_geodataframe()."
            )

        geom_points = stf.ST_SetSRID(
            stc.ST_Point(F.col("longitude"), F.col("latitude")),
            extract_epsg_number(GLOBAL_CONFIG.crs),
        )
        self.table = self.table.withColumn(GLOBAL_CONFIG.geometry_col, geom_points)
        return self

    # CHARM-POC PATCH: preprocessor.py / geomap_scorer.py still call the pre-PR18 public name
    to_geodataframe = _to_geodataframe

    def get_log(self, fields: list[str] | None = None) -> dict[str, object]:
        """Return run-level metrics and metadata.

        Retrieves selected or all entries from the run log. Useful for inspecting
        metrics after pipeline execution (e.g., row counts, imputation statistics).

        Parameters
        ----------
        fields : list[str] | None
            List of keys to extract from the log dictionary. If None, return all items.

        Returns
        -------
        dict[str, object]
            Dictionary containing only the specified keys and their values, or all items
            if fields is None. Missing keys return None.
        """
        if fields is None:
            return self.log
        return {key: self.log.get(key, None) for key in fields}

    def _log_data_size(self, log_key: str):
        """Log the number of rows in the current table under a specified key."""
        self.log[log_key] = self.table.count()
        return self

    def _print_start(self):
        """Print a standard start message for the current pipeline stage."""
        logger.info(f"Starting pipeline stage {self.__class__.__name__}")
        return self

    def _print_end(self):
        """Print a standard completion message for the current pipeline stage."""
        logger.info(f"Finished pipeline stage {self.__class__.__name__}")
        return self
