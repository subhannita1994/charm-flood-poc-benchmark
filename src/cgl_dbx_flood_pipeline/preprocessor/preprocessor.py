"""Shared preprocessing engine for flood in-force pipelines.

The class centralizes common cleansing operations and bookkeeping for
``dropped_items`` and ``imputed_items`` while delegating product-specific
logic (schema, mapping, deductible policy) to an injected rules object.
"""

from typing import Protocol

from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast, col

from ..config.config_global import GLOBAL_CONFIG
from ..config.config_unity_catalog_paths import PATHS
from ..flood_pipeline.flood_pipeline import FloodPipeline
from ..utils.io_utils import save_json
from ..utils.log_utils import get_items
from ..utils.spark_utils import persist_dataframe
from ..utils.validation_utils import validate_required_columns, warn_if_rows_removed
from .filter_bad_locations import (
    assert_valid_coordinates,
    remove_missing_coordinates,
    remove_province_centroids,
)
from .score_province import score_province


class PreprocessorRules(Protocol):
    """Behavior contract for product-specific preprocessing rules.

    This protocol defines the interface that all product-specific rules must implement.
    Rules encapsulate product-specific logic (schema, filtering, mappings, deductibles)
    and are called by FloodInForcePreprocessor with the preprocessor instance as context.
    """

    def apply_schema(self, ctx: "FloodInForcePreprocessor") -> None: ...

    def filter_coverage(self, ctx: "FloodInForcePreprocessor") -> None: ...

    def map_codes(self, ctx: "FloodInForcePreprocessor") -> None: ...

    def score_min_deductible(
        self,
        ctx: "FloodInForcePreprocessor",
        custom_min_deductible: float | None = None,
    ) -> None: ...

    def score_max_deductible(
        self,
        ctx: "FloodInForcePreprocessor",
        custom_max_deductible: float | None = None,
    ) -> None: ...

    def impute_deductible(
        self,
        ctx: "FloodInForcePreprocessor",
        custom_deductible: float | None = None,
    ) -> None: ...

    def drop_intermediary_columns(self, ctx: "FloodInForcePreprocessor") -> None: ...


class FloodInForcePreprocessor(FloodPipeline):
    """Shared preprocessing engine for flood in-force data.

    Notes
    -----
    Geospatial helper functions are exposed as class-level hooks
    (``remove_missing_coordinates``, ``remove_province_centroids``,
    ``score_province``). This keeps production behavior unchanged while making
    unit tests easy to isolate.
    """

    CRS: str = GLOBAL_CONFIG.crs
    PROVINCE_CRS: str = GLOBAL_CONFIG.province_crs
    PROVINCE_COL: str = GLOBAL_CONFIG.province_col
    CANADA_BBOX: dict[str, float] = GLOBAL_CONFIG.canada_bbox
    save_json = staticmethod(save_json)
    remove_missing_coordinates = staticmethod(remove_missing_coordinates)
    remove_province_centroids = staticmethod(remove_province_centroids)
    score_province = staticmethod(score_province)
    assert_valid_coordinates = staticmethod(assert_valid_coordinates)

    def __init__(
        self,
        table: DataFrame | str,
        rules: PreprocessorRules,
        as_of_date: str = "",
        run_name: str = "",
        **kwargs,
    ):
        """Initialize a preprocessor from a DataFrame or file path.

        Parameters
        ----------
        table : DataFrame | str
            In-memory table or file path string.
        rules : PreprocessorRules
            Product-specific rules object that implements the PreprocessorRules protocol.
        as_of_date : str
            Date associated to the in-force table. Used for logging and run naming.
        run_name : str
            Name of the preprocessing run. Used for logging and run naming.
        **kwargs
            Extra keyword arguments forwarded to Spark readers.
        """
        super().__init__(table, as_of_date=as_of_date, run_name=run_name, **kwargs)

        # Track items dropped at each preprocessing step
        self.dropped_items: dict[str, list] = {}
        # Track items imputed/transformed at each preprocessing step
        self.imputed_items: dict[str, list] = {}
        # Internal state flags for pipeline ordering
        self.schema_applied: bool = False
        self._provinces_scored: bool = False
        # Product-specific rules object
        self.rules: PreprocessorRules = rules

    def transform(
        self,
        province_shp_path: str | None = None,
        custom_deductible: float | None = None,
        drop_intermediary_columns: bool = True,
    ):
        """Run the end-to-end preprocessing sequence.

        Parameters
        ----------
        province_shp_path : str | None
            Optional path to province polygons used for province scoring.
            If omitted, ``score_province`` must be able to resolve polygons via
            its own defaults or injected test inputs.
        custom_deductible : float | None
            Optional override passed to ``impute_deductible``.
        drop_intermediary_columns : bool
            Whether to remove temporary columns at the end of preprocessing.
        """
        self._print_start()
        self._log_data_size("initial_row_count")
        self.apply_schema()
        self.set_index()
        self.filter_coverage()
        self.clean_bad_tiv()  # Déplacer à la fin
        self.clean_bad_coordinates(province_shp_path)
        self.map_codes()
        self.score_min_deductible()  # retirer
        self.score_max_deductible()  # retirer
        self.impute_deductible(custom_deductible)  # Déplacer à la fin
        self.drop_intermediary_columns(drop_intermediary_columns)
        self._log_data_size("final_row_count")
        self._print_end()
        return self

    def clean_bad_tiv(self):
        """Remove rows where building+content insured values are invalid.

        Filters the table to keep only rows where:
        - Both building_value and content_value are non-negative
        - The sum of building_value and content_value is greater than $1
        - The flood_limit is greater than $1

        Invalid rows are tracked in dropped_items for audit purposes.
        """
        # Validate that required columns exist
        self.ensure_columns(columns=["building_value", "content_value", "flood_limit"])

        # Build filter masks for each validation rule
        ## > Both insured values must be non-negative
        mask_values = (col("building_value") >= 0) & (col("content_value") >= 0)
        ## > Combined insured value must exceed $1 to be material
        mask_sum_values = (col("building_value") + col("content_value")) > 1
        ## > Flood limit must exceed $1 to be meaningful
        mask_limit = col("flood_limit") > 1

        # Combine all filters: row is valid only if ALL conditions pass
        # Find rows that FAIL validation (negate all conditions combined)
        invalid_ids_df = self.table.filter(~(mask_values & mask_sum_values & mask_limit)).select(
            self.index_col
        )

        # Persist the invalid IDs to avoid recomputation during the join and collection
        invalid_ids_df, persisted = persist_dataframe(invalid_ids_df)

        # Remove invalid rows using left_anti join (keeps rows NOT in invalid_ids_df)
        self.table = self.table.join(broadcast(invalid_ids_df), on=self.index_col, how="left_anti")

        # Collect invalid IDs for audit tracking
        removed_items = [row[0] for row in invalid_ids_df.collect()]

        # Unpersist the invalid IDs DataFrame if it was persisted
        if persisted:
            invalid_ids_df.unpersist()

        # Log the results
        self.log["nb_bad_tiv"] = len(removed_items)
        warn_if_rows_removed(
            self.log["nb_bad_tiv"],
            message="There are {removed} locations that have invalid building+content insured values.",
        )
        self.dropped_items["bad_tiv"] = removed_items

        return self

    def clean_bad_coordinates(self, province_shp_path: str | None = None):
        """Filter invalid coordinates and assign province labels.

        This method performs three sequential geospatial operations:
        1. Remove rows with missing or invalid (out-of-bounds) coordinates
        2. Remove points that match province centroids (placeholders)
        3. Perform spatial join to assign province labels

        Rows removed by any step are tracked in dropped_items for audit purposes.

        Parameters
        ----------
        province_shp_path : str | None
            Path to province polygon shapefile. If None, uses configured default.
        """
        self.ensure_columns(["latitude", "longitude"])

        if province_shp_path is None:
            province_shp_path = PATHS.province_shp_path

        # listing item_IDs before filtering to track which items are removed
        before_ids = self.table.select(self.index_col)

        # Step 1: Remove rows with missing or out-of-bounds coordinates
        self.table = type(self).remove_missing_coordinates(
            self.table,
            lat_col="latitude",
            lon_col="longitude",
        )

        # Step 2: Assert that remaining coordinates are valid coordinates in spherical projection
        # (EPSG:4326) and within the bounding box of Canada
        type(self).assert_valid_coordinates(
            self.table,
            lat_col="latitude",
            lon_col="longitude",
            bbox=self.CANADA_BBOX,
        )

        # Step 3: Remove points that match province centroids (placeholders)
        self.table = type(self).remove_province_centroids(
            self.table, lat_col="latitude", lon_col="longitude"
        )

        # Step 4: Convert to GeoDataFrame and score provinces
        self.to_geodataframe()
        self.table = type(self).score_province(
            self.table,
            province_shp_path,
            target_crs=self.CRS,
            source_table_crs=self.CRS,
            source_province_crs=self.PROVINCE_CRS,
            lat_col="latitude",
            lon_col="longitude",
            geometry_col=self.GEOMETRY_COL,
            province_col=self.PROVINCE_COL,
            bbox=self.CANADA_BBOX,
        )
        self._provinces_scored = True

        # Step 5: Identify which items were removed during the coordinate cleaning steps
        removed_items_df = before_ids.subtract(self.table.select(self.index_col))

        # Persist the removed items DataFrame to avoid recomputation during collection
        removed_items_df, persisted = persist_dataframe(removed_items_df)

        removed_items = [row[0] for row in removed_items_df.collect()]

        if persisted:  # Unpersist the removed items DataFrame if it was persisted
            removed_items_df.unpersist()

        # Step 6: Log the number of removed items and store them for auditing
        nb_removed: int = len(removed_items)
        self.log["nb_bad_coordinates"] = nb_removed
        warn_if_rows_removed(
            nb_removed, message="A total of {removed} bad coordinates were removed."
        )
        self.dropped_items["bad_coordinates"] = removed_items

        return self

    def save_dropped_items(self, filepath: str):
        """Persist ``dropped_items`` as a JSON document."""
        type(self).save_json(self.dropped_items, filepath)
        logger.info(f"Dropped items saved to {filepath}")
        return self

    def save_imputed_items(self, filepath: str):
        """Persist ``imputed_items`` as a JSON document."""
        type(self).save_json(self.imputed_items, filepath)
        logger.info(f"Imputed items saved to {filepath}")
        return self

    def get_dropped_items(self, step: str | None = None):
        """Return dropped item ids for one step or all steps."""
        return get_items(self.dropped_items, step)

    def get_imputed_items(self, step: str | None = None):
        """Return imputed item ids for one step or all steps."""
        return get_items(self.imputed_items, step)

    def ensure_schema_applied(self):
        """Validate that schema has been applied before proceeding."""
        if not self.schema_applied:
            raise RuntimeError("apply_schema() must be called before this transformation step.")
        return self

    def _ensure_provinces_scored(self):
        """Validate that provinces have been scored before proceeding.

        This check ensures geographic processing happens before downstream steps
        that depend on the province column for risk scoring.

        Raises
        ------
        RuntimeError
            If provinces have not been scored yet.
        """
        if not self._provinces_scored:
            raise RuntimeError(
                "clean_bad_coordinates() must be called before this transformation step."
            )
        return self

    def ensure_columns(self, columns: list[str]):
        """Validate that required columns exist after schema application."""
        self.ensure_schema_applied()
        validate_required_columns(self.table, columns)
        return self

    def apply_schema(self):
        """Apply product-specific schema normalization and type casting."""
        self.rules.apply_schema(self)
        return self

    def filter_coverage(self):
        """Filter rows to the product/company coverage scope configured in rules."""
        self.rules.filter_coverage(self)
        return self

    def map_codes(self):
        """Map source-system categorical values to standardized internal codes."""
        self.rules.map_codes(self)
        return self

    def score_min_deductible(self, custom_min_deductible: float | None = None):
        """Infer the minimum allowable deductible for each row.

        Parameters
        ----------
        custom_min_deductible : float | None
            Optional constant to override product-specific min deductible logic.

        Returns
        -------
        self
            Returns self to enable method chaining.
        """
        self.rules.score_min_deductible(self, custom_min_deductible)
        return self

    def score_max_deductible(self, custom_max_deductible: float | None = None):
        """Infer the maximum allowable deductible for each row.

        Parameters
        ----------
        custom_max_deductible : float | None
            Optional constant to override product-specific max deductible logic.

        Returns
        -------
        self
            Returns self to enable method chaining.
        """
        self.rules.score_max_deductible(self, custom_max_deductible)
        return self

    def impute_deductible(self, custom_deductible: float | None = None):
        """Impute deductible values that are missing or outside allowed values.

        Parameters
        ----------
        custom_deductible : float | None
            Optional fill value used when deductible imputation is required.

        Returns
        -------
        self
            Returns self to enable method chaining.
        """
        self.rules.impute_deductible(self, custom_deductible)
        return self

    def drop_intermediary_columns(self, drop_intermediary_columns: bool = True):
        """Drop temporary columns that are not needed after preprocessing.

        Parameters
        ----------
        drop_intermediary_columns : bool
            Whether to drop intermediary columns. Defaults to True.

        Returns
        -------
        self
            Returns self to enable method chaining.
        """
        if drop_intermediary_columns:
            self.rules.drop_intermediary_columns(self)
        return self
