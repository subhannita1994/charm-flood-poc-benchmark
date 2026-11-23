"""Parent class for the spatial scoring of the in-force table."""

from time import time
from typing import Protocol

import geopandas as gpd
from loguru import logger
from pyspark import StorageLevel
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast, col
from sedona.spark.sql import st_predicates as stp
from sedona.spark.sql.types import GeometryType

from ..config.config_flood_risks import RISK_CONFIGS, RiskType
from ..config.config_global import GLOBAL_CONFIG
from ..config.config_unity_catalog_paths import PATHS
from ..flood_pipeline.flood_pipeline import FloodPipeline
from ..utils.io_utils import resolve_polygon, save_json
from ..utils.log_utils import get_items
from ..utils.spark_utils import persist_dataframe
from ..utils.validation_utils import warn_if_rows_removed
from .score_pss import score_with_pss


class GeomapScorerRules(Protocol):
    """Behavior contract for product-specific geomap scoring rules.

    Rules encapsulate product-specific logic for scoring flood regions and
    selecting columns for the final output.
    """

    @property
    def cie(self) -> str: ...

    @property
    def product(self) -> str: ...

    def required_columns(self, base_required_columns: list[str]) -> list[str]: ...

    def select_required_columns(self, ctx: "GeomapScorer") -> None: ...

    def drop_intermediary_columns(
        self,
        ctx: "GeomapScorer",
        drop_intermediary_columns: bool = True,
    ) -> None: ...

    def map_pss_version(self, date: str) -> str: ...


class GeomapScorer(FloodPipeline):
    """Base class for spatial scoring of preprocessed in-force flood data.

    This class coordinates the complete spatial scoring workflow:
    1. Load preprocessed in-force data and validate schema
    2. Construct point geometries from lat/lon coordinates
    3. Perform spatial joins with fluvial (riverine) and coastal flood regions
    4. Score flood damage functions via geomap API
    5. Select and save final output columns

    Child classes adapt scoring logic to different companies/products via
    the rules pattern, which delegates product-specific logic to injected rules objects.
    """

    CRS = GLOBAL_CONFIG.crs

    def __init__(
        self,
        table: DataFrame | str,
        rules: GeomapScorerRules,
        as_of_date: str,
        run_name: str = "",
        pss_version: str | None = None,
        previous_output: DataFrame | None = None,
        **kwargs,
    ):
        """Initialize a geomap scorer from a DataFrame or file path.

        Parameters
        ----------
        table : DataFrame | str
            Preprocessed in-force table (DataFrame or file path).
        rules : GeomapScorerRules
            Product-specific rules object that implements the scoring logic.
        as_of_date : str
            Exposure in-force as of [date].
        run_name : str
            Human-readable run identifier for logging. Example: "test", "ORSA", or "pricing_indication".
        pss_version : str
            Peril Score Service (PSS) version for geomap. Current options are "v1", "v1.1", and "v2_index_100".
        previous_output : DataFrame | None
            Already-scored output from a prior run; matching items are skipped and the
            previous rows are re-attached to the final output.
        **kwargs
            Additional arguments passed to file readers.
        """
        super().__init__(table, as_of_date=as_of_date, run_name=run_name, **kwargs)

        # Store the rules object for product-specific scoring logic
        self.rules: GeomapScorerRules = rules

        # Track items filtered out at each scoring step
        self.filtered_items: dict[str, list] = {}

        # Resolving required columns and schema
        index_col = GLOBAL_CONFIG.index_col
        required_columns = [
            index_col,
            GLOBAL_CONFIG.geometry_col,
            GLOBAL_CONFIG.province_col,
            "latitude",
            "longitude",
        ]
        self.required_columns: list[str] = self._resolve_required_columns(required_columns)
        self.schema = {
            col: GLOBAL_CONFIG.schema.get(col)
            for col in self.required_columns
            if col in GLOBAL_CONFIG.schema
        } | {GLOBAL_CONFIG.province_col: str}
        self.table: DataFrame = self.table

        # Initialize previous output to avoid rescoring already scored items
        self.original_indexes: list[str] = []
        self.previous_output: DataFrame | None = previous_output

        # Initialize instance attributes for PSS version
        self.pss_version: str = (
            self.rules.map_pss_version(as_of_date) if pss_version is None else pss_version
        )
        self.log["pss_version"] = self.pss_version

    def transform(
        self,
        fluvial_regions_gdf: gpd.GeoDataFrame | DataFrame | str = PATHS.fluvial_regions,
        coastal_regions_gdf: gpd.GeoDataFrame | DataFrame | str = PATHS.coastal_regions,
        fluvial_region_source_crs: int | str = GLOBAL_CONFIG.crs,
        coastal_region_source_crs: int | str = GLOBAL_CONFIG.crs,
        drop_intermediary_columns: bool = True,
    ):
        """Run the complete end-to-end spatial scoring pipeline.

        Parameters
        ----------
        fluvial_regions_gdf : gpd.GeoDataFrame | DataFrame | str
            Fluvial (riverine) flood regions as a DataFrame or shapefile path.
        coastal_regions_gdf : gpd.GeoDataFrame | DataFrame | str
            Coastal flood regions as a DataFrame or shapefile path.
        fluvial_region_source_crs : int | str, default GLOBAL_CONFIG.crs
            Source CRS of Spark fluvial region geometries. GeoDataFrame inputs
            use their own ``.crs`` metadata.
        coastal_region_source_crs : int | str, default GLOBAL_CONFIG.crs
            Source CRS of Spark coastal region geometries. GeoDataFrame inputs
            use their own ``.crs`` metadata.
        drop_intermediary_columns : bool
            Whether to drop temporary columns from the output.

        Returns
        -------
        self
            Returns self to enable method chaining.
        """
        self._print_start()
        self._log_data_size("initial_row_count")
        self._ensure_preprocessed()
        self.set_index()
        self.filter_building_floor()  # Déplacer à la fin
        self.filter_existing_output()
        self.select_required_columns()
        if GLOBAL_CONFIG.geometry_col not in self.table.columns or not isinstance(
            self.table.select(GLOBAL_CONFIG.geometry_col), GeometryType
        ):
            self.to_geodataframe()
        self.score_flood_regions(
            fluvial_regions_gdf, source_crs=fluvial_region_source_crs, risk="flv"
        )
        self.score_flood_regions(
            coastal_regions_gdf, source_crs=coastal_region_source_crs, risk="cst"
        )
        self.filter_no_flood_region()
        start_time = time()
        self.score_pss()
        self.drop_intermediary_columns(drop_intermediary_columns)
        self.concatenate_previous_output()

        # Persist the table to avoid accidental recomputation
        self.table, _ = persist_dataframe(self.table, storage_level=StorageLevel.MEMORY_AND_DISK)

        # Final loggings
        self._log_data_size("final_row_count")
        end_time = time()
        logger.info(f"PSS scoring completed in {(end_time - start_time) / 60:.2f} minutes.")
        self._print_end()
        return self

    def score_flood_regions(
        self, regions_gdf: gpd.GeoDataFrame | DataFrame | str, source_crs: int | str, risk: RiskType
    ):
        """Score flood regions (fluvial watersheds or coastal zones) for each policy location.

        Performs a spatial join to assign the region ID to each property based on its
        coordinates. Properties outside any region receive NULL.

        Parameters
        ----------
        regions_gdf : gpd.GeoDataFrame | DataFrame | str
            Flood regions as a GeoDataFrame, Spark DataFrame, or file path.
        source_crs : int | str
            Coordinate reference system of the input regions.
        risk : RiskType
            Risk type ("flv" for fluvial, "cst" for coastal); resolved against
            ``FloodRiskConfig.RISK_CONFIGS`` to determine the output region column.

        Returns
        -------
        self
            Returns self to enable method chaining.
        """

        logger.info(f"{RISK_CONFIGS[risk].progress_description} flood regions.")

        self._ensure_geometry()
        region_config = RISK_CONFIGS[risk]
        region_col = region_config.region_column

        regions_polygons: DataFrame = resolve_polygon(
            regions_gdf,
            self.spark or self.table.sparkSession,
            source_crs=source_crs,
            target_crs=self.CRS,
            geometry_col=self.GEOMETRY_COL,
        )

        # Select a stable region when overlapping polygons match the same exposure.
        exposure_table = self.table.alias("exposure")
        duplicate_window = Window.partitionBy(col(f"exposure.`{self.index_col}`")).orderBy(
            col("region.NUMBER").asc_nulls_last()
        )

        # Intersects includes boundary points; row_number removes overlap duplicates.
        scored = (
            exposure_table.join(
                broadcast(regions_polygons).alias("region"),
                stp.ST_Intersects(col(f"exposure.{self.GEOMETRY_COL}"), col("region.geometry")),
                "left",
            )
            .withColumn("_row_number", F.row_number().over(duplicate_window))
            .filter(col("_row_number") == 1)
        )

        # Select the original exposure columns along with the region number column.
        select_cols = [col(f"exposure.`{column_name}`") for column_name in self.table.columns]
        select_cols.append(col("region.NUMBER").alias(region_col))
        self.table = scored.select(*select_cols)
        return self

    def filter_no_flood_region(self):
        """Filter the in-force table to include only risks that are located in a flood region."""

        if not all(col in self.table.columns for col in ["fluvial_region", "coastal_region"]):
            raise ValueError(
                "Fluvial and coastal regions must be scored before filtering for flood regions."
            )

        no_region_df = self.table.filter(
            col("fluvial_region").isNull() & col("coastal_region").isNull()
        )

        # Persist the DataFrame to memory and disk to optimize collection, if supported
        no_region_df, persisted = persist_dataframe(no_region_df)

        nb_no_region = no_region_df.count()

        self.filtered_items["no_corresponding_flood_region"] = [
            row[self.index_col] for row in no_region_df.select(self.index_col).collect()
        ]
        warn_if_rows_removed(
            nb_no_region, message="There are {removed} locations geocoded outside any flood region."
        )
        self.log["nb_empty_flood_region"] = int(nb_no_region)

        self.table = self.table.filter(
            ~(col("fluvial_region").isNull() & col("coastal_region").isNull())
        )

        if persisted:
            no_region_df.unpersist()

        return self

    def filter_building_floor(self):
        """Filter the in-force table to include only risks sitting on the ground level or located in basement."""
        if "floor" not in self.table.columns:
            return self

        not_ground_floor = self.table.filter(col("floor") > 1)

        # Persist the DataFrame to memory and disk to optimize collection, if supported
        not_ground_floor, persisted = persist_dataframe(not_ground_floor)

        nb_not_on_ground_floor = not_ground_floor.count()
        self.log["nb_not_on_ground_floor"] = nb_not_on_ground_floor
        warn_if_rows_removed(
            nb_not_on_ground_floor, message="There are {removed} locations not on the ground floor."
        )

        self.filtered_items["not_on_ground_floor"] = [
            row[self.index_col] for row in not_ground_floor.select(self.index_col).collect()
        ]

        self.table = self.table.filter((col("floor") < 2) | col("floor").isNull())

        if persisted:
            not_ground_floor.unpersist()

        return self

    def _ensure_preprocessed(self):
        """Ensure that the in-force table has been preprocessed before scoring.

        This method checks for the presence of required columns and any missing values.
        If the checks fail, it raises a ValueError indicating that the preprocessing step was not completed.
        """
        assertion_required_columns = all(col in self.table.columns for col in self.required_columns)
        assertion_schema_types = self._has_expected_schema_types()
        assertion_no_missing_values = (
            self.table.filter(
                col("item_id").isNull()
                | col("latitude").isNull()
                | col("longitude").isNull()
                | col("province").isNull()
            ).count()
            == 0
        )

        if not all(
            [assertion_required_columns, assertion_schema_types, assertion_no_missing_values]
        ):
            raise ValueError("Preprocessing step not completed.")

        return self

    def _ensure_geometry(self):
        """Ensure geometry points have been generated on ``self.table`` before spatial scoring."""
        if self.GEOMETRY_COL not in self.table.columns:
            raise RuntimeError(
                "Geometry column not found. Please run 'to_geodataframe()' before scoring regions."
            )
        if isinstance(self.table.select(self.GEOMETRY_COL), str):
            raise TypeError(
                "Geometry column is not of the expected type. Please run 'to_geodataframe()' before scoring regions."
            )
        return self

    def _has_expected_schema_types(self) -> bool:
        """Check that required columns roughly match expected scalar types.

        Validates that columns have compatible types for the expected Python types
        (float, int, str). Uses lenient checking to allow Spark type variations
        (e.g., double/float for numeric, bigint/int for integers).

        Returns
        -------
        bool
            True if all columns have acceptable types, False otherwise.
        """
        dtypes = dict(self.table.dtypes)
        numeric_types = {"double", "float", "int", "bigint", "smallint", "tinyint", "decimal"}
        integer_types = {"int", "bigint", "smallint", "tinyint"}

        # Check each column in the schema
        for column_name, expected_type in self.schema.items():
            if column_name not in dtypes:
                # Column missing - this will be caught elsewhere
                continue

            # Get the Spark type string and remove complex type parameters
            spark_type = dtypes[column_name].lower()
            spark_type_base = spark_type.split("(")[0]

            # Validate type compatibility
            if expected_type is float and spark_type_base not in numeric_types:
                return False
            if expected_type is int and spark_type_base not in integer_types:
                return False
            if expected_type is str and spark_type_base != "string":
                return False
        return True

    def save_filtered_items(self, filepath: str):
        """Save filtered-item tracking dictionary as JSON.

        Parameters
        ----------
        filepath : str
            Path where the JSON file will be saved.

        Returns
        -------
        self
            Returns self to enable method chaining.
        """
        save_json(self.filtered_items, filepath)
        logger.info(f"Filtered items saved to {filepath}")
        return self

    def get_filtered_items(self, field: str | None = None):
        """Return the list of filtered items for each filtering step.

        Parameters
        ----------
        field : str | None
            If provided, returns items from that filtering step. If None, returns all.

        Returns
        -------
        dict | list
            If field is None, returns dict of all filtered items by step.
            If field is provided, returns list of item IDs filtered at that step.
        """
        return get_items(self.filtered_items, field)

    def _resolve_required_columns(self, base_required_columns: list[str]) -> list[str]:
        """Compute instance-level required columns from configured rules."""
        return self.rules.required_columns(base_required_columns)

    def score_pss(self):
        """Score the geomap functions (also referred to as PSS - Peril Score Service) for each row."""
        logger.info(
            f"Starting PSS scoring with version: {self.pss_version}. This may take a while."
        )
        pss_output = score_with_pss(
            self.table,
            index_col=self.index_col,
            cie=self.rules.cie,
            product=self.rules.product,
            pss_endpoint_version=self.pss_version,
        ).withColumn("pss_version", F.lit(self.pss_version))
        self.table = self.table.join(pss_output, on=self.index_col, how="left")
        return self

    def select_required_columns(self):
        """Select only the columns that are required for the scoring process."""
        self.rules.select_required_columns(self)
        return self

    def drop_intermediary_columns(self, drop_intermediary_columns: bool = True):
        """Drop intermediate columns that are not required for the final scoring process."""
        # Geometry/lat/long are always intermediary; product-specific columns are handled by the rules.
        self.table = self.table.drop(self.GEOMETRY_COL, "latitude", "longitude")
        self.rules.drop_intermediary_columns(self, drop_intermediary_columns)
        return self

    def filter_existing_output(self):
        """Filter the in-force table to exclude items already scored in a previous run."""
        if self.previous_output is None:
            return self

        current_indexes = [
            row[self.index_col] for row in self.table.select(self.index_col).collect()
        ]
        previous_indexes = [
            row[self.index_col] for row in self.previous_output.select(self.index_col).collect()
        ]

        # Find the indexes that are present in both the current table and the previous output.
        overlaping_indexes = list(set(current_indexes) & set(previous_indexes))

        # Count the number of overlapping indexes and store them for logging and filtering purposes.
        nb_existing = len(overlaping_indexes)
        self.filtered_items["already_scored"] = list(overlaping_indexes)
        self.original_indexes = current_indexes
        warn_if_rows_removed(
            nb_existing, message="There are {removed} locations already scored in previous output."
        )
        self.log["nb_existing_output"] = int(nb_existing)

        # Filter out the overlapping indexes from the current table to avoid scoring them again.
        self.table = self.table.filter(~self.table[self.index_col].isin(overlaping_indexes))
        return self

    def concatenate_previous_output(self):
        """Reattach the previous run's already-scored output to the current in-force table."""
        if self.previous_output is None:
            return self

        logger.info("Concatenating previous output with the current table.")
        self.table = self.table.unionByName(self.previous_output, allowMissingColumns=True)
        self.table = self.table.filter(self.table[self.index_col].isin(self.original_indexes))
        return self
