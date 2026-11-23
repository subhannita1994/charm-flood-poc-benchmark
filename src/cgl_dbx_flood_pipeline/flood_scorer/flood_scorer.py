"""Spatial flood-region and direct flood API scoring for validated in-force data."""

import geopandas as gpd
from loguru import logger
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast, col
from sedona.spark.sql import st_predicates as stp

from ..api import ModelAPI, score_with_model_api
from ..sources import SourceProfile
from ..api.model_api import model_input_schema
from ..config.config_global import GLOBAL_CONFIG
from ..config.config_unity_catalog_paths import PATHS
from ..flood_pipeline.flood_pipeline import FloodPipeline
from ..utils.io_utils import resolve_polygon, save_json
from ..utils.log_utils import get_items
from ..utils.spark_utils import persist_dataframe
from ..utils.validation_utils import warn_if_rows_removed
from .clean_bad_coordinates import (
    assert_valid_coordinates,
    remove_missing_coordinates,
    remove_province_centroids,
    )
from .score_province import score_province

class FloodScorer(FloodPipeline):
    """Base class for spatial scoring of in-force flood data.

    This class coordinates the complete spatial scoring workflow:
    1. Load preprocessed in-force data and validate schema
    2. Construct point geometries from lat/lon coordinates
    3. Perform spatial joins with fluvial (riverine) and coastal flood regions
    4. Score flood damage functions via geomap API
    5. Select and save final output columns

    The source profile owns source naming and category translation. The model API
    owns standardized-input validation and request/response behavior.
    """

    def __init__(
        self,
        table: DataFrame | str,
        as_of_date: str,
        model_api: ModelAPI,
        source_profile: SourceProfile,
        run_name: str = "",
        **kwargs,
    ) -> None:
        """Initialize a flood scorer from a DataFrame or file path.

        Parameters
        ----------
        table : DataFrame | str
            In-force table (DataFrame or file path).
        as_of_date : str
            API as of date [date].
        run_name : str
            Human-readable run identifier for logging. Example: "test", "ORSA", or "pricing_indication".
        model_api : ModelAPI
            Model API instance used for scoring.
        source_profile : SourceProfile
            Source profile containing metadata about the data source.
        **kwargs
            Additional arguments passed to file readers.
        """
        super().__init__(table, as_of_date=as_of_date, run_name=run_name, **kwargs)

        # Store the rules for the source and api scoring logic
        self.source_profile: SourceProfile = source_profile
        self.model_api: ModelAPI = model_api
        self.table = table
        self.pcis_codes_mapped = False

        # Track items filtered out #should this be part of the flood_pipeline mother class
        self.filtered_items: dict[str, list] = {}

        # Validation of columns source and model api required columns
        self.schema = model_input_schema(self.model_api, self.index_col)
        source_columns = set(self.source_profile.colnames_mapping.values())
        required_source_columns = set(self.schema)
        missing_columns = required_source_columns - source_columns
        if missing_columns:
            raise ValueError(
                f"Source profile is missing column mappings for schema fields: {sorted(missing_columns)}"
            )


    def transform(
        self,
        fluvial_regions_gdf: gpd.GeoDataFrame | DataFrame | str = PATHS.fluvial_regions,
        coastal_regions_gdf: gpd.GeoDataFrame | DataFrame | str = PATHS.coastal_regions,
        fluvial_region_source_crs: int | str = GLOBAL_CONFIG.crs,
        coastal_region_source_crs: int | str = GLOBAL_CONFIG.crs,
    ) -> "FloodScorer":
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

        Returns
        -------
        self
            Returns self to enable method chaining.
        """

        # Step 1: Start of the process, setting up log, schema and index
        self._print_start()
        self._log_data_size("initial_row_count")
        self.apply_schema()
        self.set_index()

        # Step 2: Minimal cleaning, handling bad coordinates and map codes according to the source and model API
        self._to_geodataframe()
        self.clean_bad_coordinates(province_shp_path=PATHS.province_shp_path) # this is returning geometry
        self._map_codes() #TODO repeating this call makes data change
        self.save_filtered_items(PATHS.outputs_path)

        # Step 3: Convert to geospatial DataFrame and score flood regions
        self._score_flood_regions(
            fluvial_regions_gdf,
            source_crs=fluvial_region_source_crs,
            output_region_col="fluvial_region",
        )
        self._score_flood_regions(
            coastal_regions_gdf,
            source_crs=coastal_region_source_crs,
            output_region_col="coastal_region",
        )

        # Step 4: Score flood API and finalize the table
        self._score_flood_api()
        self.table = self.table.drop(GLOBAL_CONFIG.geometry_col) #TODO considerer de mettre variable geometry dans class mère et appeler self pour variable geometry
        self._log_data_size("final_row_count")
        
        self._print_end()
        return self


    def apply_schema(self):
        """Apply schema normalization and type casting."""
        renamed = self.source_profile.standardize(self.table, self.schema)

        # Select and cast columns according to the schema
        # This ensures consistent type ordering and structure
        self.table = renamed.select(
            *[
                col(name).try_cast(spark_type).alias(name)
                for name, spark_type in self.schema.items()
            ]
        )
        return self

    def clean_bad_coordinates(self, province_shp_path: str):
        """Filter invalid coordinates and assign province labels.

        This method performs three sequential geospatial operations:
        1. Remove rows with missing or invalid (out-of-bounds) coordinates
        2. Remove points that match province centroids (placeholders)
        3. Perform spatial join to assign province labels

        Rows removed by any step are tracked in dropped_items for audit purposes.

        Parameters
        ----------
        province_shp_path : str
            Path to province polygon shapefile.
        """

        # listing item_IDs before filtering to track which items are removed
        before_ids = self.table.select(self.index_col)

        # Step 1: Remove rows with missing or out-of-bounds coordinates
        self.table = remove_missing_coordinates(
            self.table,
            lat_col=GLOBAL_CONFIG.latitude_col,
            lon_col=GLOBAL_CONFIG.longitude_col,
        )

        # Step 2: Assert that remaining coordinates are valid coordinates in spherical projection
        # (EPSG:4326) and within the bounding box of Canada
        assert_valid_coordinates(
            self.table,
            lat_col=GLOBAL_CONFIG.latitude_col,
            lon_col=GLOBAL_CONFIG.longitude_col,
            bbox=GLOBAL_CONFIG.canada_bbox,
        )

        # Step 3: Remove points that match province centroids (placeholders)
        self.table = remove_province_centroids(
            self.table,
            lat_col=GLOBAL_CONFIG.latitude_col,
            lon_col=GLOBAL_CONFIG.longitude_col
        )

        # Step 4: Convert to GeoDataFrame and score provinces
        self._to_geodataframe()
        self.table = score_province(
            self.table,
            province_shp_path,
            target_crs=GLOBAL_CONFIG.crs,
            source_table_crs=GLOBAL_CONFIG.crs,
            source_province_crs=GLOBAL_CONFIG.crs,
            lat_col=GLOBAL_CONFIG.latitude_col,
            lon_col=GLOBAL_CONFIG.longitude_col,
            geometry_col=GLOBAL_CONFIG.geometry_col,
            province_col=GLOBAL_CONFIG.province_col,
            bbox=GLOBAL_CONFIG.canada_bbox,
        )

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
        self.filtered_items["bad_coordinates"] = removed_items

        return self

    def _map_codes(self):
        """Translate source categories, validate model inputs, and audit rejections."""
        standardized = self.source_profile.map_categories(self.table)
        result = self.model_api.validate_inputs(standardized)
        
        rejected_ids = [
            row[self.index_col]
            for row in result.rejected.select(self.index_col).collect()
        ]
        rejected_count = len(rejected_ids)

        self.filtered_items[result.reason] = rejected_ids
        self.log[f"nb_{result.reason}"] = rejected_count
        warn_if_rows_removed(rejected_count, message=result.warning)
        
        self.table = result.table
        return self

    def _score_flood_api(self) -> "FloodScorer":
        """Attach direct flood API outputs while retaining validated and region fields."""
        logger.info("Starting direct flood API scoring.")
        api_output = score_with_model_api(
            self.table, index_col=self.index_col,
            effective_date=str(self.log["as_of_date"]), model_api=self.model_api,
        )
        self.table = self.table.join(api_output, on=self.index_col, how="left")
        return self

    def _score_flood_regions(
        self,
        regions_gdf: gpd.GeoDataFrame | DataFrame | str,
        source_crs: int | str,
        output_region_col: str,
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
        output_region_col : str
            Name of the output column for the matched region number.

        Returns
        -------
        self
            Returns self to enable method chaining.
        """

        logger.info("Scoring flood regions into {}.", output_region_col)

        regions_polygons: DataFrame = resolve_polygon(
            regions_gdf,
            self.spark or self.table.sparkSession,
            source_crs=source_crs,
            target_crs=GLOBAL_CONFIG.crs,
            geometry_col=GLOBAL_CONFIG.geometry_col,
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
                stp.ST_Intersects(col(f"exposure.{GLOBAL_CONFIG.geometry_col}"), col("region.geometry")),
                "left",
            )
            .withColumn("_row_number", F.row_number().over(duplicate_window))
            .filter(col("_row_number") == 1)
        )

        # Select the original exposure columns along with the region number column.
        select_cols = [col(f"exposure.`{column_name}`") for column_name in self.table.columns]
        select_cols.append(col("region.NUMBER").alias(output_region_col))
        self.table = scored.select(*select_cols)
        return self

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
        save_json(self.filtered_items, filepath + self.run_name + "/filtered_items.json")
        logger.info(f"Filtered items saved to {filepath + self.run_name + '/filtered_items.json'}")
        return self


    def save_flood_scoring(self, filepath: str):
        """Save the scored in-force table to delta table"""
        output_path = f"{filepath.rstrip('/')}/{self.run_name}/flood_scoring"
        self.table.write.format("delta").mode("overwrite").save(output_path)
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


    def _has_expected_schema_types(self) -> bool: #TODO good idea, just need to know where we should put it
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