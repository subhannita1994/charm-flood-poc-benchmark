"""Class to score the flood in-force against simulations and compute losses."""

from __future__ import annotations

from loguru import logger
from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..config import GLOBAL_CONFIG, PATHS
from ..flood_pipeline.flood_pipeline import FloodPipeline
from ..utils.validation_utils import warn_if_rows_removed


class FloodSimulationMerge(FloodPipeline):
    """Class to merge the flood in-force against simulations."""

    def __init__(
        self,
        table: DataFrame,
        as_of_date: str = "",
        run_name: str = "",
        **kwargs,
    ):
        """Initialize a loss pipeline from a DataFrame or file path.

        Parameters
        ----------
        table : DataFrame
            Preprocessed and geomap-scored in-force table (DataFrame).
        as_of_date : str
            Portfolio effective date for run metadata.
        run_name : str
            Human-readable run identifier for logging.
        **kwargs
            Additional arguments passed to file readers.
        """
        super().__init__(table, as_of_date=as_of_date, run_name=run_name, **kwargs)
        self.filtered_items: dict[str, list] = {}
        self.table: DataFrame = table
        self.fluvial_events: DataFrame | None = None
        self.coastal_events: DataFrame | None = None
        self.fluvial_table: DataFrame | None = None
        self.coastal_table: DataFrame | None = None

    def transform(
        self,
        fluvial_simulation_repo: str = PATHS.fluvial_simulation,
        coastal_simulation_repo: str = PATHS.coastal_simulation,
    ):
        """Join the in-force table with fluvial and coastal flood simulations.
        Parameters
        ----------
        fluvial_simulation_repo : str
            Directory containing one ``{region}.parquet`` fluvial simulation file per region.
        coastal_simulation_repo : str
            Directory containing one ``{region}.parquet`` coastal simulation file per region.

        Returns
        -------
        self
            Returns self to enable method chaining.
        """
        self._print_start()
        self.set_index()

        self.load_regions(fluvial_simulation_repo, coastal_simulation_repo) #loading regions here #TODO persist
        self.assert_region_coverage() #validating no regions in the inforce map to no region

        self.reshape_table() #exploding the inforce table
        self.apply_defense_efficiency() #we will apply the defense efficiency

        self.join_simulations()

        
        self._print_end()
        return self


    def load_regions(self, flv_delta: str, costal_delta: str):
        """Load fluvial and coastal flood simulation regions.

        Parameters
        ----------
        flv_delta : str
            Path to the fluvial simulation Delta table.
        costal_delta : str
            Path to the coastal simulation Delta table.
        """
        events_fluvial = self.spark.read.format("delta").load(flv_delta)
        events_coastal = self.spark.read.format("delta").load(costal_delta)

        filtered_fluvial = (
            events_fluvial.filter(F.col("return_period") >= GLOBAL_CONFIG.return_periods[0])
            .withColumn("return_period", F.floor(F.col("return_period")).cast("int"))
            .select(
                "flood_region",
                F.col("simulated_year").cast("long").alias("simulated_year"),
                "event_id",
                F.col("return_period").cast("int").alias("return_period_original"),
                self.return_period_stair().alias("return_period")
            )
        )

        filtered_coastal = (
            events_coastal.filter(F.col("return_period") >= GLOBAL_CONFIG.return_periods[0])
            .withColumn("return_period", F.floor(F.col("return_period")).cast("int"))
            .select(
                "flood_region",
                F.col("simulated_year").cast("long").alias("simulated_year"),
                "event_id",
                F.col("return_period").cast("int").alias("return_period_original"),
                self.return_period_stair().alias("return_period")
            )
        )

        self.fluvial_events = filtered_fluvial
        self.coastal_events = filtered_coastal
        return self

    def return_period_stair(self, column: str = "return_period") -> F.Column:
        """Step a raw return period down to the largest config knot <= it.

        Rows below the first knot must be filtered out before (they give 0 damage).
        """
        rp = F.col(column)
        stair = F.lit(GLOBAL_CONFIG.return_periods[0])
        for knot in GLOBAL_CONFIG.return_periods[1:]:
            stair = F.when(rp >= knot, F.lit(knot)).otherwise(stair)
        return stair.cast("int")

    def assert_region_coverage(self) -> None:
        fluvial_regions_inforce = (
            self.table
            .select(F.col("fluvial_region").cast("string").alias("fluvial_region"))
            .where(F.col("fluvial_region").isNotNull())
            .distinct()
        )
        coastal_regions_inforce = (
            self.table
            .select(F.col("coastal_region").cast("string").alias("coastal_region"))
            .where(F.col("coastal_region").isNotNull())
            .distinct()
        )

        fluvial_regions_simulations = (
            self.fluvial_events
            .select(F.col("flood_region").cast("string").alias("fluvial_region"))
            .where(F.col("fluvial_region").isNotNull())
            .distinct()
        )

        coastal_regions_simulations = (
            self.coastal_events
            .select(F.col("flood_region").cast("string").alias("coastal_region"))
            .where(F.col("coastal_region").isNotNull())
            .distinct()
        )

        missing_fluvial_regions = fluvial_regions_inforce.join(
            fluvial_regions_simulations, on="fluvial_region", how="left_anti"
        )
        sample = [row["fluvial_region"] for row in missing_fluvial_regions.limit(10).collect()]

        if sample:
            raise ValueError(
                f"Fluvial simulation data is missing in-force regions; "
                f"sample of missing regions: {sample}"
            )

        missing_coastal_regions = coastal_regions_inforce.join(
            coastal_regions_simulations, on="coastal_region", how="left_anti"
        )
        sample = [row["coastal_region"] for row in missing_coastal_regions.limit(10).collect()]

        if sample:
            raise ValueError(
                f"Coastal simulation data is missing in-force regions; "
                f"sample of missing regions: {sample}"
            )

    def reshape_table(self):
        """Wide (1 row per item) -> long (1 row per item x return-period knot), per risk."""
        # One struct per knot; explode turns the 7 structs into 7 rows.
        knots = F.array(
            *[
                F.struct(
                    F.lit(rp).cast("int").alias("return_period"),
                    F.col(f"fluvialBuildingDamageRatios_{rp}").alias("fluvial_building_damage_ratio"),
                    F.col(f"fluvialContentsDamageRatios_{rp}").alias("fluvial_contents_damage_ratio"),
                    F.col(f"stormSurgeBuildingDamageRatios_{rp}").alias("storm_surge_building_damage_ratio"),
                    F.col(f"stormSurgeContentsDamageRatios_{rp}").alias("storm_surge_contents_damage_ratio"),
                    F.col(f"fluvialDefenseEfficiencies_{rp}").alias("fluvial_defense_efficiency"),
                    F.col(f"stormSurgeDefenseEfficiencies_{rp}").alias("storm_surge_defense_efficiency"),
                )
                for rp in GLOBAL_CONFIG.return_periods
            ]
        )

        self.table = (
            self.table
            .select(
                F.col(self.index_col),
                F.col("fluvial_region"),
                F.col("coastal_region"),
                F.col("pss_version"),
                F.explode(knots).alias("knot"),
            )
            .select(self.index_col, "fluvial_region", "coastal_region", "pss_version", "knot.*")
        )
        return self

    def apply_defense_efficiency(self):
        """ratio x (1 - efficiency) at each knot, then drop rows fully protected."""

        self.table = (
            self.table
            .withColumn("fluvial_building_damage_ratio", F.col("fluvial_building_damage_ratio") * (1 - F.col("fluvial_defense_efficiency")))
            .withColumn("fluvial_contents_damage_ratio", F.col("fluvial_contents_damage_ratio") * (1 - F.col("fluvial_defense_efficiency")))
            .drop("fluvial_defense_efficiency")
            .withColumn("storm_surge_building_damage_ratio", F.col("storm_surge_building_damage_ratio") * (1 - F.col("storm_surge_defense_efficiency")))
            .withColumn("storm_surge_contents_damage_ratio", F.col("storm_surge_contents_damage_ratio") * (1 - F.col("storm_surge_defense_efficiency")))
            .drop("storm_surge_defense_efficiency")
        )
        return self

    def join_simulations(self):
        fluvial_exposure = (
            self.table
            .filter(F.col("fluvial_region").isNotNull())
            .select(
                self.index_col,
                F.col("fluvial_region").alias("flood_region"),
                "pss_version",
                "return_period",
                "fluvial_building_damage_ratio",
                "fluvial_contents_damage_ratio",
            )
        )
        coastal_exposure = (
            self.table
            .filter(F.col("coastal_region").isNotNull())
            .select(
                self.index_col,
                F.col("coastal_region").alias("flood_region"),
                "pss_version",
                "return_period",
                "storm_surge_building_damage_ratio",
                "storm_surge_contents_damage_ratio"
            )
        )

        fluvial_events = self.fluvial_events
        coastal_events = self.coastal_events

        # Broadcast the smaller side explicitly; never rely on stale size estimates
        fluvial_exposure = F.broadcast(fluvial_exposure)
        coastal_exposure = F.broadcast(coastal_exposure)

        self.fluvial_table = fluvial_exposure.join(fluvial_events, on=["flood_region", "return_period"], how="inner").select(
            "item_id",
            "flood_region",
            "pss_version",
            "simulated_year",
            "event_id",
            "return_period",
            "return_period_original",
            "fluvial_building_damage_ratio",
            "fluvial_contents_damage_ratio",
            )
        self.coastal_table = coastal_exposure.join(coastal_events, on=["flood_region", "return_period"], how="inner").select(
            "item_id",
            "flood_region",
            "pss_version",
            "simulated_year",
            "event_id",
            "return_period",
            "return_period_original",
            "storm_surge_building_damage_ratio",
            "storm_surge_contents_damage_ratio",
        )

        return self

    def save_cache(self): #TODO currently unused but would need it at some point
        """Save the fluvial/coastal event loss caches to ``self.cache_output_path`` as Parquet."""
        if self.cache_output_path is not None:
            logger.info(f"Saving loss caches to {self.cache_output_path}")
            type(self).save_table(self.flv_cache, f"{self.cache_output_path}/fluvial_cache.parquet")
            type(self).save_table(self.cst_cache, f"{self.cache_output_path}/coastal_cache.parquet")
        return self

