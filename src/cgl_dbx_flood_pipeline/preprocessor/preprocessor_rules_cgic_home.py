"""CGIC Home-specific preprocessing rules.

This module keeps the shared pipeline orchestration in
``FloodInForcePreprocessor`` and puts CGIC Home behavior into a
separate rules object.

Note: This module intentionally accesses protected members (ensure_columns,
_ensure_provinces_scored) of the FloodInForcePreprocessor as part of the
Rules pattern. Rules objects communicate with preprocessor via these
protected context methods. This is by design and not a code smell.
"""

from loguru import logger
from pyspark.sql import functions as F
from pyspark.sql.functions import col

from cgl_dbx_flood_pipeline.preprocessor.preprocessor import FloodInForcePreprocessor
from cgl_dbx_flood_pipeline.sources.cgic_home import HOME_CGIC_CONFIG
from cgl_dbx_flood_pipeline.utils import (
    _map_column,
    _spark_type,
    persist_dataframe,
    validate_required_columns,
    warn_if_rows_removed,
    warn_imputation,
)


class FloodInForceHomeCGICRules:
    """Product-specific rules for CGIC Home preprocessing.

    This class encapsulates all CGIC Home insurance product-specific logic,
    including schema mappings, categorical code transformations, and
    deductible calculation rules. All mappings and configuration are loaded
    from the CGIC Home configuration module.

    Each method in this class receives a FloodInForcePreprocessor context object
    and modifies it in-place by updating the DataFrame (ctx.table) and the
    audit log (ctx.log).
    """

    COLNAMES_MAPPING = HOME_CGIC_CONFIG.colnames_mapping
    SCHEMA = HOME_CGIC_CONFIG.schema
    DWELLINGSTYLE_MAPPING = HOME_CGIC_CONFIG.dwellingstyle_mapping
    RISKTYPE_MAPPING = HOME_CGIC_CONFIG.risktype_mapping
    FOUNDATIONTYPE_MAPPING = HOME_CGIC_CONFIG.foundationtype_mapping
    HASBASEMENT_MAPPING = HOME_CGIC_CONFIG.hasbasement_mapping
    STOREYS_MAPPING = HOME_CGIC_CONFIG.storeys_mapping
    DEFAULT_DEDUCTIBLE = HOME_CGIC_CONFIG.default_deductible
    POSSIBLE_DEDUCTIBLES = HOME_CGIC_CONFIG.possible_deductibles
    MAX_DEDUCTIBLE = HOME_CGIC_CONFIG.max_deductible

    def apply_schema(self, ctx: FloodInForcePreprocessor) -> None:
        """Apply CGIC Home schema: rename, validate, cast, and select columns.

        This method:
        1. Renames incoming columns to match expected names (via COLNAMES_MAPPING)
        2. Validates that all required columns are present
        3. Casts each column to its target Spark type (string, bigint, double, boolean)
        4. Selects and orders columns according to the CGIC Home schema

        After this step, ctx.table is guaranteed to have exactly the columns defined
        in self.SCHEMA, with correct types and names.

        Parameters
        ----------
        ctx : FloodInForcePreprocessor
            The preprocessing context object (contains DataFrame and audit log).

        Returns
        -------
        None. The ctx.table is modified in-place.
        """
        if ctx.table is None:
            raise RuntimeError(
                "No table loaded. Ensure that the preprocessor has been initialized with a valid DataFrame or file path."
            )

        # Verify all required columns are present after renaming
        validate_required_columns(ctx.table, list(self.COLNAMES_MAPPING.keys()))

        # Rename columns iteratively. withColumnRenamed is a no-op if source column doesn't exist
        renamed = ctx.table
        for source, target in self.COLNAMES_MAPPING.items():
            renamed = renamed.withColumnRenamed(source, target)

        # Fill missing values in floors with 0
        renamed = renamed.fillna({"floor": 0})

        # Select and cast columns according to the schema
        # This ensures consistent type ordering and structure
        ctx.table = renamed.select(
            *[
                col(name).try_cast(_spark_type(py_type)).alias(name)
                for name, py_type in self.SCHEMA.items()
            ]
        )

        # Mark that the schema has been applied to prevent reapplication
        ctx.schema_applied = True

    def filter_coverage(self, ctx: FloodInForcePreprocessor) -> None:
        """Keep only rows that match CGIC Home with flood coverage enabled.

        Filters the dataset to include only policies where:
        - Company is 'CGIC'
        - Product contains 'home' (case-insensitive)
        - Flood coverage is enabled (value = 1)

        Logs statistics on how many rows were removed.

        Parameters
        ----------
        ctx : FloodInForcePreprocessor
            The preprocessing context object.

        Returns
        -------
        None. The ctx.table is modified in-place.
        """
        if ctx.table is None:
            raise RuntimeError(
                "No table loaded. Ensure that the preprocessor has been initialized with a valid DataFrame or file path."
            )

        # Validate that required columns exist before filtering
        # These protected methods are intentionally used by rules objects as part of
        # the preprocessing pattern
        ctx.ensure_columns(["company", "product", "flood_coverage"])
        # Build filter conditions for each requirement
        mask_cgic = col("company") == "CGIC"
        mask_home = F.lower(col("product")).contains("home")
        mask_flood_coverage = col("flood_coverage") == 1

        # Combine all conditions with AND logic (all must be true)
        coverage_mask = mask_cgic & mask_home & mask_flood_coverage

        # Count rows before and after filtering
        counters = ctx.table.select(
            F.count(F.lit(1)).alias("total"),
            F.sum(F.when(coverage_mask, F.lit(1)).otherwise(F.lit(0))).alias("kept"),
        ).collect()[0]
        nrow_before = int(counters["total"])
        nrow_after = int(counters["kept"])

        # Apply the filter
        ctx.table = ctx.table.filter(coverage_mask)

        # Log the filtering results
        warn_if_rows_removed(
            nrow_before,
            nrow_after,
            "There are {removed} policies that are not CGIC Home policies with flood coverage.",
        )
        ctx.log["nb_wrong_product"] = nrow_before - nrow_after

    def map_codes(self, ctx: FloodInForcePreprocessor) -> None:
        """Apply all required categorical code mappings for CGIC Home.

        This coordinates two separate mapping steps:
        1. Map PolicyCenter raw codes to standardized internal codes
        2. Convert dwelling style values to storey (number of stories)

        Parameters
        ----------
        ctx : FloodInForcePreprocessor
            The preprocessing context object.

        Returns
        -------
        None. The ctx.table is modified in-place.
        """
        self.map_policy_center_codes(ctx)
        self.map_buildingstyle_to_storeys(ctx)

    def score_min_deductible(
        self,
        ctx: FloodInForcePreprocessor,
        custom_min_deductible: float | None = None,
    ) -> None:
        """Set minimum deductible based on province and risk type.

        Rules:
        - If custom_min_deductible is provided, use that value (overrides all other logic)
        - Otherwise, apply CGIC Home business rules:
          * Homeowner policies (risk_type = 'HOM') outside Quebec: $2,000 minimum
          * All other policies: $1,000 minimum

        For reference, see CGIC Home Endorsement documentation:
        https://webct.cooperatorsgroup.ca/Manuals/Home/NationalHome-CGIC-EN/wwhelp/wwhimpl/js/html/wwhelp.htm#href=03-Endorsements/NationalHome-Endorsements-ComprehensiveWater.html

        Parameters
        ----------
        ctx : FloodInForcePreprocessor
            The preprocessing context object.
        custom_min_deductible : float | None
            If provided, applies this value to all policies instead of using rules.

        Returns
        -------
        None. The ctx.table is modified in-place.
        """
        if ctx.table is None:
            raise RuntimeError(
                "No table loaded. Ensure that the preprocessor has been initialized with a valid DataFrame or file path."
            )

        # Ensure required columns are available
        # These protected methods are intentionally used by rules objects as part of
        # the preprocessing pattern
        ctx._ensure_provinces_scored()  # pylint: disable=protected-access  # type: ignore[attr-defined]
        ctx.ensure_columns([ctx.PROVINCE_COL, "risk_type"])  # pylint: disable=protected-access  # type: ignore[attr-defined]

        if custom_min_deductible is not None:
            # Override with custom value (used for testing or special cases)
            ctx.table = ctx.table.withColumn("min_deductible", F.lit(custom_min_deductible))
            ctx.log["custom_min_deductible"] = custom_min_deductible
            return

        # Apply business rules
        is_homeowner = F.coalesce(col("risk_type") == "HOM", F.lit(False))
        is_qc = F.coalesce(col(ctx.PROVINCE_COL) == "QC", F.lit(False))

        # Homeowners outside QC get higher minimum ($2,000), others get $1,000
        ctx.table = ctx.table.withColumn(
            "min_deductible",
            F.when(is_homeowner & ~is_qc, F.lit(2000)).otherwise(F.lit(1000)),
        )

    def score_max_deductible(
        self,
        ctx: FloodInForcePreprocessor,
        custom_max_deductible: float | None = None,
    ) -> None:
        """Set maximum deductible with optional constant override.

        The maximum deductible defines the ceiling on customer risk participation.
        If a custom value is provided, it is applied to all policies; otherwise,
        the CGIC Home standard maximum is used.

        For reference, see CGIC Home Endorsement documentation:
        https://webct.cooperatorsgroup.ca/Manuals/Home/NationalHome-CGIC-EN/wwhelp/wwhimpl/js/html/wwhelp.htm#href=03-Endorsements/NationalHome-Endorsements-ComprehensiveWater.html

        Parameters
        ----------
        ctx : FloodInForcePreprocessor
            The preprocessing context object.
        custom_max_deductible : float | None
            If provided, applies this value to all policies instead of the standard maximum.

        Returns
        -------
        None. The ctx.table is modified in-place.
        """
        if ctx.table is None:
            raise RuntimeError(
                "No table loaded. Ensure that the preprocessor has been initialized with a valid DataFrame or file path."
            )

        if custom_max_deductible is not None:
            # Override with custom value (used for testing or special cases)
            ctx.table = ctx.table.withColumn("max_deductible", F.lit(custom_max_deductible))
            ctx.log["custom_max_deductible"] = custom_max_deductible
            return

        # Apply the standard CGIC Home maximum deductible to all policies
        ctx.table = ctx.table.withColumn("max_deductible", F.lit(self.MAX_DEDUCTIBLE))

    def impute_deductible(
        self,
        ctx: FloodInForcePreprocessor,
        custom_deductible: float | None = None,
    ) -> None:
        """Normalize and impute deductible values outside allowed options.

        This method:
        1. Converts decimal deductibles (0 < x < 1) to percentages by multiplying by 100
           - Example: 0.5 becomes 50 (representing 50%)
        2. Identifies deductibles not in POSSIBLE_DEDUCTIBLES
        3. Replaces invalid deductibles with a standard value (or custom override)
        4. Tracks which items were imputed for audit purposes

        For reference, see CGIC Home Endorsement documentation:
        https://webct.cooperatorsgroup.ca/Manuals/Home/NationalHome-CGIC-EN/wwhelp/wwhimpl/js/html/wwhelp.htm#href=03-Endorsements/NationalHome-Endorsements-ComprehensiveWater.html

        Parameters
        ----------
        ctx : FloodInForcePreprocessor
            The preprocessing context object.
        custom_deductible : float | None
            If provided, uses this value for imputation instead of the default.

        Returns
        -------
        None. The ctx.table is modified in-place.
        """
        if ctx.table is None:
            raise RuntimeError(
                "No table loaded. Ensure that the preprocessor has been initialized with a valid DataFrame or file path."
            )

        # Ensure the deductible column exists
        # This protected method is intentionally used by rules objects as part of
        # the preprocessing pattern
        ctx.ensure_columns(["flood_deductible"])  # pylint: disable=protected-access  # type: ignore[attr-defined]

        # Step 1: Normalize decimal percentages (0-1) to whole number percentages (0-100)
        # Example: 0.5 becomes 50; 1000 stays 1000
        scaled = F.when(
            (col("flood_deductible") > 0) & (col("flood_deductible") < 1),
            col("flood_deductible") * 100,
        ).otherwise(col("flood_deductible"))
        ctx.table = ctx.table.withColumn("flood_deductible", scaled)

        # Step 2: Identify invalid deductibles (not in the allowed list)
        # Spark Connect doesn't support tuple literals in isin(), so we convert to list
        mask_invalid = ~F.coalesce(
            col("flood_deductible").isin(list(self.POSSIBLE_DEDUCTIBLES)), F.lit(False)
        )

        # Step 3: Collect IDs of rows with invalid deductibles for audit logging
        invalid_ids = [
            row[0] for row in ctx.table.filter(mask_invalid).select(ctx.index_col).collect()
        ]
        nb_imputed: int = len(invalid_ids)
        ctx.log["nb_imputed_deductibles"] = nb_imputed
        ctx.imputed_items["deductibles"] = invalid_ids

        # Warn about imputed values
        warn_imputation(
            nb_imputed,
            "There are {nb_of_imputed} items with invalid flood deductibles that were imputed with the default value.",
        )

        # Step 4: Replace invalid values with the fill value
        fill_value: float = (
            custom_deductible if custom_deductible is not None else self.DEFAULT_DEDUCTIBLE
        )
        ctx.log["imputed_deductible"] = fill_value
        ctx.table = ctx.table.withColumn(
            "flood_deductible",
            F.when(mask_invalid, F.lit(fill_value)).otherwise(col("flood_deductible")),
        )

    def drop_intermediary_columns(self, ctx: FloodInForcePreprocessor) -> None:
        """Optionally remove columns used only during preprocessing.

        These columns are intermediate values used for filtering and calculations
        but are not needed in the final output:
        - company, product, flood_coverage: used for filtering

        By default, these columns are dropped to keep the final table lean.
        Set drop_intermediary_columns=False to retain them (useful for debugging).

        Parameters
        ----------
        ctx : FloodInForcePreprocessor
            The preprocessing context object.
        drop_intermediary_columns : bool
            Whether to drop intermediary columns. Defaults to True.

        Returns
        -------
        None. The ctx.table is modified in-place.
        """
        ctx.table = ctx.table.drop("company", "product", "flood_coverage")

    def map_policy_center_codes(self, ctx: FloodInForcePreprocessor) -> None:
        """Map raw Policy Center values to standardized internal codes.

        This method converts PolicyCenter source codes to standardized internal
        representations for three categorical fields:
        - dwelling_style: building type (e.g., 'D' -> 'Detached')
        - foundation_type: foundation construction (e.g., 'C' -> 'Concrete')
        - risk_type: policy risk classification (e.g., 'HO' -> 'HOM' for homeowner)

        Values not found in the mapping dictionaries become NULL and are tracked
        in ctx.imputed_items for audit purposes. Note: Condo and Tenant policies
        often have missing dwelling/foundation data (expected), so unmapped values
        for these risk types are NOT flagged as errors.

        Parameters
        ----------
        ctx : FloodInForcePreprocessor
            The preprocessing context object.

        Returns
        -------
        None. The ctx.table is modified in-place.
        """
        if ctx.table is None:
            raise RuntimeError(
                "No table loaded. Ensure that the preprocessor has been initialized with a valid DataFrame or file path."
            )

        # Ensure all required columns are available
        # This protected method is intentionally used by rules objects as part of
        # the preprocessing pattern
        ctx.ensure_columns(["dwelling_style", "foundation_type", "risk_type"])  # pylint: disable=protected-access  # type: ignore[attr-defined]

        # Apply the mappings - values not found in the dictionary become NULL
        ctx.table = (
            ctx.table.withColumn(
                "dwelling_style", _map_column(col("dwelling_style"), self.DWELLINGSTYLE_MAPPING)
            )
            .withColumn(
                "foundation_type", _map_column(col("foundation_type"), self.FOUNDATIONTYPE_MAPPING)
            )
            .withColumn("risk_type", _map_column(col("risk_type"), self.RISKTYPE_MAPPING))
        )

        # Build diagnostic masks to identify unmapped values
        # Condo and Tenant policies are expected to have missing data, so we exclude them
        mask_condo_or_tenant = F.coalesce(col("risk_type").isin("CON", "TEN"), F.lit(False))
        mask_unmapped_dwelling_style = col("dwelling_style").isNull()
        mask_unmapped_foundation_type = col("foundation_type").isNull()
        mask_unmapped_risk_type = col("risk_type").isNull()

        # Build a DataFrame with diagnostics for each row
        diagnostics = ctx.table.select(
            col(ctx.index_col).alias("id"),
            (~mask_condo_or_tenant & mask_unmapped_dwelling_style).alias("is_unmapped_dwelling"),
            (~mask_condo_or_tenant & mask_unmapped_foundation_type).alias("is_unmapped_foundation"),
            mask_unmapped_risk_type.alias("is_unmapped_risk"),
        )

        # Persist the diagnostics DataFrame to memory and disk to optimize collection
        diagnostics, persisted = persist_dataframe(diagnostics)

        diagnostic_rows = diagnostics.collect()

        if persisted:  # Unpersist the diagnostics DataFrame to free up memory and disk space
            diagnostics.unpersist()

        # Extract IDs for each unmapped category
        ctx.imputed_items["unmapped_dwelling_styles"] = [
            row["id"] for row in diagnostic_rows if row["is_unmapped_dwelling"]
        ]
        ctx.imputed_items["unmapped_foundation_types"] = [
            row["id"] for row in diagnostic_rows if row["is_unmapped_foundation"]
        ]
        ctx.imputed_items["unmapped_risk_types"] = [
            row["id"] for row in diagnostic_rows if row["is_unmapped_risk"]
        ]

        # Log counts for each unmapped category
        ctx.log["nb_unmapped_dwelling_styles"] = len(ctx.imputed_items["unmapped_dwelling_styles"])
        ctx.log["nb_unmapped_foundation_types"] = len(
            ctx.imputed_items["unmapped_foundation_types"]
        )
        ctx.log["nb_unmapped_risk_types"] = len(ctx.imputed_items["unmapped_risk_types"])

        # Warn about unmapped values for audit purposes
        logger.warning(
            f"Number of unmapped dwelling styles: {ctx.log['nb_unmapped_dwelling_styles']}"
        )
        logger.warning(
            f"Number of unmapped foundation types: {ctx.log['nb_unmapped_foundation_types']}"
        )
        logger.warning(f"Number of unmapped risk types: {ctx.log['nb_unmapped_risk_types']}")

        # Replace NULL values with empty strings (makes downstream operations easier)
        ctx.table = ctx.table.fillna({"dwelling_style": "", "foundation_type": "", "risk_type": ""})

    def map_buildingstyle_to_storeys(self, ctx: FloodInForcePreprocessor) -> None:
        """Derive storey (number of stories) count from standardized dwelling style.

        Uses the STOREYS_MAPPING to look up the typical number of storeys for each
        dwelling style. For example, 'Bungalow' maps to 1 storey, 'Two Storey' maps to 2.

        This mapping supports flood risk scoring, as flood risk varies with building height.

        Parameters
        ----------
        ctx : FloodInForcePreprocessor
            The preprocessing context object.

        Returns
        -------
        None. The ctx.table is modified in-place.
        """
        if ctx.table is None:
            raise RuntimeError(
                "No table loaded. Ensure that the preprocessor has been initialized with a valid DataFrame or file path."
            )

        # This protected method is intentionally used by rules objects as part of
        # the preprocessing pattern
        ctx.ensure_columns(["dwelling_style"])  # pylint: disable=protected-access  # type: ignore[attr-defined]

        # Normalize mixed int/float mapping values to a single type (float)
        # This is required by Spark's create_map function which enforces type uniformity
        storeys_mapping = {key: float(value) for key, value in self.STOREYS_MAPPING.items()}

        ctx.table = ctx.table.withColumn(
            "storey", _map_column(col("dwelling_style"), storeys_mapping)
        )
