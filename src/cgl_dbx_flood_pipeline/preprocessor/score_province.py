"""Assign province/territory to each record via a Sedona spatial join.

The scoring is expressed as a **broadcast** spatial join: the province polygon
set is tiny (~13 rows) and is broadcast to every executor, so Sedona builds a
spatial index on the broadcast side and probes it locally. This avoids a full
spatial shuffle. A cheap bounding-box filter runs *first* to discard clearly
out-of-Canada points before the join, minimizing the data that reaches it.
"""

from __future__ import annotations

import geopandas as gpd
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast, col
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf
from sedona.spark.sql import st_predicates as stp

from cgl_dbx_flood_pipeline.config import GLOBAL_CONFIG, PATHS
from cgl_dbx_flood_pipeline.utils import (
    extract_epsg_number,
    resolve_polygon,
    to_crs,
    validate_required_columns,
)

CANADA_BBOX: dict[str, float] = GLOBAL_CONFIG.canada_bbox
DEFAULT_CRS: str= GLOBAL_CONFIG.crs
DEFAULT_PROVINCE_CRS: str = GLOBAL_CONFIG.province_crs
DEFAULT_PROVINCES_SHP_PATH: str = PATHS.province_shp_path
PROVINCE_ABBR_REPLACEMENTS = {
    "Que": "QC",
    "Ont": "ON",
    "Man": "MB",
    "Sask": "SK",
    "Alta": "AB",
    "NWT": "NT",
    "Nvt": "NU",
    "PEI": "PE",
}


def score_province(
    table: DataFrame,
    province_polygons: str | gpd.GeoDataFrame | DataFrame | None = None,
    lat_col: str = "latitude",
    lon_col: str = "longitude",
    geometry_col: str = "geometry",
    province_col: str = "province",
    bbox: dict[str, float] | None = None,
    target_crs: str = DEFAULT_CRS,
    source_table_crs: str | int = DEFAULT_CRS,
    source_province_crs: str | int = DEFAULT_PROVINCE_CRS
) -> DataFrame:
    """Assign province/territory to each record via a Sedona spatial join.
    All non-Canada points are dropped. The output DataFrame contains all columns
    from ``table`` plus a new ``province_col`` with the province abbreviation.

    This performs a two-step geographic screening:

    1. **Bounding-box prefilter** using ``bbox`` to quickly drop points clearly
       outside Canada-like coordinate limits (lazy, pushed down, no shuffle).
     2. **Broadcast spatial containment join** against province polygons, keeping
         points covered by a province geometry, including points on its boundary.
         If a point is covered by multiple overlapping polygons, one province is
         selected deterministically from the joined rows.

    Parameters
    ----------
    table : DataFrame
        Input Spark DataFrame containing latitude/longitude columns.
    province_polygons : gpd.GeoDataFrame | DataFrame | None, default None
        Province polygons as a file path, GeoDataFrame, or Spark DataFrame.
        Paths are read with GeoPandas; GeoDataFrame inputs use their own CRS
        metadata; Spark DataFrames use ``source_province_crs``. If omitted,
        the configured default province path is used.
    lat_col, lon_col : str
        Latitude/longitude column names in ``table``.
    geometry_col : str, default "geometry"
        Input geometry column name in ``province_polygons``. If not present in table, the
        function will construct point geometries from ``lat_col`` and ``lon_col``.
    province_col : str, default "province"
        Output column name for the province abbreviation.
    bbox : dict[str, float] | None, default None
        Pre-filter bounds with keys ``lat_min``, ``lat_max``, ``lon_min``, ``lon_max``.
        Defaults to CANADA_BBOX if not provided.
    target_crs : str, default "EPSG:4326"
        Target CRS for both points and province geometries before the join.
    source_table_crs : str | int, default DEFAULT_CRS
        Source CRS of the input point coordinates.
    source_province_crs : str | int, default DEFAULT_PROVINCE_CRS
        Source CRS of Spark province geometries. GeoDataFrame inputs use their
        own ``.crs`` metadata before conversion.

    Returns
    -------
    DataFrame
        ``table`` columns (after bbox filter) plus ``province_col``. Points that
        do not fall within any province polygon are excluded. Original columns,
        including ``geometry`` when created or supplied, are preserved; only
        the internal deduplication id is removed.

    Raises
    ------
    KeyError
        If required coordinate columns are missing.
    ValueError
        If ``province_polygons`` cannot be resolved to a supported polygon input.
    """
    # Fail fast if the input table is missing the required coordinate columns.
    validate_required_columns(table, [lat_col, lon_col])

    # Resolve default variables if not provided.
    bb: dict[str, int | float] = CANADA_BBOX if bbox is None else bbox

    if province_polygons is None:
        # Resolve the configured polygon path through the common resolver.
        province_polygons = DEFAULT_PROVINCES_SHP_PATH

    # Resolve province polygons; an explicitly supplied frame takes precedence
    # over the optional shapefile path.
    prov = _resolve_province_polygons(
        province_polygons=province_polygons,
        geometry_col=geometry_col,
        province_col=province_col,
        spark=table.sparkSession,
        source_crs=source_province_crs,
        target_crs=target_crs,
    ).alias("prov")

    # Bounding-box prefilter (cheap, lazy) before the spatial join.
    candidate = table.filter(
        F.col(lat_col).between(bb["lat_min"], bb["lat_max"])
        & F.col(lon_col).between(bb["lon_min"], bb["lon_max"])
    )

    # Build or normalize the point geometry column.
    if geometry_col not in candidate.columns:
        make_geometries = stf.ST_SetSRID(
            stc.ST_Point(F.col(lon_col), F.col(lat_col)),
            F.lit(extract_epsg_number(source_table_crs)),
        )
        candidate = candidate.withColumn(geometry_col, make_geometries)
    elif dict(candidate.dtypes).get(geometry_col, "").lower() == "string":
        # An existing "geometry" column may hold plain WKT text (e.g. round-tripped
        # through CSV/pandas) rather than a real Sedona geometry; parse it if so.
        candidate = candidate.withColumn(
            geometry_col,
            stf.ST_SetSRID(
                stc.ST_GeomFromWKT(F.col(geometry_col)),
                F.lit(extract_epsg_number(source_table_crs)),
            ),
        )

    candidate = to_crs(candidate, target_crs, source_crs=source_table_crs, geometry_col=geometry_col)

    # Unique per-row id so a point matching >1 polygon (border overlaps) can be
    # deduplicated without collapsing rows that happen to share the same geometry.
    row_id_col = "_score_province_row_id"
    candidate = candidate.withColumn(row_id_col, F.monotonically_increasing_id())

    # Perform a spatial join between points and province polygons, keeping exactly
    # one matching province per input row. Province code is a stable tie-breaker;
    # it does not represent a business priority.
    # ST_Intersects includes boundary-touching points and is available in the
    # cluster Sedona runtime where ST_CoveredBy is not registered.
    dedup_window = Window.partitionBy(
        col(f"exposure.`{row_id_col}`")
    ).orderBy(col(f"prov.`{province_col}`").asc_nulls_last())
    scored_provinces = (
        candidate.alias("exposure")
        .join(
            broadcast(prov).alias("prov"),
            on=stp.ST_Intersects(col(f"exposure.`{geometry_col}`"), col(f"prov.`{geometry_col}`")),
            how="inner"
        )
        .withColumn("_row_number", F.row_number().over(dedup_window))
        .filter(col("_row_number") == 1)
    )

    # Return original columns plus the selected province; omit only the internal row id.
    select_cols = [col(f"exposure.`{col_name}`") for col_name in candidate.columns if col_name != row_id_col]
    select_cols.append(col(f"prov.`{province_col}`"))
    return scored_provinces.select(*select_cols)


def _resolve_province_polygons(
    province_polygons: str | DataFrame | gpd.GeoDataFrame,
    geometry_col: str,
    province_col: str,
    spark: SparkSession,
    source_crs: str | int = DEFAULT_CRS,
    target_crs: str = DEFAULT_CRS,
) -> DataFrame:
    """Load, reproject, and normalize province polygons to a 2-column frame.

    Returns a DataFrame with exactly ``[province_col, geometry_col]``.
    """
    province_polygons = resolve_polygon(
        province_polygons,
        spark,
        source_crs=source_crs,
        target_crs=target_crs,
        geometry_col=geometry_col
    )

    # Validate that the provided DataFrame has the required columns.
    validate_required_columns(province_polygons, ["PREABBR", geometry_col])

    return province_polygons.select(
        normalize_province_abbr(F.col("PREABBR")).alias(province_col),
        F.col(geometry_col),
    )


def normalize_province_abbr(column: F.Column) -> F.Column:
    """Normalize province/territory abbreviations to standard 2-letter codes.

    The column is cast to string, trimmed, stripped of periods and hyphens, then
    standardized (e.g. ``Que`` -> ``QC``, ``PEI`` -> ``PE``).

    Parameters
    ----------
    column : Column
        Column of raw province/territory abbreviations.

    Returns
    -------
    Column
        Normalized abbreviation column.
    """
    normalized = F.trim(column.cast("string"))
    normalized = F.regexp_replace(normalized, r"[.\-]", "")
    for old, new in PROVINCE_ABBR_REPLACEMENTS.items():
        normalized = F.regexp_replace(normalized, old, new)

    return normalized
