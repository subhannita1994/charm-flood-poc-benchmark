"""Utility functions for filtering bad location data on Spark DataFrames.

These are pure, lazy transformations: they build column predicates and rely on
Spark to fuse and push them down. No shuffles or joins are introduced, and no
eager ``count()`` actions are triggered here — row-count logging is done once at
explicit pipeline checkpoints instead.

IMPORTANT NOTE: Coordinates are expected to be in EPSG:4326 (WGS84) decimal degrees,
with latitude in the northern hemisphere and longitude in the western hemisphere.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from cgl_dbx_flood_pipeline.config import GLOBAL_CONFIG
from cgl_dbx_flood_pipeline.utils import validate_required_columns

CANADA_BBOX: dict[str, float] = GLOBAL_CONFIG.canada_bbox
PROVINCE_CENTROIDS: list[tuple[str, float, float]] = [
    ("ON", 50.08990811, -85.83358805),
    ("QC", 53.01757475, -70.92960413),
    ("QC", 53.00482000, -70.77841000),
    ("NL", 52.64990765, -60.24440640),
    ("AB", 55.16720057, -114.51205581),
    ("SK", 54.41643638, -105.89182305),
    ("BC", 54.56061105, -125.10931540),
]


def remove_missing_coordinates(
    table: DataFrame,
    lat_col: str = "latitude",
    lon_col: str = "longitude",
) -> DataFrame:
    """Remove rows with missing or invalid coordinate values.

    A row is kept only when:
    - ``lat_col`` and ``lon_col`` are both non-null,
    - latitude is strictly positive (``> 0``),
    - longitude is strictly negative (``< 0``).

    This rule is intended for Canada-focused data quality checks where valid
    coordinates are expected in the northern and western hemispheres.

    Parameters
    ----------
    table : DataFrame
        Input Spark DataFrame containing coordinate columns.
    lat_col : str, default "latitude"
        Name of the latitude column.
        Latitude is expected to be in decimal degrees (EPSG:4326), northern hemisphere.
    lon_col : str, default "longitude"
        Name of the longitude column.
        Longitude is expected to be in decimal degrees (EPSG:4326), western hemisphere.

    Returns
    -------
    DataFrame
        Filtered DataFrame with rows failing coordinate checks removed.

    Raises
    ------
    KeyError
        If required coordinate columns are missing.
    """
    validate_required_columns(table, [lat_col, lon_col])

    cleaned_table = table.filter(F.col(lat_col).isNotNull() & F.col(lon_col).isNotNull() & (F.col(lat_col) > 0) & (F.col(lon_col) < 0))

    if cleaned_table.count() == 0:
        raise ValueError(
            "All coordinates are missing or invalid. "
            "Coordinates are expected to be in projection EPSG:4326 (WGS84) decimal degrees, "
            "with latitude in the northern hemisphere and longitude in the western hemisphere."
        )

    return cleaned_table


def remove_province_centroids(
    table: DataFrame,
    lat_col: str = "latitude",
    lon_col: str = "longitude",
    tolerance: float = 0.04,
) -> DataFrame:
    """Remove records geocoded at/near known province centroid coordinates.

    A row is flagged as a centroid match when both latitude and longitude are
    within ``tolerance`` (absolute difference) of any known centroid pair. The
    centroid set is tiny (7 points), so matching is expressed as a single boolean
    column predicate rather than a join to avoid any shuffle.

    Parameters
    ----------
    table : DataFrame
        Input Spark DataFrame containing latitude/longitude columns.
    lat_col : str, default "latitude"
        Latitude column name in ``table``.
    lon_col : str, default "longitude"
        Longitude column name in ``table``.
    tolerance : float, default 0.04
        Maximum absolute difference allowed for both latitude and longitude when
        matching a record to a centroid. Should not exceed 0.04 degrees (~4.4 km)
        to avoid removing valid non-centroid locations.

    Returns
    -------
    DataFrame
        DataFrame with centroid-matching rows removed.

    Raises
    ------
    KeyError
        If required coordinate columns are missing.
    """
    validate_required_columns(table, [lat_col, lon_col])

    is_centroid = F.lit(False)
    for _, clat, clon in PROVINCE_CENTROIDS:
        is_centroid = is_centroid | ((F.abs(F.col(lat_col) - F.lit(clat)) <= tolerance) & (F.abs(F.col(lon_col) - F.lit(clon)) <= tolerance))

    return table.filter(~is_centroid)


def assert_valid_coordinates(
    table: DataFrame,
    bbox: dict[str, float] | None = None,
    lat_col: str = "latitude",
    lon_col: str = "longitude",
) -> None:
    """Assert that coordinates are valid:
    - latitude is strictly positive (``> 0``),
    - longitude is strictly negative (``< 0``),
    - most coordinates fall within the specified bounding box (``bbox``).

    Parameters
    ----------
    table : DataFrame
        Input Spark DataFrame containing latitude/longitude columns.
    bbox : dict[str, float], default CANADA_BBOX
        Bounding box with keys ``lat_min``, ``lat_max``, ``lon_min``, ``lon_max``.
    lat_col : str, default "latitude"
        Latitude column name in ``table``.
    lon_col : str, default "longitude"
        Longitude column name in ``table``.

    Raises
    ------
    KeyError
        If required coordinate columns are missing.
    ValueError
        If any coordinates fall outside the bounding box.
    """
    validate_required_columns(table, [lat_col, lon_col])

    bb: dict[str, int | float] = CANADA_BBOX if bbox is None else bbox

    within_bbox = table.filter(
        (F.col(lat_col) >= bb["lat_min"])
        & (F.col(lat_col) <= bb["lat_max"])
        & (F.col(lon_col) >= bb["lon_min"])
        & (F.col(lon_col) <= bb["lon_max"])
    )
    if within_bbox.count() == 0:
        raise ValueError(
            f"All coordinates are outside the expected Canada-like bounding box: {bb}. "
            "Coordinates are expected to be in projection EPSG:4326 (WGS84) decimal degrees, "
            "with latitude in the northern hemisphere and longitude in the western hemisphere."
        )
