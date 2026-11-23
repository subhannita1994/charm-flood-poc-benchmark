"""Utility functions for working with Sedona geometry columns on Spark DataFrames.

Geometry lives in a Spark ``DataFrame`` column (Sedona ``GeometryType``).
The source CRS must be supplied explicitly (or embedded in the
geometry SRID) so that reprojection can be expressed with Sedona ``ST_Transform``.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark.sql import st_functions as stf


def extract_epsg_number(crs: str | int) -> int:
    """Extract the numeric EPSG code from a CRS identifier.

    Converts a CRS identifier (like "EPSG:4326") to its numeric EPSG code (4326).
    If an integer is provided, it is returned unchanged. Handles case-insensitivity
    and whitespace variation in string input.

    Parameters
    ----------
    crs : str | int
        CRS identifier: either a string like "EPSG:4326" or an integer EPSG code.

    Returns
    -------
    int
        The numeric EPSG code.

    Examples
    --------
    >>> extract_epsg_number("EPSG:4326")
    4326
    >>> extract_epsg_number("epsg:3857")
    3857
    >>> extract_epsg_number(4326)
    4326
    """
    if isinstance(crs, int):
        return crs
    return int(str(crs).upper().replace("EPSG:", "").strip())


def detect_srid(df: DataFrame, geometry_col: str = "geometry") -> int | None:
    """Read the SRID actually embedded in a Sedona geometry column.

    A geometry only carries an SRID if it was explicitly set (``ST_SetSRID``) or
    the source format encoded it (e.g. GeoParquet CRS metadata, EWKB/EWKT with an
    SRID prefix). External file metadata is not inspected by this function.

    Parameters
    ----------
    df : DataFrame
        Spark DataFrame containing a Sedona geometry column.
    geometry_col : str, default "geometry"
        Name of the geometry column to inspect.

    Returns
    -------
    int | None
        The first geometry's SRID, or ``None`` if the frame is empty or the
        first SRID is null or zero. This does not validate uniformity across rows.
    """
    row = df.select(stf.ST_SRID(F.col(geometry_col)).alias("srid")).first()
    if row is None or row["srid"] in (None, 0):
        return None
    return row["srid"]


def to_crs(
    df: DataFrame,
    target_crs: str | int,
    *,
    source_crs: str | int | None = None,
    geometry_col: str = "geometry",
) -> DataFrame:
    """Reproject a Sedona geometry column to ``target_crs`` when needed.

    Parameters
    ----------
    df : DataFrame
        Spark DataFrame containing a Sedona geometry column.
    target_crs : str | int
        Target CRS as an EPSG string (e.g. ``"EPSG:4326"``) or int code (``4326``).
    source_crs : str | int | None, default None
        Source CRS of the geometry column. When ``None``, the column's embedded
        SRID (via ``ST_SRID``) is used if present; otherwise an error is raised
        since the source CRS cannot be inferred from coordinates alone.
    geometry_col : str, default "geometry"
        Name of the geometry column to reproject.

    Returns
    -------
    DataFrame
        DataFrame whose geometry column is expressed in ``target_crs`` with its
        SRID set. Matching source and target CRSes skip coordinate transformation.

    Raises
    ------
    KeyError
        If ``geometry_col`` is not present in ``df``.
    ValueError
        If ``source_crs`` is not supplied and no SRID is embedded in the geometry.
    """
    target = extract_epsg_number(target_crs)

    if geometry_col not in df.columns:
        raise KeyError(f"Geometry column '{geometry_col}' not found in DataFrame.")

    source: int | None = (
        detect_srid(df, geometry_col) if source_crs is None else extract_epsg_number(source_crs)
    )
    if source is None:
        raise ValueError(
            f"Cannot reproject '{geometry_col}': no SRID is embedded in the geometry "
            "and no source_crs was provided. Pass source_crs explicitly."
        )
    geometry = stf.ST_SetSRID(F.col(geometry_col), F.lit(source))
    if source != target:
        geometry = stf.ST_Transform(geometry, F.lit(f"epsg:{target}"))
    return df.withColumn(geometry_col, geometry)
