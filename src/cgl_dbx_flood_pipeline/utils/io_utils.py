"""Utility functions for reading and writing in-force tables with Spark."""

from __future__ import annotations

from json import dumps
from pathlib import Path

import geopandas as gpd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from sedona.spark.sql import st_functions as stf

from cgl_dbx_flood_pipeline.config import GLOBAL_CONFIG

from .geo_utils import extract_epsg_number, to_crs
from .spark_utils import convert_geopandas_to_spark


def resolve_polygon(
    polygons: str | DataFrame | gpd.GeoDataFrame,
    spark: SparkSession,
    source_crs: int | str = GLOBAL_CONFIG.crs,
    target_crs: int | str = GLOBAL_CONFIG.crs,
    geometry_col: str = GLOBAL_CONFIG.geometry_col,
) -> DataFrame:
    """Resolve a path, GeoDataFrame, or Spark geometry frame to Spark.

    Path inputs are read with GeoPandas. GeoDataFrames are reprojected using
    their own CRS metadata and transferred through WKB. Spark DataFrames are
    transformed with ``to_crs`` using the explicitly supplied ``source_crs``.

    Parameters
    ----------
    polygons : str | DataFrame | gpd.GeoDataFrame
        Polygon input as a file path, Spark DataFrame, or GeoDataFrame.
    spark : SparkSession
        Active Spark session used to convert GeoDataFrames to Spark.
    source_crs : int | str, default GLOBAL_CONFIG.crs
        CRS of Spark DataFrame geometries. Ignored for GeoDataFrame inputs,
        whose ``.crs`` metadata is authoritative.
    target_crs : int | str, default GLOBAL_CONFIG.crs
        CRS required for the returned Spark geometry column.
    geometry_col : str, default GLOBAL_CONFIG.geometry_col
        Name of the geometry column in Spark DataFrame inputs.

    Returns
    -------
    DataFrame
        Spark DataFrame containing geometries expressed in ``target_crs``.
    """
    if isinstance(polygons, str):
        polygons = gpd.read_file(polygons)

    if isinstance(polygons, gpd.GeoDataFrame):
        polygons = polygons.to_crs(target_crs)
        return convert_geopandas_to_spark(polygons, spark, geometry_col)

    if isinstance(polygons, DataFrame):
        polygons = polygons.withColumn(
            geometry_col,
            stf.ST_SetSRID(
                F.col(geometry_col),
                F.lit(extract_epsg_number(source_crs)),
            ),
        )
        return to_crs(polygons, target_crs, source_crs=source_crs, geometry_col=geometry_col)

    raise TypeError("Polygons must be a path, GeoDataFrame, or Spark DataFrame")


def save_table(df: DataFrame, path: str, *, mode: str = "overwrite", **options) -> None:
    """Write a Spark DataFrame to Delta table, CSV or Parquet.

    Parameters
    ----------
    df : DataFrame
        Spark DataFrame to persist.
    path : str
        Output path (directory) or table name.
        Format is inferred from the file extension (CSV or Parquet) or
        treated as a table name if no extension is present.
    mode : str, default "overwrite"
        Spark save mode.
    **options
        Additional writer options forwarded to Spark.
    """
    file_format = Path(path).suffix.lower().lstrip(".")
    writer = df.write.mode(mode).options(**options)
    if file_format == "csv":
        writer.option("header", "true").csv(path)
    elif file_format == "parquet":
        writer.parquet(path)
    else:
        try:
            writer.saveAsTable(path)
        except Exception as err:
            raise ValueError(f"Failed to save DataFrame to {path}.\nError: {err}") from err


def save_json(data: dict, path: str) -> None:
    """Save a dictionary to a JSON file.

    Parameters
    ----------
    data : dict
        Dictionary to save.
    path : str
        File path to save the JSON file.
    """
    if Path(path).suffix.lower() != ".json":
        raise ValueError("Filepath must end with .json")
    with open(path, "w", encoding="utf-8") as f:
        f.write(dumps(data, indent=2))


def read_table(spark: SparkSession, path: str, **options) -> DataFrame:
    """Read a Delta table, CSV or Parquet source into a Spark DataFrame.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session used to read the source.
    path : str
        Input path. Ignored if format is "table".
    **options
        Additional reader options forwarded to Spark (override the CSV defaults
        ``header=true`` / ``inferSchema=true``).

    Returns
    -------
    DataFrame
        Spark DataFrame containing the source data.
    """
    file_format = Path(path).suffix.lower().lstrip(".")
    if file_format == "csv":
        defaults = {"header": "true", "inferSchema": "true"}
        return spark.read.options(**{**defaults, **options}).csv(path)
    if file_format == "parquet":
        return spark.read.options(**options).parquet(path)
    try:
        return spark.read.options(**options).table(path)
    except Exception as err:
        raise ValueError(f"Failed to read DataFrame from {path}.\nError: {err}") from err
