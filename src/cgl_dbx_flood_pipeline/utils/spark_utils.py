"""Utility functions for working with Spark DataFrames"""

from loguru import logger
from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from sedona.spark.sql import st_functions as stf

from ..config import GLOBAL_CONFIG
from .geo_utils import extract_epsg_number

_PY_TO_SPARK = {str: "string", int: "bigint", float: "double", bool: "boolean"}


def convert_geopandas_to_spark(gdf, spark, geometry_col=GLOBAL_CONFIG.geometry_col):
    """Convert a GeoPandas GeoDataFrame to a Sedona-enabled Spark DataFrame.

    Geometry is serialized to WKB before the Spark conversion because Arrow
    cannot directly transfer Shapely's geometry extension dtype. It is then
    reconstructed with Sedona's ``ST_GeomFromWKB``.

    Parameters
    ----------
    gdf : geopandas.GeoDataFrame
        The GeoPandas GeoDataFrame to convert.
    spark : pyspark.sql.SparkSession
        The active Spark session used to create the Spark DataFrame.
    geometry_col : str
        Name of the geometry column in the resulting Spark DataFrame.

    Returns
    -------
    pyspark.sql.DataFrame
        The equivalent Spark DataFrame.
    """
    # Convert the geometry column to WKB (Well-Known Binary) format for Spark compatibility
    gdf['geometry_wkb'] = gdf['geometry'].to_wkb()
    df_pandas = gdf.drop(columns=["geometry"])

    # Convert the Pandas DataFrame to a Spark DataFrame with valid geometries
    spark_df = (
        spark.createDataFrame(df_pandas)
        .withColumn(
            geometry_col,
            F.expr("ST_GeomFromWKB(geometry_wkb)")
        )
        .drop("geometry_wkb")
    )

    # Set the SRID (Spatial Reference System Identifier) for the geometry column based on the GeoDataFrame's CRS
    spark_df= spark_df.withColumn(
            geometry_col,
            stf.ST_SetSRID(
                F.col(geometry_col),
                F.lit(extract_epsg_number(gdf.crs)),
            )
    )
    return spark_df


def persist_dataframe(df:DataFrame, storage_level: StorageLevel = StorageLevel.MEMORY_ONLY) -> tuple[DataFrame, bool]:
    """Persist a Spark DataFrame with the specified storage level.

    Note: If the cluster is serverless, persistence may not be supported and the function will return False.

    Parameters
    ----------
    df : DataFrame
        The Spark DataFrame to persist.
    storage_level : StorageLevel, optional
        The storage level to use for persisting the DataFrame, by default StorageLevel.MEMORY_ONLY.

    Returns
    -------
    tuple[DataFrame, bool]
        The persisted DataFrame and a boolean indicating if the persistence was successful.
    """
    try:
        df = df.persist(storage_level)
        persisted = True

    # Skip persistence if cluster is serverless
    except AnalysisException:
        persisted = False
        logger.debug("Persistence not supported in serverless mode; proceeding without caching diagnostics.")

    return df, persisted


def _spark_type(py_type: type) -> str:
    """Convert a Python type to its Spark SQL equivalent string.

    Maps standard Python types (str, int, float, bool) to Spark SQL type names.
    This is used when casting DataFrame columns to ensure type consistency.

    Parameters
    ----------
    py_type : type
        The Python type to convert (e.g., str, int, float, bool).

    Returns
    -------
    str
        The Spark SQL type name (e.g., 'string', 'bigint', 'double', 'boolean').
        Defaults to 'string' if the type is not recognized.
    """
    return _PY_TO_SPARK.get(py_type, "string")


def _map_column(column: Column, mapping: dict) -> Column:
    """Apply a value mapping to a Spark DataFrame column (like pandas Series.map).

    This function creates a lookup transformation where each value in the input
    column is replaced by its corresponding value from the mapping dictionary.
    Values not found in the mapping dictionary become NULL (missing), similar to
    how pandas Series.map() behaves.

    Parameters
    ----------
    column : Column
        The Spark DataFrame column to transform.
    mapping : dict
        Dictionary mapping source values to target values.
        All values in the mapping must be the same type (required by Spark).

    Returns
    -------
    Column
        A new column with values replaced according to the mapping.
        Returns NULL for values not found in the mapping.

    Examples
    --------
    If you have a column with values ['A', 'B', 'C'] and mapping {'A': 'Group1', 'B': 'Group2'},
    the result will be: ['Group1', 'Group2', NULL]
    """
    if not mapping:
        # Empty mapping: return a column of NULL values
        return F.lit(None)

    # Build a list of alternating key-value pairs for Spark's create_map function
    # Spark's create_map() expects: F.create_map(key1, val1, key2, val2, ...)
    pairs: list[Column] = []
    for key, value in mapping.items():
        pairs.extend([F.lit(key), F.lit(value)])

    # Create the mapping and apply it to the column using subscript notation
    return F.create_map(*pairs)[column]
