"""Input preparation shared by tests. Uses upstream mappings (HOME_CGIC_CONFIG) and _map_column verbatim."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from cgl_dbx_flood_pipeline.sources.cgic_home import HOME_CGIC_CONFIG
from cgl_dbx_flood_pipeline.utils.spark_utils import _map_column

BOOK_LABELS = {10: "10", 100: "100", 1_000: "1k", 10_000: "10k", 100_000: "100k", 1_000_000: "1m"}


def book_table(catalog: str, schema: str, n: int) -> str:
    return f"{catalog}.{schema}.inforce_book_{BOOK_LABELS[n]}"


def pss_input(df_raw: DataFrame) -> DataFrame:
    """Raw book -> the columns score_with_pss / score_with_model_api read (what the preprocessor would emit)."""
    c = HOME_CGIC_CONFIG
    return (df_raw
            .filter(F.col("latitudeinternal").isNotNull() & F.col("longitudeinternal").isNotNull())
            .filter(F.col("latitudeinternal").between(41, 84) & F.col("longitudeinternal").between(-141, -52))
            .select(F.col("item_id_m").alias("item_id"),
                    F.col("latitudeinternal").alias("latitude"), F.col("longitudeinternal").alias("longitude"),
                    F.coalesce(_map_column(F.col("dwellingstyle_name"), c.dwellingstyle_mapping), F.lit("")).alias("dwelling_style"),
                    F.coalesce(_map_column(F.col("foundationtype_name"), c.foundationtype_mapping), F.lit("")).alias("foundation_type"),
                    F.coalesce(_map_column(F.col("riskoption_name"), c.risktype_mapping), F.lit("")).alias("risk_type")))


def project(points: list[tuple[int, float]], n_target: int) -> float:
    """Affine extrapolation t = a + b*n from the last two measured (n, seconds) points (A8). Needs >= 2 points."""
    if len(points) < 2:
        # one point cannot separate fixed start-up cost from per-row cost: no estimate, never a reason to skip
        return float("nan")
    (n1, t1), (n2, t2) = sorted(points)[-2:]
    b = max(0.0, (t2 - t1) / (n2 - n1)) if n2 != n1 else 0.0
    a = max(0.0, t2 - b * n2)
    return a + b * n_target
