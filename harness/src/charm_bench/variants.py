"""Variants under test. Every function states whether it is upstream-verbatim or a Databricks proposal."""
from __future__ import annotations

import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

_PANDAS_TO_SPARK_TYPE = {"Float64": DoubleType(), "Int64": IntegerType(), "string": StringType()}
RETRYABLE_STATUS = {408, 429, 500, 502, 503, 504}


def set_arrow_batch(spark, n: int) -> bool:
    """Serverless blocks spark.sql.execution.arrow.maxRecordsPerBatch; fall back to its default (10,000)."""
    try:
        spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", str(n))
        return True
    except Exception:  # noqa: BLE001
        return False


class FloodApiHTTPError(RuntimeError):
    """VERBATIM from scratch/test_paralle_api (Oct 2026)."""

    def __init__(self, status_code: int, retry_after: float | None = None):
        super().__init__(f"Flood API returned HTTP {status_code}.")
        self.status_code = status_code
        self.retry_after = retry_after


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, (ConnectionError, TimeoutError)):
        return True
    if isinstance(exc, FloodApiHTTPError):
        return exc.status_code in RETRYABLE_STATUS
    return isinstance(exc, RuntimeError) and "HTTP 5" in str(exc)  # upstream post_json raises RuntimeError("...HTTP 503.")


# --------------------------------------------------------------------------------------------------
# VERBATIM (logic) — scratch/test_paralle_api.score_with_model_api: retry + repartition(max_workers*2)
# --------------------------------------------------------------------------------------------------
def scratch_score_with_model_api(table: DataFrame, *, index_col: str, effective_date: str, model_api,
                                 max_records_per_batch: int = 1000, max_error_rate=0.05, max_attempts: int = 4,
                                 backoff_s: float = 1) -> DataFrame:
    max_workers = model_api.worker_count()
    table = table.repartition(max_workers * 2)
    set_arrow_batch(table.sparkSession, max_records_per_batch)  # upstream: unconditional conf.set (fails on serverless)
    pandas_schema = model_api.pandas_schema
    empty_record = dict.fromkeys(pandas_schema)
    pandas_schema = pandas_schema | {"scoring_error": "string"}
    input_columns = tuple(model_api.input_schema)
    output_schema = StructType([StructField(index_col, StringType(), nullable=False)] + [
        StructField(n, _PANDAS_TO_SPARK_TYPE[t], nullable=True) for n, t in pandas_schema.items()])
    call_row = model_api.score_row

    def safe_call(values, effective_date):
        for attempt in range(max_attempts):
            try:
                return {**call_row(values, effective_date), "scoring_error": None}
            except Exception as exc:  # noqa: BLE001
                if attempt + 1 < max_attempts and _is_retryable(exc):
                    wait = getattr(exc, "retry_after", None) or min(backoff_s * 2 ** attempt, 30)
                    time.sleep(wait * random.uniform(0.5, 1.5))
                    continue
                return {**empty_record, "scoring_error": f"{type(exc).__name__}: {exc}"[:500]}

    def score_partition(batches):
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for batch in batches:
                if batch.empty:
                    continue
                row_values = batch[list(input_columns)].itertuples(index=False, name=None)
                records = list(executor.map(lambda values: safe_call(values, effective_date), row_values))
                failed = sum(r["scoring_error"] is not None for r in records)
                if failed / len(records) > max_error_rate:
                    raise RuntimeError(f"{failed}/{len(records)} rows failed; aborting partition.")
                output = pd.DataFrame.from_records(records, columns=list(pandas_schema))
                output = output.astype(pandas_schema, errors="raise")
                output.insert(0, index_col, batch[index_col].astype("string").to_numpy())
                yield output

    return table.select(index_col, *input_columns).mapInPandas(score_partition, schema=output_schema)


# --------------------------------------------------------------------------------------------------
# DATABRICKS PROPOSAL — parallelism contract: partitions tied to cluster cores, threads tied to I/O latency,
# one persistent pool per Python worker, bounded retries. Same output contract as upstream.
# --------------------------------------------------------------------------------------------------
_POOL: ThreadPoolExecutor | None = None
_POOL_SIZE = 0
_POOL_LOCK = threading.Lock()


def _pool(size: int) -> ThreadPoolExecutor:
    global _POOL, _POOL_SIZE  # noqa: PLW0603
    with _POOL_LOCK:
        if _POOL is None or _POOL_SIZE != size:
            _POOL = ThreadPoolExecutor(max_workers=size, thread_name_prefix="pss-api")
            _POOL_SIZE = size
    return _POOL


def tuned_score_with_model_api(table: DataFrame, *, index_col: str, effective_date: str, model_api,
                               num_partitions: int, threads_per_task: int, max_records_per_batch: int = 500,
                               max_attempts: int = 4, backoff_s: float = 0.5, max_error_rate: float = 0.05,
                               dedupe: bool = False) -> DataFrame:
    input_columns = list(model_api.input_schema)
    pandas_schema = model_api.pandas_schema | {"scoring_error": "string"}
    empty_record = dict.fromkeys(model_api.pandas_schema)
    output_schema = StructType([StructField(index_col, StringType(), nullable=False)] + [
        StructField(n, _PANDAS_TO_SPARK_TYPE[t], nullable=True) for n, t in pandas_schema.items()])
    call_row = model_api.score_row
    set_arrow_batch(table.sparkSession, max_records_per_batch)

    src = table.select(index_col, *input_columns)
    if dedupe:  # score each distinct input once (deterministic endpoint), join back afterwards
        keyed = src.withColumn("_k", F.xxhash64(*input_columns).cast("string"))
        payload = keyed.dropDuplicates(["_k"]).select(F.col("_k").alias(index_col), *input_columns)
    else:
        payload = src

    def safe_call(values):
        for attempt in range(max_attempts):
            try:
                return {**call_row(values, effective_date), "scoring_error": None}
            except Exception as exc:  # noqa: BLE001
                if attempt + 1 < max_attempts and _is_retryable(exc):
                    time.sleep(min(backoff_s * 2 ** attempt, 10) * random.uniform(0.5, 1.5))
                    continue
                return {**empty_record, "scoring_error": f"{type(exc).__name__}: {exc}"[:500]}

    def score_partition(batches):
        pool = _pool(threads_per_task)
        for batch in batches:
            if batch.empty:
                continue
            records = list(pool.map(safe_call, batch[input_columns].itertuples(index=False, name=None)))
            failed = sum(r["scoring_error"] is not None for r in records)
            if failed / len(records) > max_error_rate:
                raise RuntimeError(f"{failed}/{len(records)} rows failed; aborting partition.")
            out = pd.DataFrame.from_records(records, columns=list(pandas_schema)).astype(pandas_schema, errors="raise")
            out.insert(0, index_col, batch[index_col].astype("string").to_numpy())
            yield out

    scored = payload.repartition(num_partitions).mapInPandas(score_partition, schema=output_schema)
    if dedupe:
        scored = keyed.select(index_col, "_k").join(
            scored.withColumnRenamed(index_col, "_k"), on="_k", how="left").drop("_k")
    return scored


# --------------------------------------------------------------------------------------------------
# DATABRICKS PROPOSAL — FloodSimulationMerge with the join hint flipped: broadcast the (small, per-region)
# events side instead of the 7M-row exposure side; or no hint and let AQE choose.
# --------------------------------------------------------------------------------------------------
def make_fixed_merge(base_cls, mode: str = "broadcast_events"):
    class FixedFloodSimulationMerge(base_cls):
        def join_simulations(self):
            idx = self.index_col
            flv = (self.table.filter(F.col("fluvial_region").isNotNull())
                   .select(idx, F.col("fluvial_region").alias("flood_region"), "pss_version", "return_period",
                           "fluvial_building_damage_ratio", "fluvial_contents_damage_ratio"))
            cst = (self.table.filter(F.col("coastal_region").isNotNull())
                   .select(idx, F.col("coastal_region").alias("flood_region"), "pss_version", "return_period",
                           "storm_surge_building_damage_ratio", "storm_surge_contents_damage_ratio"))
            fe, ce = self.fluvial_events, self.coastal_events
            if mode == "broadcast_events":
                fe, ce = F.broadcast(fe), F.broadcast(ce)
            keys = ["flood_region", "return_period"]
            self.fluvial_table = flv.join(fe, on=keys, how="inner").select(
                idx, "flood_region", "pss_version", "simulated_year", "event_id", "return_period",
                "return_period_original", "fluvial_building_damage_ratio", "fluvial_contents_damage_ratio")
            self.coastal_table = cst.join(ce, on=keys, how="inner").select(
                idx, "flood_region", "pss_version", "simulated_year", "event_id", "return_period",
                "return_period_original", "storm_surge_building_damage_ratio", "storm_surge_contents_damage_ratio")
            return self
    FixedFloodSimulationMerge.__name__ = f"FixedFloodSimulationMerge_{mode}"
    return FixedFloodSimulationMerge


# --------------------------------------------------------------------------------------------------
# STAND-IN (A6) — loss aggregation the upload references (LossPipeline) but does not contain.
# loss = (building DR x building_value + contents DR x content_value), net of deductible, capped at flood_limit;
# per item: AAL = sum(year losses)/N_years, plus 1-in-100 / 1-in-250 annual-max (OEP-style) per item.
# --------------------------------------------------------------------------------------------------
def aggregate_losses(fluvial_table: DataFrame, coastal_table: DataFrame, financial: DataFrame, n_years: int = 50_000,
                     index_col: str = "item_id") -> DataFrame:
    fin = financial.select(index_col, "building_value", "content_value", "flood_deductible", "flood_limit")
    ev = (fluvial_table.select(index_col, "simulated_year", "event_id",
                               F.col("fluvial_building_damage_ratio").alias("bdr"), F.col("fluvial_contents_damage_ratio").alias("cdr"))
          .unionByName(coastal_table.select(index_col, "simulated_year", "event_id",
                                            F.col("storm_surge_building_damage_ratio").alias("bdr"),
                                            F.col("storm_surge_contents_damage_ratio").alias("cdr"))))
    gross = ev.join(fin, on=index_col).withColumn(
        "loss", F.least(F.greatest(F.col("bdr") * F.col("building_value") + F.col("cdr") * F.col("content_value")
                                   - F.col("flood_deductible"), F.lit(0.0)), F.col("flood_limit")))
    yearly = gross.groupBy(index_col, "simulated_year").agg(F.sum("loss").alias("year_loss"), F.max("loss").alias("year_max"))
    return yearly.groupBy(index_col).agg(
        (F.sum("year_loss") / F.lit(n_years)).alias("aal"),
        F.max("year_max").alias("max_event_loss"),
        F.count("*").alias("loss_years"))
