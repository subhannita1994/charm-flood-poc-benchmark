"""
Peril Score Service (PSS) for Geomap Scoring.

This module calls modules from the GeomapAPI
https://github.com/Cooperators-BI/Flood-Model-Serving/tree/main/libs/flood_serving_project

The PSS endpoint only exposes a row-level ``predict()`` call that consumes one row as a
mapping of keyword arguments, so the row-by-row call cannot be avoided. Rather than
collecting the table to the driver and fanning out with a ``ProcessPoolExecutor`` (which
is bounded by the driver's cores and by driver memory), this module streams Arrow batches
through ``mapInPandas`` so the row-level calls run on every executor core in the cluster.

Compute-time design choices:

* Two levels of parallelism. Spark runs one ``mapInPandas`` task per executor core, and
  each task fans its rows out to a persistent ``ThreadPoolExecutor`` of
  ``threads_per_task`` threads. ``predict()`` is I/O-bound (raster and shapefile reads
  release the GIL), so threads overlap the I/O waits and concurrency becomes
  ``cluster cores x threads_per_task`` instead of ``cluster cores``.
* Endpoint scope. With ``endpoint_scope="thread"`` each pool thread owns its endpoint,
  which is safe even if the underlying file handles are not thread-safe, at the cost of
  one set of assets per thread. ``endpoint_scope="worker"`` shares one endpoint per
  Python worker across its threads (lowest memory) and is only safe if ``predict()`` is
  thread-safe.
* Distinct inputs are scored once. ``predict()`` is deterministic for a given endpoint
  version, so the payload is deduplicated on the predict inputs before the shuffle and
  scores are joined back to every ``index_col`` with null-safe equality.
* Partitions are oversubscribed (``partitions_per_core``) so a slow partition does not
  leave the other cores idle at the end of the stage.
* The thread pool and the endpoints are cached per Python worker and survive across
  batches and tasks (Spark Python worker reuse), so assets load once per thread.
  The driver does not initialize model assets.
"""

from __future__ import annotations

import os
import threading
from collections.abc import Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import lru_cache
from types import ModuleType
from typing import Any

import pandas as pd
from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from cgl_dbx_flood_pipeline.config.pss_versions import PSSVersionsMapping

# Pandas dtypes for the PSS output columns, kept identical to the reference implementation.
_PSS_SCHEMA: dict[str, Any] = {
    "distanceToWater": "Float64",
    "elevationToWater": "Float64",
    "exclusionZoneIndicator": str,
    "fluvialReturnPeriodCode": "Int64",
    "fluvialDefenseStandardofProtection": "Int64",
    "partialFluvialDefenseIndicator": str,
    "stormSurgeReturnPeriodCode": "Int64",
    "stormSurgeDefenseStandardofProtection": "Int64",
    "partialStormSurgeDefenseIndicator": str,
    "fluvialFloodZone": "Int64",
    "stormSurgeFloodZone": "Int64",
    "fluvialBuildingDamageRatios_20": "Float64",
    "fluvialBuildingDamageRatios_50": "Float64",
    "fluvialBuildingDamageRatios_75": "Float64",
    "fluvialBuildingDamageRatios_100": "Float64",
    "fluvialBuildingDamageRatios_200": "Float64",
    "fluvialBuildingDamageRatios_500": "Float64",
    "fluvialBuildingDamageRatios_1500": "Float64",
    "fluvialContentsDamageRatios_20": "Float64",
    "fluvialContentsDamageRatios_50": "Float64",
    "fluvialContentsDamageRatios_75": "Float64",
    "fluvialContentsDamageRatios_100": "Float64",
    "fluvialContentsDamageRatios_200": "Float64",
    "fluvialContentsDamageRatios_500": "Float64",
    "fluvialContentsDamageRatios_1500": "Float64",
    "fluvialDefenseEfficiencies_20": "Float64",
    "fluvialDefenseEfficiencies_50": "Float64",
    "fluvialDefenseEfficiencies_75": "Float64",
    "fluvialDefenseEfficiencies_100": "Float64",
    "fluvialDefenseEfficiencies_200": "Float64",
    "fluvialDefenseEfficiencies_500": "Float64",
    "fluvialDefenseEfficiencies_1500": "Float64",
    "stormSurgeBuildingDamageRatios_20": "Float64",
    "stormSurgeBuildingDamageRatios_50": "Float64",
    "stormSurgeBuildingDamageRatios_75": "Float64",
    "stormSurgeBuildingDamageRatios_100": "Float64",
    "stormSurgeBuildingDamageRatios_200": "Float64",
    "stormSurgeBuildingDamageRatios_500": "Float64",
    "stormSurgeBuildingDamageRatios_1500": "Float64",
    "stormSurgeContentsDamageRatios_20": "Float64",
    "stormSurgeContentsDamageRatios_50": "Float64",
    "stormSurgeContentsDamageRatios_75": "Float64",
    "stormSurgeContentsDamageRatios_100": "Float64",
    "stormSurgeContentsDamageRatios_200": "Float64",
    "stormSurgeContentsDamageRatios_500": "Float64",
    "stormSurgeContentsDamageRatios_1500": "Float64",
    "stormSurgeDefenseEfficiencies_20": "Float64",
    "stormSurgeDefenseEfficiencies_50": "Float64",
    "stormSurgeDefenseEfficiencies_75": "Float64",
    "stormSurgeDefenseEfficiencies_100": "Float64",
    "stormSurgeDefenseEfficiencies_200": "Float64",
    "stormSurgeDefenseEfficiencies_500": "Float64",
    "stormSurgeDefenseEfficiencies_1500": "Float64",
}

_PANDAS_TO_SPARK_TYPE = {
    "Float64": DoubleType(),
    "Int64": IntegerType(),
    str: StringType(),
}

# Maps each input table column onto the ``predict()`` keyword argument it feeds. Renaming a
# batch with these maps turns ``to_dict(orient="records")`` output into ready-to-splat kwargs.
_HOME_PREDICT_KWARGS: dict[str, str] = {
    "latitude": "lat",
    "longitude": "long",
    "dwelling_style": "building_style",
    "foundation_type": "foundation_type",
    "risk_type": "risk_type",
}
_COMMERCIAL_PREDICT_KWARGS: dict[str, str] = {
    "latitude": "lat",
    "longitude": "long",
    "ibc_code": "ibc_code",
}

_HOME_COLUMNS = list(_HOME_PREDICT_KWARGS)
_COMMERCIAL_COLUMNS = list(_COMMERCIAL_PREDICT_KWARGS)

# predict() expects native floats for the coordinates; cast the whole column once per batch.
_FLOAT_PREDICT_KWARGS = ("lat", "long")

_ARROW_BATCH_CONF = "spark.sql.execution.arrow.maxRecordsPerBatch"

# "thread": one endpoint per pool thread (safe for non-thread-safe file handles).
# "worker": one endpoint per Python worker shared by its threads (requires thread safety).
_ENDPOINT_SCOPES = ("thread", "worker")

# Per-Python-worker state. With spark.python.worker.reuse (default true) the pool and the
# endpoints held by its threads survive across batches and tasks.
_THREAD_STATE = threading.local()
_EXECUTOR: ThreadPoolExecutor | None = None
_EXECUTOR_SIZE: int = 0
_EXECUTOR_LOCK = threading.Lock()


def score_with_pss(
    table: DataFrame,
    *,
    index_col: str,
    cie: str = "CGIC",
    product: str = "home",
    pss_endpoint_version: str = "v1_1",  # Possibilities: v1, v1_1, v2_index_100.
    max_records_per_batch: int = 1000,
    partitions_per_core: int = 2,
    endpoint_scope: str = "worker",
    endpoint_memory_mb: int = 256,
    max_threads_per_task: int = 16,
) -> DataFrame:
    """Score every row of ``table`` against the Peril Score Service (PSS).

    Parameters
    ----------
    table : DataFrame
        Spark table prepared for PSS scoring. Must contain ``index_col`` plus the
        product-specific input columns (see ``_HOME_COLUMNS``/``_COMMERCIAL_COLUMNS``).
    index_col : str
        Column used to join the PSS output back onto the caller's table.
    cie : str
        Company which we want to score with the PSS. Either "CGIC", "SGIC" or "CUMIS".
    product : str
        Line of business for which we want to score with the PSS. Either "home", "farm",
        "comm" or "spec".
    pss_endpoint_version : str
        PSS endpoint version tag. Current options are "v1", "v1_1" and "v2_index_100".
    max_records_per_batch : int
        When set, applies ``spark.sql.execution.arrow.maxRecordsPerBatch`` to the active
        session. Smaller batches lower the peak memory held by each Python worker; larger
        batches amortize the per-batch conversion. This is a session-level setting, so it
        also affects other queries running on the same session. Defaults to 1000.
    partitions_per_core : int
        Partitions created per cluster core, to balance stragglers. Cores are detected
        from the attached cluster (maximum size for autoscaling clusters). Defaults to 2.
    endpoint_scope : str
        ``"thread"`` (one endpoint per thread, safe default) or ``"worker"`` (one shared
        endpoint per Python worker, only if ``predict()`` is thread-safe).
    endpoint_memory_mb : int
        Estimated resident memory of one endpoint instance, used to size the automatic
        thread count. Measure it once and set it accurately. Defaults to 256.
    max_threads_per_task : int
        Upper bound for the automatic thread count. Defaults to 16.

    Returns
    -------
    DataFrame
        A DataFrame with ``index_col`` and the PSS output columns (see ``_PSS_SCHEMA``),
        one row per input row. Evaluation is lazy; unavailable endpoints or
        model assets raise on workers when an action evaluates the scores.

    Notes
    -----
    Concurrency is ``cluster cores x threads_per_task``; partitions only balance load.
    With ``endpoint_scope="thread"`` each executor holds up to
    ``cores x threads_per_task`` endpoint copies. Peak Python-worker memory is bounded by
    the Arrow batch size rather than by the partition size, because batches stream
    through the iterator instead of being materialized together.
    Service reuse across batches requires Spark Python worker reuse. Each worker
    caches at most one endpoint configuration; changing company, product or version
    rebuilds it.
    """
    # Normalize and validate the input parameters, returning the canonical names and version ID.
    product_name, cie_name, pss_version = _normalize_and_validate(
        cie, product, pss_endpoint_version
    )

    # Select the input columns and their predict() keyword names based on the product type.
    predict_kwargs: dict[str, str] = (
        _HOME_PREDICT_KWARGS if product_name == "HOME" else _COMMERCIAL_PREDICT_KWARGS
    )
    input_cols: list[str] = list(predict_kwargs)

    # Fail fast on the driver rather than on a worker that already paid for model assets.
    missing_columns = [name for name in (index_col, *input_cols) if name not in table.columns]
    if missing_columns:
        raise ValueError(f"PSS input table is missing required columns: {missing_columns}.")

    for name, value in (
        ("max_records_per_batch", max_records_per_batch),
        ("partitions_per_core", partitions_per_core),
        ("endpoint_memory_mb", endpoint_memory_mb),
        ("max_threads_per_task", max_threads_per_task),
    ):
        if value < 1:
            raise ValueError(f"{name} must be at least 1. Got {value}.")

    if endpoint_scope not in _ENDPOINT_SCOPES:
        raise ValueError(
            f"endpoint_scope must be one of {_ENDPOINT_SCOPES}. Got {endpoint_scope!r}."
        )

    spark: SparkSession = table.sparkSession

    # Size the job from the cluster attached to the session; re-detected on every call.
    resources: _ClusterResources = _detect_cluster_resources(spark)
    threads_per_task = _auto_threads_per_task(
        resources,
        endpoint_scope=endpoint_scope,
        endpoint_memory_mb=endpoint_memory_mb,
        max_threads_per_task=max_threads_per_task,
    )
    # One task per core; oversubscribe partitions so stragglers do not idle other cores.
    # Target the largest cluster size so nodes added by autoscaling also receive work.
    num_partitions: int = resources.max_total_cores * partitions_per_core

    # Project first: only the scoring payload is shuffled and serialized to workers.
    payload = table.select(index_col, *input_cols)
    passthrough_cols = [index_col]
    payload = payload.repartition(num_partitions)
    output_names: list[str] = [_key_alias(name) for name in passthrough_cols]

    spark.conf.set(_ARROW_BATCH_CONF, str(max_records_per_batch))
    logger.info(
        f"executors={resources.executors}, cores_per_executor={resources.cores_per_executor}, "
        f"total_cores={resources.total_cores} (max {resources.max_total_cores}), "
        f"python_mb_per_task={resources.python_mb_per_task}, threads_per_task={threads_per_task} "
        f"(max concurrent predict calls={resources.total_cores * threads_per_task}), "
        f"num_partitions={num_partitions}, endpoint_scope={endpoint_scope}, "
    )

    # Carry the payload key types through instead of assuming strings.
    output_schema: StructType = _build_output_schema(
        [
            StructField(alias, payload.schema[name].dataType, nullable=True)
            for name, alias in zip(passthrough_cols, output_names)
        ]
    )
    endpoint_config: tuple[str, str, str] = (cie_name, product_name, pss_version)

    def _score_batches(batches: Iterable[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        for batch in batches:
            if batch.empty:
                continue
            yield _score_batch(
                batch,
                endpoint_config=endpoint_config,
                input_cols=input_cols,
                predict_kwargs=predict_kwargs,
                passthrough_cols=passthrough_cols,
                output_names=output_names,
                threads_per_task=threads_per_task,
                endpoint_scope=endpoint_scope,
            )

    scored: DataFrame = payload.mapInPandas(_score_batches, schema=output_schema)
    pss_cols = [F.col(name) for name in _PSS_SCHEMA]

    return scored.select(F.col(_key_alias(index_col)).alias(index_col), *pss_cols)


def _score_batch(
    batch: pd.DataFrame,
    *,
    endpoint_config: tuple[str, str, str],
    input_cols: list[str],
    predict_kwargs: dict[str, str],
    passthrough_cols: list[str],
    output_names: list[str],
    threads_per_task: int,
    endpoint_scope: str,
) -> pd.DataFrame:
    """Score one Arrow batch with concurrent predict() calls and shape it to the output schema."""
    # One vectorized rename and cast, so the row loop below is a bare predict() call.
    frame: pd.DataFrame = batch[input_cols].rename(columns=predict_kwargs)
    frame = frame.astype({name: "float64" for name in _FLOAT_PREDICT_KWARGS})
    records: list[dict] = frame.to_dict(orient="records")

    predict = _make_predict(endpoint_config, endpoint_scope)
    if threads_per_task == 1 or len(records) == 1:
        scores = [predict(record) for record in records]
    else:
        # I/O-bound raster/shapefile reads release the GIL, so threads overlap the waits.
        # Executor.map preserves input order, keeping scores aligned with the batch rows.
        scores = list(_get_executor(threads_per_task).map(predict, records))

    output: pd.DataFrame = (
        pd.json_normalize(data=scores, sep="_", max_level=1, errors="ignore")
        .reindex(columns=list(_PSS_SCHEMA))
        .replace("", pd.NA)
        .astype(_PSS_SCHEMA, errors="raise")
    )
    for position, (source, alias) in enumerate(zip(passthrough_cols, output_names)):
        output.insert(position, alias, batch[source].to_numpy())
    return output


def _make_predict(endpoint_config: tuple[str, str, str], endpoint_scope: str):
    """Return a one-record predict callable bound to the requested endpoint scope."""
    if endpoint_scope == "worker":
        endpoint = _resolve_pss_service(*endpoint_config)
        return lambda record: endpoint.predict(**record)

    def _predict(record: dict) -> dict:
        return _thread_endpoint(endpoint_config).predict(**record)

    return _predict


def _thread_endpoint(endpoint_config: tuple[str, str, str]) -> Any:
    """Build (once) and return the endpoint owned by the calling thread."""
    if getattr(_THREAD_STATE, "config", None) != endpoint_config:
        _THREAD_STATE.endpoint = _build_pss_service(*endpoint_config)
        _THREAD_STATE.config = endpoint_config
    return _THREAD_STATE.endpoint


def _get_executor(size: int) -> ThreadPoolExecutor:
    """Return the Python worker's persistent thread pool, resized if needed."""
    global _EXECUTOR, _EXECUTOR_SIZE  # noqa: PLW0603
    with _EXECUTOR_LOCK:
        if _EXECUTOR is None or size != _EXECUTOR_SIZE:
            if _EXECUTOR is not None:
                _EXECUTOR.shutdown(wait=True)
            _EXECUTOR = ThreadPoolExecutor(max_workers=size, thread_name_prefix="pss")
            _EXECUTOR_SIZE = size
    return _EXECUTOR


def _key_alias(name: str) -> str:
    """Name used for passthrough key columns in the mapInPandas output."""
    return f"__pss_key_{name}"


@dataclass(frozen=True)
class _ClusterResources:
    """Compute resources of the cluster attached to the Spark session."""

    executors: int
    cores_per_executor: int
    total_cores: int
    max_total_cores: int
    python_mb_per_task: int | None


_MAX_WORKERS_CONF = "spark.databricks.clusterUsageTags.clusterMaxWorkers"
_MEMORY_UNITS_MB = {"k": 1 / 1024, "m": 1, "g": 1024, "t": 1024 * 1024}


def _detect_cluster_resources(spark: SparkSession) -> _ClusterResources:
    """Detect executors, cores and Python memory per task on the attached cluster."""
    try:
        sc = spark.sparkContext
        total_cores = max(1, int(sc.defaultParallelism))
        # Block-manager entries include the driver; zero executors means single-node mode.
        executors = max(0, sc._jsc.sc().getExecutorMemoryStatus().size() - 1)
    except Exception:  # noqa: BLE001 - Spark Connect / serverless expose no SparkContext.
        cores = os.cpu_count() or 8
        return _ClusterResources(1, cores, cores, cores, None)

    local_mode = executors == 0
    executor_count = max(1, executors)
    cores_per_executor = max(1, total_cores // executor_count)

    # Autoscaling clusters: size partitions for the maximum worker count (best effort).
    max_workers = _conf_int(spark, _MAX_WORKERS_CONF)
    max_total_cores = total_cores
    if max_workers and not local_mode:
        max_total_cores = max(total_cores, cores_per_executor * max_workers)

    # Python workers live outside the JVM heap: node memory - JVM heap - OS reserve.
    jvm_conf = "spark.driver.memory" if local_mode else "spark.executor.memory"
    jvm_mb = _parse_memory_mb(spark.conf.get(jvm_conf, None))
    node_mb = _probe_node_memory_mb(spark, executor_count, local_mode)
    python_mb_per_task: int | None = None
    if node_mb and jvm_mb:
        reserve_mb = max(1024, node_mb // 10)
        python_mb_per_task = max(0, node_mb - jvm_mb - reserve_mb) // cores_per_executor

    return _ClusterResources(
        executors=executor_count,
        cores_per_executor=cores_per_executor,
        total_cores=total_cores,
        max_total_cores=max_total_cores,
        python_mb_per_task=python_mb_per_task,
    )


def _auto_threads_per_task(
    resources: _ClusterResources,
    *,
    endpoint_scope: str,
    endpoint_memory_mb: int,
    max_threads_per_task: int,
) -> int:
    """Largest thread count per task that fits the Python memory budget."""
    budget_mb = resources.python_mb_per_task
    if budget_mb is None:
        # Memory unknown (e.g. Spark Connect): stay conservative.
        return min(4, max_threads_per_task)
    if endpoint_scope == "worker":
        # One endpoint shared by all threads, so threads are not bounded by endpoint memory.
        return max_threads_per_task if budget_mb >= endpoint_memory_mb else 1
    return max(1, min(max_threads_per_task, budget_mb // endpoint_memory_mb))


def _probe_node_memory_mb(spark: SparkSession, executors: int, local_mode: bool) -> int | None:
    """Smallest physical memory (MB) across worker nodes, read from /proc/meminfo."""
    if local_mode:
        return _read_mem_total_mb()
    try:
        sizes = (
            spark.sparkContext.parallelize(range(executors), executors)
            .map(lambda _: _read_mem_total_mb())
            .collect()
        )
    except Exception:  # noqa: BLE001 - probing is best effort.
        return None
    sizes = [size for size in sizes if size]
    return min(sizes) if sizes else None


def _read_mem_total_mb() -> int | None:
    try:
        with open("/proc/meminfo") as handle:
            for line in handle:
                if line.startswith("MemTotal:"):
                    return int(line.split()[1]) // 1024
    except (OSError, ValueError):
        return None
    return None


def _parse_memory_mb(value: str | None) -> int | None:
    """Parse a Spark memory string (e.g. ``21588m``, ``4g``, bytes) into MB."""
    if not value:
        return None
    text = str(value).strip().lower().removesuffix("b")
    try:
        if text and text[-1] in _MEMORY_UNITS_MB:
            return int(float(text[:-1]) * _MEMORY_UNITS_MB[text[-1]])
        return int(text) // (1024 * 1024)
    except ValueError:
        return None


def _conf_int(spark: SparkSession, key: str) -> int | None:
    try:
        value = spark.conf.get(key, None)
        return int(value) if value is not None else None
    except Exception:  # noqa: BLE001 - missing or non-numeric conf.
        return None


def _build_output_schema(passthrough_fields: list[StructField]) -> StructType:
    fields: list[StructField] = list(passthrough_fields)
    # Add the remaining fields based on the PSS schema.
    fields += [
        StructField(name, _PANDAS_TO_SPARK_TYPE[dtype], nullable=True)
        for name, dtype in _PSS_SCHEMA.items()
    ]
    return StructType(fields)


def _normalize_and_validate(cie: str, product: str, version: str) -> tuple[str, str, str]:
    """Validate company/product and build the PSS endpoint name without loading assets."""
    cie_name = cie.upper()
    cie_name = "CGIC" if cie_name == "CUMIS" else cie_name
    cie_name = "SGIC" if cie_name == "SOV" else cie_name
    product_name = product.upper()
    product_name = "HOME" if product_name == "FARM" else product_name
    product_name = "COMM" if product_name in ("COMMERCIAL", "SPEC") else product_name
    if isinstance(version, (float, int)) or (isinstance(version, str) & ("v" not in version)):
        version = f"v{version}"
    pss_version = str(version).lower().replace(".", "_")

    if cie_name not in ("CGIC", "SGIC"):
        raise ValueError(f"Cie must be one of 'CGIC', 'SGIC' or 'CUMIS'. Got {cie}.")
    if product_name not in ("HOME", "COMM"):
        raise ValueError(f"Product must be one of 'HOME', 'FARM', 'COMM' or 'SPEC'. Got {product}.")

    # Import PSS version module dynamically based on the company, product, and version.
    pss_version_mapper_name = f"PSS_VERSIONS_MAP_{cie_name}_{product_name}"
    pss_versions_module = __import__(
        "cgl_dbx_flood_pipeline.config.pss_versions", fromlist=[pss_version_mapper_name]
    )
    pss_versions: dict[str, PSSVersionsMapping] = getattr(
        pss_versions_module, pss_version_mapper_name, {}
    )

    # Make sure the requested PSS version is available for the given company and product.
    available_versions = pss_versions.keys()
    if pss_version not in available_versions:
        raise ValueError(
            f"Unsupported PSS version: {pss_version}. Available versions are: {list(available_versions)}."
        )

    return product_name, cie_name, pss_version


def _build_pss_service(cie_name: str, product_name: str, pss_endpoint_version: str) -> Any:
    """Build a new asset-heavy PSS service (uncached)."""

    module_root = "cgl_dbx_flood_pipeline.geomap_scorer"

    if cie_name == "CGIC" and product_name == "HOME":
        init_pss_service = _try_import_pss_service(
            f"{module_root}.create_pss_endpoint_cgic_home", "init_cgic_home_farm_pss_service"
        )

    # TODO: Implement other PSS service resolutions for different cie and product combinations.
    else:
        raise ValueError(
            f"Endpoint not implemented yet for cie '{cie_name}' and product '{product_name}'."
        )

    return init_pss_service(endpoint_version=pss_endpoint_version)


# One shared service per Python worker, keyed by endpoint configuration (endpoint_scope="worker").
_resolve_pss_service = lru_cache(maxsize=1)(_build_pss_service)


def _try_import_pss_service(module_name: str, function_name: str) -> Any:
    try:
        module: ModuleType = __import__(module_name, fromlist=[function_name])
        return getattr(module, function_name)
    except ImportError as exc:
        raise ImportError(f"Failed to import {function_name} from {module_name}.") from exc
