"""Bounded worker-side execution for an explicitly selected model adapter."""

from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from .model_api import ModelAPI

_PANDAS_TO_SPARK_TYPE = {
    "Float64": DoubleType(),
    "Int64": IntegerType(),
    "string": StringType(),
}


def score_with_model_api(
    table: DataFrame,
    *,
    index_col: str,
    effective_date: str,
    model_api: ModelAPI,
) -> DataFrame:
    """Score declared fields only and return the model's declared output schema."""
    max_workers = model_api.worker_count()
    if max_workers < 1:
        raise ValueError("Model API worker count must be a positive integer.")
    pandas_schema = model_api.pandas_schema
    input_columns = tuple(model_api.input_schema)
    call_row = model_api.score_row
    output_schema = StructType(
        [StructField(index_col, StringType(), nullable=False)]
        + [
            StructField(name, _PANDAS_TO_SPARK_TYPE[pandas_type], nullable=True)
            for name, pandas_type in pandas_schema.items()
        ]
    )

    def score_partition(batches):
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for batch in batches:
                if batch.empty:
                    continue
                row_values = batch[list(input_columns)].itertuples(index=False, name=None)
                records = list(
                    executor.map(lambda values: call_row(values, effective_date), row_values)
                )
                output = pd.DataFrame.from_records(records, columns=list(pandas_schema))
                output = output.astype(pandas_schema, errors="raise")
                output.insert(0, index_col, batch[index_col].astype("string").to_numpy())
                yield output

    return table.select(index_col, *input_columns).mapInPandas(
        score_partition, schema=output_schema
    )
