"""The small model contract used by worker-side API execution."""

from typing import Protocol

from pyspark.sql import DataFrame

from ..sources.source_profile import BusinessValidation

class ModelAPI(Protocol):
    """Declare model inputs, typed response fields, and worker-side row scoring."""

    input_schema: dict[str, str]
    pandas_schema: dict[str, str]

    def validate_inputs(
        self, table: DataFrame
    ) -> BusinessValidation:
        """Validate standardized fields against this model's business rules."""

    def worker_count(self) -> int:
        """Return the maximum concurrent requests per Spark partition."""

    def score_row(
        self, values: tuple[object, ...], effective_date: str
    ) -> dict[str, object]:
        """Score input values in input_schema order."""


def model_input_schema(model_api: ModelAPI, index_col: str) -> dict[str, str]:
    """Build the prepared exposure contract from model types and shared fields."""
    schema = model_api.input_schema
    if not schema:
        raise ValueError("Model API input_schema must be nonempty.")
    for name in ("longitude", "latitude"):
        if schema.get(name) != "double":
            raise ValueError(f"Model API input_schema must contain {name!r} with type 'double'.")
    conflicts = {index_col, "province"} & schema.keys()
    if conflicts:
        raise ValueError(f"Model API input_schema contains reserved pipeline fields: {sorted(conflicts)}")
    return {index_col: "string", **schema}
