"""Small contracts for source-specific preparation and business validation."""

from collections.abc import Mapping
from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import Protocol

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from ..utils.spark_utils import _map_column
from ..utils.validation_utils import validate_required_columns

class MappedSourceProfile(ABC):
    """Share column selection and renaming for sources with column mappings."""

    @property
    @abstractmethod
    def colnames_mapping(self) -> dict[str, str]:
        """Map original column names to standardized names."""

    @property
    @abstractmethod
    def category_mappings(self) -> dict[str, dict[str, str]]:
        """Map source category values to standardized values by field."""

    def standardize(
        self, table: DataFrame, schema: Mapping[str, str]
    ) -> DataFrame:
        """Select and rename source fields in model schema order."""
        source_columns = {
            target: source
            for source, target in self.colnames_mapping.items()
            if target in schema
        }
        missing_mappings = set(schema) - source_columns.keys()
        if missing_mappings:
            raise ValueError(f"Missing source-column mappings: {sorted(missing_mappings)}")
        validate_required_columns(table, source_columns.values())
        return table.select(
            *[F.col(source_columns[column]).alias(column) for column in schema]
        )

    def map_categories(self, table: DataFrame) -> DataFrame:
        """Translate source categories; unknown values become null for validation."""
        mappings = self.category_mappings
        validate_required_columns(table, mappings.keys())
        return table.select(
            *[
                _map_column(F.col(name), mappings[name]).alias(name)
                if name in mappings else F.col(name)
                for name in table.columns
            ]
        )


@dataclass
class BusinessValidation:
    """Validated rows and rejected rows for one source-specific business rule."""

    table: DataFrame
    rejected: DataFrame
    reason: str = "invalid_map_codes"
    detail: str = "invalid_map_codes"
    warning: str = "Removed {removed} items with invalid source attributes."


class SourceProfile(Protocol):
    """Describe standardized source fields; coordinates use longitude/latitude."""

    @property
    def colnames_mapping(self) -> dict[str, str]:
        """Map source column names to standardized column names."""
        ...

    @property
    def category_mappings(self) -> dict[str, dict[str, str]]:
        """Return source-code mappings keyed by standardized field name."""
        ...

    def standardize(
        self, table: DataFrame, schema: Mapping[str, str]
    ) -> DataFrame:
        """Select and rename raw fields to the supplied model schema."""
        ...

    def map_categories(self, table: DataFrame) -> DataFrame:
        """Translate source category values into standardized values."""
        ...

