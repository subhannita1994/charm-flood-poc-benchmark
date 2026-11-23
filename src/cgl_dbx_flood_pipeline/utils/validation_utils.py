"""Validation helpers."""

from collections.abc import Iterable
from typing import Any

from loguru import logger


def validate_required_columns(table: Any, columns: Iterable[str]) -> None:
    """Validate that all required columns are present in a DataFrame.

    Parameters
    ----------
    table : Any
        Input table exposing a ``.columns`` attribute.
    columns : Iterable[str]
        Required column names.

    Raises
    ------
    KeyError
        If one or more required columns are missing.
    """
    table_columns = set(table.columns)
    missing = [column for column in columns if column not in table_columns]
    if missing:
        raise KeyError(f"Missing required columns: {missing}")


def warn_if_rows_removed(
    count: int,
    after_count: int | None = None,
    message: str = "Rows removed: {removed}"
    ) -> None:
    """Print a warning message when rows are removed.

    Parameters
    ----------
    count : int
        If `after_count` is provided, the initial row count before the step.
        If `after_count` is None, the total number of removed rows.
    after_count : int | None
        Row count after the step. If None, count represents the total number of removed rows.
    message : str
        Warning message template. Must include ``{removed}`` placeholder.
        Default is "Rows removed: {removed}".
    """
    if after_count is None:
        removed = count
    elif isinstance(count, int) and isinstance(after_count, int):
        removed = count - after_count
    else:
        raise TypeError("Both 'count' and 'after_count' must be integers or 'after_count' can be None.")
    if removed > 0:
        logger.warning(message.format(removed=removed))


def warn_imputation(
    nb_of_imputed: int,
    message: str="Number of imputed items: {nb_of_imputed}"
    ) -> None:
    """Print a warning message when values are imputed.

    Parameters
    ----------
    nb_of_imputed : int
        Number of values imputed.
    message : str
        Warning message template. Must include ``{nb_of_imputed}`` placeholder.
        Default is "Number of imputed items: {nb_of_imputed}".
    """
    if nb_of_imputed > 0:
        logger.warning(message.format(nb_of_imputed=nb_of_imputed))
