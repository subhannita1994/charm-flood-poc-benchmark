"""Utility functions to access log information."""

from __future__ import annotations

from collections.abc import Sequence


def get_items(
    dictionary: dict[str, list],
    keys: Sequence[str] | str | None = None,
) -> dict[str, list] | list:
    """From a dictionary of removed/filtered item_IDs, return the items affected
    by a specific filtering operation (key).

    Parameters
    ----------
    dictionary : dict[str, list]
        Dictionary of removed/filtered item_IDs.
    keys : list[str] | str | None
        Name of the filtering operation(s) to retrieve. If None, return all filtered items.

    Returns
    -------
    dict[str, list] | list
        Dictionary containing the filtered item_IDs for the specified filtering operation(s).
        If keys is None, return the entire dictionary.
        If keys is a single string, return the list of filtered item_IDs for that single operation.
    """
    if keys is None:
        return dictionary
    if isinstance(keys, str):
        return dictionary.get(keys, [])
    return {key: dictionary.get(key, []) for key in keys}
