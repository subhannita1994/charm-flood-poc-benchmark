"""A function to map a given date to the corresponding PSS version."""

from datetime import datetime

from cgl_dbx_flood_pipeline.config.pss_versions import PSSVersionsMapping


def map_date_to_pss_version(
    date: str | datetime, pss_mapping: dict[str, PSSVersionsMapping]
) -> str:
    """
    Map a given date to the corresponding PSS version.

    Parameters
    ----------
    date : str | datetime
        The date to map. Can be a string in the format "YYYY-MM-DD" or a datetime object.
    pss_mapping : dict[str, PSSVersionsMapping]
        A dictionary mapping PSS version strings to their corresponding PSSVersionsMapping objects.

    Returns
    -------
    str
        The PSS version string corresponding to the given date.

    Raises
    ------
    ValueError
        If no PSS version is found for the given date.
    """
    if isinstance(date, str):
        date = datetime.strptime(date, "%Y-%m-%d")

    for version, mapping in sorted(
        pss_mapping.items(), key=lambda item: item[1].release_date, reverse=True
    ):
        if date >= datetime.strptime(mapping.release_date, "%Y-%m-%d"):
            return version
    return "v1"


if __name__ == "__main__":
    from cgl_dbx_flood_pipeline.config.pss_versions import PSS_VERSIONS_MAP_CGIC_HOME

    print(
        map_date_to_pss_version(
            datetime.strptime("2026-05-30", "%Y-%m-%d"), PSS_VERSIONS_MAP_CGIC_HOME
        )
    )
    print(
        map_date_to_pss_version(
            datetime.strptime("2026-06-01", "%Y-%m-%d"), PSS_VERSIONS_MAP_CGIC_HOME
        )
    )
    print(
        map_date_to_pss_version(
            datetime.strptime("2026-06-02", "%Y-%m-%d"), PSS_VERSIONS_MAP_CGIC_HOME
        )
    )
    print(
        map_date_to_pss_version(
            datetime.strptime("2026-10-01", "%Y-%m-%d"), PSS_VERSIONS_MAP_CGIC_HOME
        )
    )
