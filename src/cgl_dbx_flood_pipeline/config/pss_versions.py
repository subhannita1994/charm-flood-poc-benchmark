"""CHARM-POC RECONSTRUCTED (A9): referenced by score_pss.py / create_pss_endpoint_cgic_home.py, absent from the upload.

Field names inferred from create_pss_endpoint_cgic_home.py; release dates are assumptions.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class PSSVersionsMapping:
    release_date: str
    generic_pss_assets_version: str
    product_pss_assets_version: str
    product_dc_version: str
    cie_flex_map_version: str


PSS_VERSIONS_MAP_CGIC_HOME: dict[str, PSSVersionsMapping] = {
    "v1": PSSVersionsMapping("2025-01-01", "v1", "v1", "v1", "v1"),
    "v1_1": PSSVersionsMapping("2026-06-01", "v1", "v1", "v1", "v1_1"),
}
