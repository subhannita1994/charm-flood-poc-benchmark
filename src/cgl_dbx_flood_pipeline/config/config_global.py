"""Configuration constants for all components."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

InterpolationMethod = Literal["linear", "constant"]

@dataclass(frozen=True)
class GlobalConfig:
    """Container for global configuration constants."""
    crs: str
    province_crs:str
    interpolation_method: InterpolationMethod
    province_col: str
    index_col: str
    geometry_col: str
    latitude_col: str
    longitude_col: str
    canada_bbox: dict[str, float]
    seasons: tuple[str, ...]
    return_periods: tuple[int, ...]
    required_return_periods_for_metrics: tuple[int, ...]
    province_abbr_replacements: dict[str, str]
    province_centroids: list[tuple[str, float, float]]
    centroid_tolerance: float
    schema: dict  # CHARM-POC RECONSTRUCTED (A9)

GLOBAL_CONFIG = GlobalConfig(
    crs="EPSG:4326",
    province_crs="EPSG:3347",
    interpolation_method="constant",
    province_col="province",
    index_col="item_id",
    geometry_col="geometry",
    latitude_col="latitude",
    longitude_col="longitude",
    canada_bbox={"lat_min": 41., "lat_max": 84., "lon_min": -141., "lon_max": -52.},
    seasons=("spring", "summer", "fall"),
    return_periods=(20, 50, 75, 100, 200, 500, 1500),
    required_return_periods_for_metrics=(5, 10, 200, 500, 1000),
    province_abbr_replacements={
        "Que": "QC",
        "Ont": "ON",
        "Man": "MB",
        "Sask": "SK",
        "Alta": "AB",
        "NWT": "NT",
        "Nvt": "NU",
        "PEI": "PE",
    },
    province_centroids=[
        ("ON", 50.08990811, -85.83358805),
        ("QC", 53.01757475, -70.92960413),
        ("QC", 53.00482000, -70.77841000),
        ("NL", 52.64990765, -60.24440640),
        ("AB", 55.16720057, -114.51205581),
        ("SK", 54.41643638, -105.89182305),
        ("BC", 54.56061105, -125.10931540),
    ],
    centroid_tolerance=0.04,
    schema={"item_id": str, "latitude": float, "longitude": float, "province": str,
            "risk_type": str, "dwelling_style": str, "foundation_type": str},  # CHARM-POC RECONSTRUCTED (A9)
)
