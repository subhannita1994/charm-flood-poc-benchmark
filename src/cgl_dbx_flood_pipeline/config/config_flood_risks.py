"""CHARM-POC RECONSTRUCTED (A9): referenced by geomap_scorer.py, absent from the upload.

Shape inferred from usage: RISK_CONFIGS[risk].region_column / .progress_description, risk in {"flv", "cst"}.
"""
from dataclasses import dataclass
from typing import Literal

RiskType = Literal["flv", "cst"]


@dataclass(frozen=True)
class FloodRiskConfig:
    region_column: str
    progress_description: str


RISK_CONFIGS: dict[str, FloodRiskConfig] = {
    "flv": FloodRiskConfig(region_column="fluvial_region", progress_description="Scoring fluvial"),
    "cst": FloodRiskConfig(region_column="coastal_region", progress_description="Scoring coastal"),
}
