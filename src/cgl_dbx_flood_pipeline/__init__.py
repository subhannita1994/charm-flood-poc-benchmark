"""Public package exports for the flood scoring pipeline."""

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .flood_pipeline import FloodPipeline
    from .geomap_scorer import GeomapScorer, GeomapScorerCGICHomeRules, GeomapScorerRules
    from .flood_simulation_merge import LossPipeline
    from .preprocessor import (
        FloodInForceHomeCGICRules,
        FloodInForcePreprocessor,
        PreprocessorRules,
        score_province,
    )
    from .run_pipeline import OutputKeys, run_flood_pipeline

_EXPORT_MODULES = {
    "FloodInForceHomeCGICRules": ".preprocessor",
    "FloodInForcePreprocessor": ".preprocessor",
    "FloodPipeline": ".flood_pipeline",
    "GeomapScorer": ".geomap_scorer",
    "GeomapScorerCGICHomeRules": ".geomap_scorer",
    "GeomapScorerRules": ".geomap_scorer",
    "LossPipeline": ".loss_pipeline",
    "OutputKeys": ".run_pipeline",
    "PreprocessorRules": ".preprocessor",
    "run_flood_pipeline": ".run_pipeline",
    "score_province": ".preprocessor",
}

__all__ = [
    "FloodInForceHomeCGICRules",
    "FloodInForcePreprocessor",
    "FloodPipeline",
    "GeomapScorer",
    "GeomapScorerCGICHomeRules",
    "GeomapScorerRules",
    "LossPipeline",
    "OutputKeys",
    "PreprocessorRules",
    "run_flood_pipeline",
    "score_province",
]


def __getattr__(name: str) -> Any:
    """Load public exports only when callers request them."""
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module_name, __name__), name)
    globals()[name] = value
    return value
