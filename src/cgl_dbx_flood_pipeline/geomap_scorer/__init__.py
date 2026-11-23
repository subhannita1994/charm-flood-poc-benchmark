"""Public exports for Geomap scorer implementations."""

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .geomap_rules_cgic_home import GeomapScorerCGICHomeRules
    from .geomap_scorer import GeomapScorer, GeomapScorerRules

_EXPORT_MODULES = {
    "GeomapScorer": ".geomap_scorer",
    "GeomapScorerCGICHomeRules": ".geomap_rules_cgic_home",
    "GeomapScorerRules": ".geomap_scorer",
}

__all__ = ["GeomapScorer", "GeomapScorerCGICHomeRules", "GeomapScorerRules"]


def __getattr__(name: str) -> Any:
    """Load scorer classes without importing model services for helper modules."""
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module_name, __name__), name)
    globals()[name] = value
    return value
