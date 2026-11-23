"""CGIC Home-specific geomap rules."""

from cgl_dbx_flood_pipeline.config.pss_versions import PSS_VERSIONS_MAP_CGIC_HOME

from ..utils.validation_utils import validate_required_columns
from .geomap_scorer import GeomapScorer
from .map_date_to_pss_version import map_date_to_pss_version


class GeomapScorerCGICHomeRules:
    """Product rules for CGIC Home geomap scoring."""

    DROP_COLUMNS = ("risk_type", "dwelling_style", "foundation_type")

    def __init__(self):
        """Initialize the CGIC Home geomap rules."""
        self.extra_required_columns = ["risk_type", "dwelling_style", "foundation_type"]
        self.cie = "CGIC"
        self.product = "home"

    def required_columns(self, base_required_columns: list[str]) -> list[str]:
        """Extend base geomap inputs with CGIC Home-specific fields."""
        return list(dict.fromkeys(base_required_columns + self.extra_required_columns))

    def select_required_columns(self, ctx: GeomapScorer) -> None:
        """Select only the columns required for CGIC Home scoring."""
        required_columns = list(ctx.required_columns)
        validate_required_columns(ctx.table, required_columns)
        ctx.table = ctx.table.select(*required_columns)

    def drop_intermediary_columns(
        self, ctx: GeomapScorer, drop_intermediary_columns: bool = True
    ) -> None:
        """Drop columns that are only needed during CGIC Home geomap scoring."""
        if not drop_intermediary_columns:
            return
        if ctx.table is not None:
            ctx.table = ctx.table.drop(*self.DROP_COLUMNS)

    def map_pss_version(self, date: str) -> str:
        """Map the given date to the appropriate PSS version for CGIC Home."""
        return map_date_to_pss_version(date, PSS_VERSIONS_MAP_CGIC_HOME)
