"""CGIC Home source definitions and configurable category translations."""

from dataclasses import dataclass

from .source_profile import MappedSourceProfile


@dataclass(frozen=True)
class HomeCGICConfig:
    """Source mappings, including legacy basement and storey derivations."""

    colnames_mapping: dict[str, str]
    dwellingstyle_mapping: dict[str, str]
    risktype_mapping: dict[str, str]
    foundationtype_mapping: dict[str, str]
    hasbasement_mapping: dict[str, bool]
    storeys_mapping: dict[str, float]
    # CHARM-POC RECONSTRUCTED (A9): fields referenced by preprocessor_rules_cgic_home.py but absent upstream
    schema: dict = None
    default_deductible: float = 2000.0
    possible_deductibles: tuple = (500.0, 1000.0, 1500.0, 2000.0, 2500.0, 5000.0, 10000.0)
    max_deductible: float = 10000.0

HOME_CGIC_CONFIG = HomeCGICConfig(
    colnames_mapping={
        "item_id_m": "item_id",  # CHARM-POC PATCH: upstream had undefined name GLOBAL
        "longitudeinternal": "longitude",
        "latitudeinternal": "latitude",
        "riskoption_name": "risk_type",
        "dwellingstyle_name": "dwelling_style",
        "foundationtype_name": "foundation_type",
    },
    dwellingstyle_mapping={
        "1 storey": "1storey",
        "1.5 storey": "1.5storey",
        "2 storey": "2storey",
        "2.5 storey": "2.5storey",
        "3 storey": "3storey",
        "Bi-level": "Bilevel",
        "Tri-level": "Trilevel",
        "Bungalow": "Bungalow",
        "Single-Wide": "SingleWide",
        "Double-Wide": "DoubleWide",
        "Backsplit": "Backsplit",
        "Sidesplit": "Sidesplit",
    },
    risktype_mapping={
        "Homeowner": "HOM",
        "Residential": "RES",
        "Tenant": "TEN",
        "Condominium": "CON",
        "Seasonal": "SEA",
        "Seasonal high value": "SHV",
        "Mobile Homeowner": "MOB",
        "Mobile Residential": "MRE",
    },
    foundationtype_mapping={
        "Concrete": "Concrete",
        "Concrete blocks": "ConcreteBlocks",
        "Continuous reinforced concrete foundation walls and footing": "ContinuousReinforced",
        "Crawl space": "CrawlSpace",
        "Filled concrete blocks": "FilledConcreteBlock",
        "Full Basement": "FullBasement",
        "Fully reinforced concrete or preserved wood basement": "FullyReinforced",
        "Other": "Other",
        "Piers": "Piers",
        "Pier and Beam": "PierAndBeam",
        "Preserved wood basement": "PresWoodBase",
        "Preserved wood blocks": "PresWoodBlocks",
        "Preserved wood cribbing": "PresWoodCribbing",
        "Railway ties": "RailwayTies",
        "Raised Slab": "RaisedSlab",
        "Reinforced concrete or steel pilings/piers": "ReinforcedConcreteSteel",
        "Slab": "Slab",
        "Steel piers or pilings": "SteelPiersPilings",
        "Stone": "Stone",
        "Wood": "Wood",
    },
    hasbasement_mapping={
        "Concrete": True,
        "ConcreteBlocks": True,
        "ContinuousReinforced": True,
        "CrawlSpace": False,
        "FilledConcreteBlock": True,
        "FullBasement": True,
        "FullyReinforced": True,
        "Other": False,
        "Piers": False,
        "PierAndBeam": False,
        "PresWoodBase": False,
        "PresWoodBlocks": False,
        "PresWoodCribbing": False,
        "RailwayTies": False,
        "RaisedSlab": False,
        "ReinforcedConcreteSteel": False,
        "Slab": False,
        "SteelPiersPilings": False,
        "Stone": True,
        "Wood": False,
        "": True,
    },
    storeys_mapping={
        "1storey": 1.,
        "1.5storey": 1.5,
        "2storey": 2.,
        "2.5storey": 2.,
        "3storey": 2.,
        "Bilevel": 2.,
        "Trilevel": 2.,
        "Bungalow": 1.,
        "SingleWide": 1.,
        "DoubleWide": 1.,
        "Backsplit": 2.,
        "Sidesplit": 2.,
        "": 1.,
    },
    schema={  # CHARM-POC RECONSTRUCTED (A9): columns the preprocessor/geomap/loss stages read
        "item_id": str, "longitude": float, "latitude": float,
        "risk_type": str, "dwelling_style": str, "foundation_type": str,
        "company": str, "product": str, "flood_coverage": int, "flood_deductible": float,
        "floor": int, "building_value": float, "content_value": float, "flood_limit": float,
    },
)


@dataclass(frozen=True)
class CGICHomeSourceProfile(MappedSourceProfile):
    """Translate CGIC Home source fields into standardized scoring inputs."""

    config: HomeCGICConfig = HOME_CGIC_CONFIG

    @property
    def colnames_mapping(self) -> dict[str, str]:
        return self.config.colnames_mapping


    @property
    def category_mappings(self) -> dict[str, dict[str, str]]:
        return {
            "risk_type": self.config.risktype_mapping,
            "dwelling_style": self.config.dwellingstyle_mapping,
            "foundation_type": self.config.foundationtype_mapping,
        }
