"""Configuration for Unity Catalog paths used in the flood scoring pipeline, in Databricks."""

import os
from dataclasses import dataclass


@dataclass
class PipelineInputPaths:
    fluvial_regions: str
    coastal_regions: str
    fluvial_simulation: str
    coastal_simulation: str
    province_shp_path: str
    outputs_path: str
    fluvial_events: str = ""  # CHARM-POC RECONSTRUCTED (A9)
    coastal_events: str = ""


# CHARM-POC PATCH: root overridable so the same code runs against the PoC volume
unity_catalog_repo: str = os.environ.get("CHARM_DATA_ROOT", "/Volumes/cgic_mlai_charm_exp/mlai_charm_exp/")
PATHS = PipelineInputPaths(
    fluvial_regions=unity_catalog_repo + "flood_watersheds/can_flv_regions_v02.shp",
    coastal_regions=unity_catalog_repo + "flood_watersheds/can_cst_regions_v04.shp",
    fluvial_simulation=unity_catalog_repo + "simulations2/fluvial_simulation_events",
    coastal_simulation=unity_catalog_repo + "simulations2/coastal_simulation_events",
    province_shp_path=unity_catalog_repo
    + "ext_bi_ml_floodgeomap/flood_model_serving/maps/provinces/public/lpr_000a21a_e/lpr_000a21a_e.shp",
    outputs_path=unity_catalog_repo + "outputs/"
)
