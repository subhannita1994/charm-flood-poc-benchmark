"""Damage-curve table paths (stand-in), with import-time existence checks like upstream."""
import os

from flood_model_serving import ASSETS_ROOT
from flood_model_serving._paths_common import check_exists

_DC = os.path.join(ASSETS_ROOT, "damage_curves")
PATH_HOME_FARM_DC = os.path.join(_DC, "home_farm_dc.csv")
PATH_HOME_FARM_DC_MAPPING = os.path.join(_DC, "home_farm_dc_mapping.csv")
PATH_HOME_FARM_BUILDING_STYLE = os.path.join(_DC, "building_style.csv")
PATH_HOME_FARM_FOUNDATION_TYPE = os.path.join(_DC, "foundation_type.csv")
PATH_HOME_FARM_RISK_TYPE = os.path.join(_DC, "risk_type.csv")
IMPORT_CHECK_SECONDS = check_exists([PATH_HOME_FARM_DC, PATH_HOME_FARM_DC_MAPPING, PATH_HOME_FARM_BUILDING_STYLE,
                                     PATH_HOME_FARM_FOUNDATION_TYPE, PATH_HOME_FARM_RISK_TYPE])
