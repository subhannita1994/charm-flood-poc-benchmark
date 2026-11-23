"""
Peril Score Service (PSS) for Geomap Scoring.

This function calls modules from the GeomapAPI
https://github.com/Cooperators-BI/Flood-Model-Serving/tree/main/libs/flood_serving_project

To install the package and import its objects, run the following commands in the terminal:
poetry add git+https://github.com/Cooperators-BI/Flood-Model-Serving.git@develop#subdirectory=libs/flood_serving_project
"""

# **Important notes**:
# - This version of the PSS instance builder is hard to maintain as the Flood Service Project evolves.
# It is recommended to use the PSS instantiated services from the flood_model_serving.services.instantiated_services
# module instead of this script.
# - However, if we stick to the provided endpoints, none is made to run ORSA reporting for Sovereign.
# In this case, this user-defined PSS instance builder becomes usefull to build a custom PSS instance for Sovereign.
# - For the moment, this script is kept for reference and to be used as a template to build custom PSS instances.

from importlib import import_module
from typing import TYPE_CHECKING

# Import version information for the CGIC Home/Farm PSS assets
from cgl_dbx_flood_pipeline.config.pss_versions import PSS_VERSIONS_MAP_CGIC_HOME

if TYPE_CHECKING:
    from flood_model_serving.pss_lib.pss import HomeFarmPSSV1


def import_flex_map_function(module: str, function_name: str):
    flex_map_module = __import__(module, fromlist=[function_name])
    return getattr(flex_map_module, function_name)


def init_cgic_home_farm_pss_service(
    endpoint_version: str = "v1_1",
    default_basement_indicator: str = "Y",
    default_number_storeys: str = "1.0",
    default_is_condo_ten: str = "No",
) -> "HomeFarmPSSV1":
    """
    Initialize the PSS service for the CGIC Home/Farm product.
    Args:
            endpoint_version (str): Version of the PSS endpoint to use. Current options are "v1", "v1_1", and "v2_index_100".
            default_basement_indicator (str): Default value for basement indicator.
            default_number_storeys (str): Default value for the number of storeys.
            default_is_condo_ten (str): Default value for condo tenure indicator.

    Returns:
            HomeFarmPSSV1: Initialized PSS service instance.
    """
    if endpoint_version not in PSS_VERSIONS_MAP_CGIC_HOME:
        raise ValueError(f"Unsupported endpoint version: {endpoint_version}")

    # Defer optional dependency imports until the service is initialized.
    pss_metrics = import_module("flood_model_serving.pss_lib.pss_metrics")
    building_dc_extractor_class = import_module(
        "flood_model_serving.pss_lib.dc_extractor"
    ).BuildingDcExtractor
    home_farm_pss_class = import_module("flood_model_serving.pss_lib.pss").HomeFarmPSSV1

    # Map the endpoint version to the corresponding PSS versioning information
    pss_versioning = PSS_VERSIONS_MAP_CGIC_HOME[endpoint_version]
    generic_pss_assets_version = pss_versioning.generic_pss_assets_version
    product_pss_assets_version = pss_versioning.product_pss_assets_version
    dc_version = pss_versioning.product_dc_version
    flex_map_version = pss_versioning.cie_flex_map_version

    # Import the necessary asset modules based on the versioning information
    generic_assets = import_module(
        f"flood_model_serving.assets.pss.cgic.generic.{generic_pss_assets_version}.assets"
    )
    product_assets = import_module(
        f"flood_model_serving.assets.pss.cgic.home_farm.{product_pss_assets_version}.assets"
    )
    dc_path = import_module(f"flood_model_serving.assets.pss.dc.home_farm.cgic.{dc_version}.paths")

    # Initialize the PSS calculator class based on the generic PSS assets version
    pss_calculator = getattr(
        pss_metrics, f"CgicPSSCalculatorHomeFarm{generic_pss_assets_version.upper()}"
    )

    # Initialize the flexible map lookup based on the flex map version
    if flex_map_version in ["v1", "v1_1"]:
        flex_map_function = import_flex_map_function(
            "flood_model_serving.flex_map_lib.map_lookup", "FlexibleMapLookupV1"
        )

        map_assets = import_module(
            f"flood_model_serving.assets.flex_map.cgic.{flex_map_version}.assets"
        )
        map_paths = import_module(
            f"flood_model_serving.assets.flex_map.cgic.{flex_map_version}.paths"
        )

        map_lookup = flex_map_function(
            default_output=map_assets.DEFAULT_VALUES_OUTPUT,
            output_type=map_assets.OUTPUT_TYPE,
            shape_file_col_rename=map_assets.SHAPEFILE_COL_RENAME,
            shapefile_col_value_mapping=map_assets.SHAPEFILE_COL_VALUE_MAPPING,
            dict_raster_paths=map_paths.RASTER_PATHS,
            dict_shapefile_paths=map_paths.SHAPEFILES_PATHS,
            dict_multi_tiled_raster_paths=map_paths.MULTI_TILED_RASTERS_PATHS,
        )
    elif "v2_index" in flex_map_version:
        # In V2, we created the Flood Spatial index. This implies that the lookup mechanism is different from V1.
        flex_map_module = import_module("flood_model_serving.assets.flex_map.cgic.v2.services")
        flex_map_version = flex_map_version.split("v2_index_")[-1]
        map_lookup = getattr(flex_map_module, f"FlexiMap_CGIC_v{flex_map_version}")()
    else:
        raise ValueError(f"Unsupported flex map version: {flex_map_version}")

    # Initialize the class that manage damage curves
    building_dc_extractor = building_dc_extractor_class(
        path_dc=dc_path.PATH_HOME_FARM_DC,
        path_dc_mapping=dc_path.PATH_HOME_FARM_DC_MAPPING,
        path_building_style_convertion_table=dc_path.PATH_HOME_FARM_BUILDING_STYLE,
        path_foundation_type_convertion_table=dc_path.PATH_HOME_FARM_FOUNDATION_TYPE,
        path_risk_type_convertion_table=dc_path.PATH_HOME_FARM_RISK_TYPE,
        default_basement_indicator=default_basement_indicator,
        default_number_storeys=default_number_storeys,
        default_is_condo_ten=default_is_condo_ten,
    )

    # Initialize the PSS calculator for Home Farm buildings
    _pss_calculator = pss_calculator(
        pss_coop_ss_fd_adj_factor=product_assets.PSS_HOME_FARM_COOP_SS_FD_ADJ_FACTORS,
        liftoff_point=product_assets.PSS_HOME_FARM_LIFTOFF_POINT,
        pss_jba_coop_split_factor=product_assets.PSS_HOME_FARM_SPLIT_FACTOR,
        pss_weights=product_assets.PSS_HOME_FARM_WEIGHTS,
        pss_is_protected_code=product_assets.PSS_HOME_FARM_IS_PROTECTED_CODE,
        pss_is_not_protected_code=product_assets.PSS_HOME_FARM_IS_NOT_PROTECTED_CODE,
        building_dc_extractor=building_dc_extractor,
        list_q_series=generic_assets.LIST_Q_SERIES_INT,
        output_type=generic_assets.OUTPUT_TYPE,
        flex_map_convertion_dict=generic_assets.FLEX_MAP_TYPE_CONVERTION,
        pss_zone_mapping=product_assets.PSS_HOME_FARM_ZONE_MAPPING,
    )

    # Build the endpoint instance for the Home Farm PSS service
    pss_instance = home_farm_pss_class(
        flex_map_object=map_lookup, pss_calculator_object=_pss_calculator
    )
    return pss_instance


if __name__ == "__main__":
    # Example usage of the imported modules

    try:
        pss_cgic_home = init_cgic_home_farm_pss_service(endpoint_version="v1")
        pss_cgic_home.predict(
            lat=43.6532,
            long=-79.3832,
            building_style="Bungalow",
            foundation_type="FullBasement",
            risk_type="RES",
        )

        pss_cgic_home = init_cgic_home_farm_pss_service(endpoint_version="v2_index_100")
        pss_cgic_home.predict(
            lat=43.6532,
            long=-79.3832,
            building_style="Bungalow",
            foundation_type="FullBasement",
            risk_type="RES",
        )
    except Exception as e:  # noqa: BLE001
        print(f"An error occurred: {e}")
    else:
        print("PSS service initialized and prediction made successfully.")
