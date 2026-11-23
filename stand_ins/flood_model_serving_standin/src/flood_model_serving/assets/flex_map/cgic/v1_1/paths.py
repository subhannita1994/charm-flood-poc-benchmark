"""Flex-map asset paths, CGIC v1_1 (stand-in). Import-time existence checks mimic upstream."""
from flood_model_serving._paths_common import (  # noqa: F401
    MULTI_TILED_RASTERS_PATHS, RASTER_PATHS, SHAPEFILES_PATHS, all_map_files, check_exists,
)

IMPORT_CHECK_SECONDS = check_exists(all_map_files())
