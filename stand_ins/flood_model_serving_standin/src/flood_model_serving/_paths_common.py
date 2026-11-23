"""Asset path registry shared by the versioned `paths` modules (stand-in)."""
import os
import time

from . import ASSETS_ROOT, RETURN_PERIODS

MAPS = os.path.join(ASSETS_ROOT, "maps")
N_FLUVIAL_TILES = int(os.environ.get("FMS_N_FLUVIAL_TILES", "16"))
N_SURGE_TILES = int(os.environ.get("FMS_N_SURGE_TILES", "8"))

RASTER_PATHS = {
    "distanceToWater": os.path.join(MAPS, "rasters", "distance_to_water.tif"),
    "elevationToWater": os.path.join(MAPS, "rasters", "elevation_to_water.tif"),
}
MULTI_TILED_RASTERS_PATHS = {
    "fluvialDepth": [os.path.join(MAPS, "tiles", "fluvial_depth", f"tile_{i:02d}.tif") for i in range(N_FLUVIAL_TILES)],
    "stormSurgeDepth": [os.path.join(MAPS, "tiles", "storm_surge_depth", f"tile_{i:02d}.tif") for i in range(N_SURGE_TILES)],
}
SHAPEFILES_PATHS = {
    "fluvialDefense": os.path.join(MAPS, "shapefiles", "fluvial_defenses.shp"),
    "stormSurgeDefense": os.path.join(MAPS, "shapefiles", "coastal_defenses.shp"),
    "fluvialFloodZone": os.path.join(MAPS, "shapefiles", "fluvial_flood_zones.shp"),
    "stormSurgeFloodZone": os.path.join(MAPS, "shapefiles", "coastal_flood_zones.shp"),
    "exclusionZone": os.path.join(MAPS, "shapefiles", "exclusion_zones.shp"),
}
_SHP_SIDECARS = (".shp", ".shx", ".dbf", ".prj")


def check_exists(paths):
    """Mimic the upstream import-time dependency checks (one stat() per file, over FUSE when on a Volume)."""
    t0 = time.perf_counter()
    missing = [p for p in paths if not os.path.exists(p)]
    if missing:
        raise FileNotFoundError(f"{len(missing)} flood_model_serving assets missing, e.g. {missing[:3]}")
    return time.perf_counter() - t0


def all_map_files():
    files = list(RASTER_PATHS.values())
    for tiles in MULTI_TILED_RASTERS_PATHS.values():
        files += tiles
    for shp in SHAPEFILES_PATHS.values():
        files += [shp[:-4] + ext for ext in _SHP_SIDECARS]
    return files
