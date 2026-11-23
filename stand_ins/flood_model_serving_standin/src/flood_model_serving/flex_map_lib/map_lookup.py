"""FlexibleMapLookupV1 stand-in: the raster/vector I/O that dominates PSS runtime.

ASSUMED upstream behaviour (A4, to be confirmed against the real map_lookup.py):
  * vector layers (defenses, flood zones, exclusion zones) are read fully at init with GeoPandas,
  * rasters / tiles are opened per lookup and sampled at the point (no persistent handle).
Env FMS_RASTER_HANDLES=cached keeps raster handles open per object instead; this is a
*proposed* optimisation for the library owners and is reported separately, never as their code.
"""
from __future__ import annotations

import os
import threading
import time

import geopandas as gpd
import numpy as np
import rasterio
from shapely.geometry import Point

from flood_model_serving import RETURN_PERIODS


class FlexibleMapLookupV1:
    def __init__(
        self,
        default_output: dict,
        output_type: dict,
        shape_file_col_rename: dict,
        shapefile_col_value_mapping: dict,
        dict_raster_paths: dict[str, str],
        dict_shapefile_paths: dict[str, str],
        dict_multi_tiled_raster_paths: dict[str, list[str]],
    ):
        t0 = time.perf_counter()
        self.default_output = default_output
        self.output_type = output_type
        self.rename = shape_file_col_rename
        self.value_mapping = shapefile_col_value_mapping
        self.raster_paths = dict_raster_paths
        self.handle_mode = os.environ.get("FMS_RASTER_HANDLES", "per_call")
        self._local = threading.local()

        # Vector layers: full read + spatial index at init (driver of first-call latency over FUSE).
        self.layers: dict[str, gpd.GeoDataFrame] = {}
        for name, path in dict_shapefile_paths.items():
            gdf = gpd.read_file(path).rename(columns=self.rename)
            _ = gdf.sindex  # build STRtree now
            self.layers[name] = gdf

        # Tile index: open each tile once to read its bounds.
        self.tiles: dict[str, list[tuple[tuple[float, float, float, float], str]]] = {}
        for name, paths in dict_multi_tiled_raster_paths.items():
            index = []
            for p in paths:
                with rasterio.open(p) as src:
                    b = src.bounds
                    index.append(((b.left, b.bottom, b.right, b.top), p))
            self.tiles[name] = index
        self.init_seconds = time.perf_counter() - t0

    # ---------------------------------------------------------------- raster access
    def _open(self, path: str):
        if self.handle_mode != "cached":
            return rasterio.open(path), True
        cache = getattr(self._local, "handles", None)
        if cache is None:
            cache = self._local.handles = {}
        if path not in cache:
            cache[path] = rasterio.open(path)
        return cache[path], False

    def _sample(self, path: str, lon: float, lat: float) -> np.ndarray:
        src, close = self._open(path)
        try:
            return np.asarray(next(src.sample([(lon, lat)])), dtype="float64")
        finally:
            if close:
                src.close()

    def _tile_for(self, name: str, lon: float, lat: float) -> str | None:
        for (left, bottom, right, top), path in self.tiles[name]:
            if left <= lon < right and bottom <= lat < top:
                return path
        return None

    # ---------------------------------------------------------------- vector access
    def _polygon_attrs(self, name: str, pt: Point) -> dict | None:
        gdf = self.layers[name]
        hits = gdf.sindex.query(pt, predicate="intersects")
        if len(hits) == 0:
            return None
        return gdf.iloc[int(hits[0])].drop(labels="geometry").to_dict()

    # ---------------------------------------------------------------- public API
    def lookup(self, lat: float, long: float) -> dict:
        out = dict(self.default_output)
        for name, path in self.raster_paths.items():
            out[name] = float(self._sample(path, long, lat)[0])
        for name in self.tiles:
            tile = self._tile_for(name, long, lat)
            depths = self._sample(tile, long, lat) if tile else np.zeros(len(RETURN_PERIODS))
            out[name] = {rp: max(0.0, float(d)) / 100.0 for rp, d in zip(RETURN_PERIODS, depths)}  # cm -> m
        pt = Point(long, lat)
        for name in ("fluvialDefense", "stormSurgeDefense"):
            attrs = self._polygon_attrs(name, pt)
            out[f"{name}SOP"] = int(attrs["standard_of_protection"]) if attrs else 0
            out[f"{name}Partial"] = self.value_mapping["partial"].get(int(attrs["partial"]), "N") if attrs else "N"
        for name in ("fluvialFloodZone", "stormSurgeFloodZone"):
            attrs = self._polygon_attrs(name, pt)
            out[name] = int(attrs["zone"]) if attrs else 0
        out["exclusionZoneIndicator"] = "Y" if self._polygon_attrs("exclusionZone", pt) else "N"
        return out
