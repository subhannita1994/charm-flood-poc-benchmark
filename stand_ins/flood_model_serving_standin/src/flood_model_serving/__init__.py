"""CHARM-POC STAND-IN for Cooperators' internal `flood_model_serving` (Flood-Model-Serving repo).

The real package (and its JBA-style flood maps) is not available to Databricks (assumption A4).
This stand-in reproduces the *I/O shape* that drives runtime, not the actuarial values:
  * import-time `os.path.exists` checks on every asset path (rec #1 "~5 min per class" symptom),
  * service init: read all vector layers (GeoPandas) + damage-curve tables,
  * per-predict(): open + sample each raster / tile (rasterio), point-in-polygon on vector layers,
    damage-ratio interpolation per return period.
Asset root comes from env FMS_ASSETS_ROOT so the same code can point at a UC Volume (FUSE) or local disk.
"""
import os

ASSETS_ROOT = os.environ.get("FMS_ASSETS_ROOT", "/Volumes/catalog_8as5lj_bvv7nu/charm_poc/assets/flood_model_serving")
RETURN_PERIODS = (20, 50, 75, 100, 200, 500, 1500)
__version__ = "2.0.1+charm.standin"
