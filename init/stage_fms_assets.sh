#!/bin/bash
# Rec #1 as cluster configuration (no code change): copy PSS map assets from the governed UC Volume to each
# node's local NVMe at startup, then point flood_model_serving at the local copy via FMS_ASSETS_ROOT.
set -euo pipefail
SRC="${FMS_STAGE_SRC:-/Volumes/catalog_8as5lj_bvv7nu/charm_poc/assets/flood_model_serving}"
DST="/local_disk0/fms_assets"
start=$(date +%s)
mkdir -p "$DST"
cp -r "$SRC/." "$DST/"
echo "{\"staged_seconds\": $(( $(date +%s) - start )), \"bytes\": $(du -sb "$DST" | cut -f1)}" > "$DST/STAGED.json"
