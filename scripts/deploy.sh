#!/usr/bin/env bash
# One-time (and after every code change): create UC objects, build the 3 wheels, upload them, import code to the workspace.
#   ./scripts/deploy.sh            # everything
#   ./scripts/deploy.sh wheels     # only rebuild + upload wheels
set -euo pipefail
source "$(dirname "$0")/config.sh"
P=(--profile "$CHARM_PROFILE"); VOL="dbfs:/Volumes/$CHARM_CATALOG/$CHARM_SCHEMA/assets/wheels"
what="${1:-all}"

if [[ $what == all ]]; then
  echo "== Unity Catalog: schema + volumes (raw, assets)"
  databricks schemas create "$CHARM_SCHEMA" "$CHARM_CATALOG" "${P[@]}" >/dev/null 2>&1 || echo "   schema exists"
  for v in raw assets; do
    databricks volumes create "$CHARM_CATALOG" "$CHARM_SCHEMA" "$v" MANAGED "${P[@]}" >/dev/null 2>&1 || echo "   volume $v exists"
  done
fi

echo "== Build wheels -> dist/"
rm -f "$REPO"/dist/*.whl
(cd "$REPO/src" && uv build --wheel -o "$REPO/dist" -q)
(cd "$REPO/stand_ins/flood_model_serving_standin" && uv build --wheel -o "$REPO/dist" -q)
(cd "$REPO/harness" && uv build --wheel -o "$REPO/dist" -q)
ls -1 "$REPO"/dist/*.whl

echo "== Upload wheels -> $VOL"
databricks fs mkdir "$VOL" "${P[@]}"
for w in "$REPO"/dist/*.whl; do databricks fs cp "$w" "$VOL/$(basename "$w")" --overwrite "${P[@]}"; done

if [[ $what == all ]]; then
  echo "== Import code -> /Workspace$CHARM_WS_ROOT"
  databricks workspace mkdirs "$CHARM_WS_ROOT" "${P[@]}"
  databricks workspace import-dir "$REPO/notebooks" "$CHARM_WS_ROOT/notebooks" --overwrite "${P[@]}" >/dev/null
  databricks workspace import-dir "$REPO/src" "$CHARM_WS_ROOT/src" --overwrite "${P[@]}" >/dev/null   # T1 variant A imports from here
  databricks workspace mkdirs "$CHARM_WS_ROOT/init" "${P[@]}"
  databricks workspace import "$CHARM_WS_ROOT/init/stage_fms_assets.sh" --file "$REPO/init/stage_fms_assets.sh" \
    --format AUTO --overwrite "${P[@]}"
fi
echo "done"
