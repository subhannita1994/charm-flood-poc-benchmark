# Shared settings for the scripts in this folder. Override any of them in your shell before running.
export CHARM_PROFILE="${CHARM_PROFILE:-charm-azure-classic}"          # Databricks CLI profile (Azure workspace)
export CHARM_CATALOG="${CHARM_CATALOG:-catalog_8as5lj_bvv7nu}"        # existing UC catalog you can create schemas in
export CHARM_SCHEMA="${CHARM_SCHEMA:-charm_poc}"
export CHARM_WS_ROOT="${CHARM_WS_ROOT:-/Users/$(databricks current-user me --profile "$CHARM_PROFILE" -o json | python3 -I -c 'import json,sys;print(json.load(sys.stdin)["userName"])')/CHARM PoC/charm_benchmark}"
export CHARM_WAREHOUSE_ID="${CHARM_WAREHOUSE_ID:-}"                   # SQL warehouse for scripts/sql.sh
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"; export REPO
