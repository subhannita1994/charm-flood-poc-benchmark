#!/usr/bin/env bash
# Run one SQL statement on a SQL warehouse and print rows as TSV.   ./scripts/sql.sh "SELECT ..."
set -euo pipefail
source "$(dirname "$0")/config.sh"
: "${CHARM_WAREHOUSE_ID:?set CHARM_WAREHOUSE_ID (databricks warehouses list)}"
python3 -I - "$1" <<'PY'
import json, os, subprocess, sys, time
p = os.environ["CHARM_PROFILE"]
def api(method, path, body=None):
    a = ["databricks", "api", method, path, "--profile", p] + (["--json", json.dumps(body)] if body else [])
    return json.loads(subprocess.run(a, capture_output=True, text=True, check=True).stdout)
r = api("post", "/api/2.0/sql/statements", {"statement": sys.argv[1], "warehouse_id": os.environ["CHARM_WAREHOUSE_ID"], "wait_timeout": "50s"})
while r["status"]["state"] in ("PENDING", "RUNNING"):
    time.sleep(3); r = api("get", f"/api/2.0/sql/statements/{r['statement_id']}")
if r["status"]["state"] != "SUCCEEDED":
    sys.exit(json.dumps(r["status"]))
print("\t".join(c["name"] for c in r["manifest"]["schema"]["columns"]))
for row in r.get("result", {}).get("data_array", []) or []:
    print("\t".join("" if v is None else str(v) for v in row))
PY
