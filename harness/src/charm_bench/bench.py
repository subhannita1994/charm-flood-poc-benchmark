"""Timing + results recording. One row per (test, variant, stage, run)."""
from __future__ import annotations

import json
import os
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

RESULT_SCHEMA = (
    "result_id string, recorded_at timestamp, test_id string, variant string, recs string, stage string, "
    "n_rows long, seconds double, rows_per_s double, measure string, code_fidelity string, "
    "compute string, job_run_id string, task_run_id string, workspace string, params string, notes string, ok boolean"
)


def materialize(df) -> None:
    """Force full evaluation without collecting to the driver."""
    try:
        df.write.format("noop").mode("overwrite").save()
    except Exception:  # noqa: BLE001 — fall back where noop sink is unavailable
        df.selectExpr("count(1)").collect()


class Recorder:
    def __init__(self, spark, table: str, *, compute: dict, job_run_id: str = "", task_run_id: str = "",
                 workspace: str = "", code_fidelity: str = "verbatim"):
        self.spark, self.table = spark, table
        self.compute = json.dumps(compute, sort_keys=True)
        self.job_run_id, self.task_run_id, self.workspace = job_run_id, task_run_id, workspace
        self.code_fidelity = code_fidelity
        self.rows: list[dict] = []
        self.last_seconds = float('nan')
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({RESULT_SCHEMA}) USING DELTA")

    @contextmanager
    def time(self, test_id: str, variant: str, stage: str, n_rows: int, *, recs: str = "", params: dict | None = None,
             notes: str = "", code_fidelity: str | None = None, measure: str = "measured"):
        t0 = time.perf_counter()
        row = {"ok": True, "notes": notes}
        try:
            yield row
        except Exception as e:  # noqa: BLE001 — record failures as findings, then re-raise
            row["ok"] = False
            row["notes"] = (notes + " | " if notes else "") + f"{type(e).__name__}: {e}"[:800]
            raise
        finally:
            secs = time.perf_counter() - t0
            self.last_seconds = secs
            self.rows.append({
                "result_id": str(uuid.uuid4()), "recorded_at": datetime.now(timezone.utc), "test_id": test_id,
                "variant": variant, "recs": recs, "stage": stage, "n_rows": int(n_rows), "seconds": float(secs),
                "rows_per_s": float(n_rows / secs) if secs > 0 else None, "measure": measure,
                "code_fidelity": code_fidelity or self.code_fidelity, "compute": self.compute,
                "job_run_id": self.job_run_id, "task_run_id": self.task_run_id, "workspace": self.workspace,
                "params": json.dumps(params or {}, sort_keys=True, default=str), "notes": row["notes"], "ok": row["ok"],
            })
            print(f"[{test_id}/{variant}/{stage}] n={n_rows} {secs:.2f}s ok={row['ok']}")
            self.flush()

    def add(self, test_id: str, variant: str, stage: str, n_rows: int, seconds: float, *, recs: str = "",
            params: dict | None = None, notes: str = "", measure: str = "measured", code_fidelity: str | None = None,
            ok: bool = True):
        """Record a value measured elsewhere (subprocess) or an extrapolation (measure='extrapolated')."""
        self.rows.append({
            "result_id": str(uuid.uuid4()), "recorded_at": datetime.now(timezone.utc), "test_id": test_id,
            "variant": variant, "recs": recs, "stage": stage, "n_rows": int(n_rows), "seconds": float(seconds),
            "rows_per_s": float(n_rows / seconds) if seconds and seconds == seconds else None, "measure": measure,
            "code_fidelity": code_fidelity or self.code_fidelity, "compute": self.compute, "job_run_id": self.job_run_id,
            "task_run_id": self.task_run_id, "workspace": self.workspace,
            "params": json.dumps(params or {}, sort_keys=True, default=str), "notes": notes, "ok": ok})
        self.flush()

    def flush(self):
        if not self.rows:
            return
        df = self.spark.createDataFrame(self.rows, schema=RESULT_SCHEMA)
        df.write.mode("append").saveAsTable(self.table)
        self.rows = []


def env_snapshot() -> dict:
    return {"cpus": os.cpu_count(), "python": os.sys.version.split()[0],
            "runtime": os.environ.get("DATABRICKS_RUNTIME_VERSION", "")}
