"""In-process stand-in for the hosted CGIC flood API (assumption A5).

Subclasses the *verbatim* upstream CGICHomeModelAPI: request building and parse_response are theirs;
only the network round trip is replaced by a sleep with the calibrated latency plus a deterministic
synthetic response in the real API schema. Faults raise the same exception type upstream post_json raises.
"""
from __future__ import annotations

import hashlib
import random
import time
from typing import Any

import pandas as pd

from cgl_dbx_flood_pipeline.api.cgic_home_endpoint import MAP_FIELDS, RETURN_PERIODS, CGICHomeModelAPI


class StubbedCGICHomeModelAPI(CGICHomeModelAPI):
    def __init__(self, latency_ms: float = 49.0, jitter: float = 0.3, fault_rate: float = 0.0,
                 max_workers: int = 8, seed: int = 7):
        self.latency_ms, self.jitter, self.fault_rate = latency_ms, jitter, fault_rate
        self._max_workers, self.seed = max_workers, seed

    def worker_count(self) -> int:  # upstream reads CGIC_FLOOD_API_MAX_WORKERS (default 8)
        return self._max_workers

    def _fake_post(self, payload: dict) -> dict:
        h = int(hashlib.md5(f"{payload['lat']:.5f}{payload['long']:.5f}".encode()).hexdigest()[:8], 16)
        rnd = random.Random(h ^ self.seed)
        lat_s = max(0.0, rnd.lognormvariate(0, self.jitter)) * self.latency_ms / 1000.0
        time.sleep(lat_s)
        if self.fault_rate and random.random() < self.fault_rate:
            raise RuntimeError("Flood API returned HTTP 503.")  # same type/message shape as upstream post_json
        data: dict[str, Any] = {
            "distanceToWater": round(rnd.uniform(0, 3000), 2), "elevationToWater": round(rnd.uniform(-2, 40), 2),
            "exclusionZoneIndicator": rnd.choice(["Y", "N", "N", "N"]),
            "fluvialReturnPeriodCode": rnd.choice([20, 50, 100, 200, 500, ""]),
            "fluvialDefenseStandardofProtection": rnd.choice([0, 20, 50, 100]),
            "partialFluvialDefenseIndicator": rnd.choice(["Y", "N"]),
            "stormSurgeReturnPeriodCode": rnd.choice([20, 100, 500, ""]),
            "stormSurgeDefenseStandardofProtection": rnd.choice([0, 50, 100]),
            "partialStormSurgeDefenseIndicator": rnd.choice(["Y", "N"]),
            "fluvialFloodZone": rnd.randint(0, 4), "stormSurgeFloodZone": rnd.randint(0, 4),
        }
        for field in MAP_FIELDS:
            data[field] = {str(rp): round(rnd.random() * (0.3 if "Damage" in field else 1.0), 4) for rp in RETURN_PERIODS}
        return {"data": data, "error_info": None, "model_version": {"version": "pss_v1_1_stub"}}

    def score_row(self, values, effective_date, *, session=None):
        longitude, latitude, dwelling_style, foundation_type, risk_type = values
        payload = {  # identical payload construction to upstream score_row
            "long": float(longitude), "lat": float(latitude),
            "building_style": "" if pd.isna(dwelling_style) else dwelling_style,
            "foundation_type": "" if pd.isna(foundation_type) else foundation_type,
            "risk_type": "" if pd.isna(risk_type) else risk_type,
        }
        return self.parse_response(self._fake_post(payload))  # parse_response: verbatim upstream
