"""BuildingDcExtractor stand-in: damage-curve tables loaded once at service init."""
from __future__ import annotations

import numpy as np
import pandas as pd


class BuildingDcExtractor:
    def __init__(self, path_dc, path_dc_mapping, path_building_style_convertion_table,
                 path_foundation_type_convertion_table, path_risk_type_convertion_table,
                 default_basement_indicator="Y", default_number_storeys="1.0", default_is_condo_ten="No"):
        dc = pd.read_csv(path_dc)  # columns: curve_id, depth_m, building_dr, contents_dr
        self.depths = {cid: g["depth_m"].to_numpy() for cid, g in dc.groupby("curve_id")}
        self.bdr = {cid: g["building_dr"].to_numpy() for cid, g in dc.groupby("curve_id")}
        self.cdr = {cid: g["contents_dr"].to_numpy() for cid, g in dc.groupby("curve_id")}
        mapping = pd.read_csv(path_dc_mapping)  # building_style, foundation_type, risk_type -> curve_id
        self.mapping = {(r.building_style, r.foundation_type, r.risk_type): r.curve_id for r in mapping.itertuples()}
        self.style = dict(pd.read_csv(path_building_style_convertion_table).values)
        self.foundation = dict(pd.read_csv(path_foundation_type_convertion_table).values)
        self.risk = dict(pd.read_csv(path_risk_type_convertion_table).values)
        self.default_curve = int(dc["curve_id"].min())

    def curve(self, building_style, foundation_type, risk_type) -> int:
        key = (self.style.get(building_style, "other"), self.foundation.get(foundation_type, "other"),
               self.risk.get(risk_type, "other"))
        return int(self.mapping.get(key, self.default_curve))

    def damage_ratios(self, curve_id: int, depth_m: float) -> tuple[float, float]:
        d = self.depths[curve_id]
        return float(np.interp(depth_m, d, self.bdr[curve_id])), float(np.interp(depth_m, d, self.cdr[curve_id]))
