"""PSS calculator stand-in: converts map lookups into the PSS response fields."""
from __future__ import annotations


class CgicPSSCalculatorHomeFarmV1:
    def __init__(self, pss_coop_ss_fd_adj_factor, liftoff_point, pss_jba_coop_split_factor, pss_weights,
                 pss_is_protected_code, pss_is_not_protected_code, building_dc_extractor, list_q_series,
                 output_type, flex_map_convertion_dict, pss_zone_mapping):
        self.liftoff = liftoff_point
        self.rps = list(list_q_series)
        self.dc = building_dc_extractor
        self.zone_mapping = pss_zone_mapping

    @staticmethod
    def _first_rp(depths: dict) -> int | None:
        for rp, d in sorted(depths.items()):
            if d > 0:
                return rp
        return None

    def compute(self, m: dict, building_style, foundation_type, risk_type) -> dict:
        curve = self.dc.curve(building_style, foundation_type, risk_type)
        out = {
            "distanceToWater": m["distanceToWater"],
            "elevationToWater": m["elevationToWater"],
            "exclusionZoneIndicator": m["exclusionZoneIndicator"],
            "fluvialReturnPeriodCode": self._first_rp(m["fluvialDepth"]),
            "fluvialDefenseStandardofProtection": m["fluvialDefenseSOP"],
            "partialFluvialDefenseIndicator": m["fluvialDefensePartial"],
            "stormSurgeReturnPeriodCode": self._first_rp(m["stormSurgeDepth"]),
            "stormSurgeDefenseStandardofProtection": m["stormSurgeDefenseSOP"],
            "partialStormSurgeDefenseIndicator": m["stormSurgeDefensePartial"],
            "fluvialFloodZone": self.zone_mapping.get(m["fluvialFloodZone"], 0),
            "stormSurgeFloodZone": self.zone_mapping.get(m["stormSurgeFloodZone"], 0),
        }
        for peril, depth_key, sop_key, partial_key in (
            ("fluvial", "fluvialDepth", "fluvialDefenseSOP", "fluvialDefensePartial"),
            ("stormSurge", "stormSurgeDepth", "stormSurgeDefenseSOP", "stormSurgeDefensePartial"),
        ):
            bdr, cdr, eff = {}, {}, {}
            for rp in self.rps:
                depth = max(0.0, m[depth_key].get(rp, 0.0) - self.liftoff)
                b, c = self.dc.damage_ratios(curve, depth)
                bdr[str(rp)], cdr[str(rp)] = b, c
                protected = m[sop_key] and rp <= m[sop_key]
                eff[str(rp)] = (0.5 if m[partial_key] == "Y" else 1.0) if protected else 0.0
            out[f"{peril}BuildingDamageRatios"] = bdr
            out[f"{peril}ContentsDamageRatios"] = cdr
            out[f"{peril}DefenseEfficiencies"] = eff
        return out
