"""HomeFarmPSSV1 stand-in: row-level predict(), the only call the scoring pipeline can make."""
from __future__ import annotations


class HomeFarmPSSV1:
    def __init__(self, flex_map_object, pss_calculator_object):
        self.flex_map = flex_map_object
        self.calculator = pss_calculator_object

    def predict(self, lat: float, long: float, building_style: str = "", foundation_type: str = "",
                risk_type: str = "") -> dict:
        maps = self.flex_map.lookup(lat=lat, long=long)
        return self.calculator.compute(maps, building_style, foundation_type, risk_type)
