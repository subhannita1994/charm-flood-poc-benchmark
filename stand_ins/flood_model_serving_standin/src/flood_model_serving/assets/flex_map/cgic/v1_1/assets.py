"""Flex-map constants, CGIC v1_1 (stand-in)."""
DEFAULT_VALUES_OUTPUT = {"distanceToWater": 9999.0, "elevationToWater": 999.0, "exclusionZoneIndicator": "N",
                         "fluvialFloodZone": 0, "stormSurgeFloodZone": 0}
OUTPUT_TYPE = {"distanceToWater": float, "elevationToWater": float}
SHAPEFILE_COL_RENAME = {"SOP": "standard_of_protection", "PARTIAL": "partial", "ZONE": "zone"}
SHAPEFILE_COL_VALUE_MAPPING = {"partial": {1: "Y", 0: "N"}}
