# src/nyc_taxi_pipeline/contracts/dim_zone_schema.py 

"""
Taxi Zone Data Contract 
Match with table dim_taxi_zone_h3 (Migration 003)
""" 


# ---------------------------------------------------------
# Full field mapping of dimension table (Source of Truth)
# ---------------------------------------------------------
ZONE_LOCATION_ID_COL = "LocationID" 
ZONE_BOROUGH_COL = "borough" 
ZONE_ZONE_NAME_COL = "zone" 
ZONE_CENTROID_LAT_COL = "centroid_lat" 
ZONE_CENTROID_LNG_COL = "centroid_lng"
ZONE_H3_CELL_COL = "h3_cell" 
ZONE_WKT_COL = "raw_boundary_wkt"



