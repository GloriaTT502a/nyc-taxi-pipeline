from enum import Enum 
from nyc_taxi_pipeline.config.settings import PipelineSettings 

class Table(Enum): 
    """ Data Dictionary """
    # Bronze Layer 
    BRONZE_TRIP = ("bronze", "brz_yellow_nyc_taxi") 
    BRONZE_ZONE = ("bronze", "brz_taxi_zone") 
    STG_TAXI_ZONES_RAW = ("bronze", "stg_taxi_zones_raw") 

    # Silver Layer 
    SILVER_TRIP = ("silver", "slv_yellow_nyc_taxi") 
    SILVER_ZONE_H3 = ("silver", "slv_taxi_zone_h3")

    # Gold Layer 
    DIM_H3 = ("gold", "dim_taxi_zone_h3")
    BRIDGE_H3 = ("gold", "brg_taxi_zone_h3")
    FACT_TRIP_DAILY = ("gold", "fct_trip_daily_summary") 

    # Audit 
    AUDIT_LOG = ("silver", "ops_pipeline_audit")

def get_table_name(settings: PipelineSettings, table_enum: Table) -> str: 
    layer, table_name = table_enum.value 
    return settings.resolve_table_path(layer, table_name)
