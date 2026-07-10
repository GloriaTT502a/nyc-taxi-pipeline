# src/nyc_taxi_pipeline/contracts/silver_schema.py

from typing import Dict, Any, List
from nyc_taxi_pipeline.metadata.system_columns import (
    BRONZE_RUN_ID_COLUMN, 
    BRONZE_LOAD_TIMESTAMP_COLUMN, 
    INPUT_FILE_COLUMN, 
    SILVER_RUN_ID_COLUMN,
    SILVER_LOAD_TIMESTAMP_COLUMN, 
    PARTITION_COL_YYYYMM 
)

from nyc_taxi_pipeline.metadata.business_columns import *


# ==========================================
# Data Contract: Silver Layer Expected Schema
# ========================================== 


SILVER_SCHEMA: Dict[str, Dict[str, Any]] = {
    # Unique key and spatial index 
    COL_TRIP_KEY: {"type": "string", "required": True},
    COL_H3_PU: {"type": "string", "required": False},
    COL_H3_DO: {"type": "string", "required": False}, 

    # Critical Business fields 
    COL_VENDOR_ID: {"type": "string", "required": False},
    COL_RATE_CODE: {"type": "long", "required": False},
    COL_STORE_FWD_FLAG: {"type": "string", "required": False},
    COL_PAYMENT_TYPE: {"type": "string", "required": False},
    COL_PASSENGER_COUNT: {"type": "long", "required": False}, 
    COL_TRIP_DISTANCE: {"type": "double", "required": False}, 

    # Local Time and UTC Time 
    COL_PU_DATETIME: {"type": "timestamp", "required": False},
    COL_DO_DATETIME: {"type": "timestamp", "required": False},
    COL_PU_DATETIME_UTC: {"type": "timestamp", "required": True},
    COL_DO_DATETIME_UTC: {"type": "timestamp", "required": True}, 

    
    # Business Derived
    COL_DURATION_MIN: {"type": "double", "required": False},
    COL_FARE_PER_MINUTE: {"type": "double", "required": False}, 

    # Financial Detail Columns
    COL_FARE_AMOUNT: {"type": "double", "required": False},
    COL_SURCHARGE: {"type": "double", "required": False},
    COL_MTA_TAX: {"type": "double", "required": False},
    COL_TIP_AMOUNT: {"type": "double", "required": False},
    COL_TOLLS_AMOUNT: {"type": "double", "required": False},
    COL_IMPROVEMENT_SURCHARGE: {"type": "double", "required": False}, 
    COL_CONGESTION_SURCHARGE: {"type": "double", "required": False},
    COL_AIRPORT_FEE: {"type": "double", "required": False},
    COL_CBD_CONGESTION_FEE: {"type": "double", "required": False},

    # Financial summary 
    COL_TOTAL_AMOUNT: {"type": "double", "required": True}, 

    # Spatial and location information 
    COL_PU_LOCATION_ID: {"type": "long", "required": False},
    COL_DO_LOCATION_ID: {"type": "long", "required": False},
    COL_PU_LNG: {"type": "double", "required": False},
    COL_PU_LAT: {"type": "double", "required": False},
    COL_DO_LNG: {"type": "double", "required": False},
    COL_DO_LAT: {"type": "double", "required": False}, 

    # ------------------------------------------
    # Telemetry for RCA (根因排查探针)
    # ------------------------------------------
    COL_IS_PU_FALLBACK: {"type": "integer", "required": False},
    COL_IS_DO_FALLBACK: {"type": "integer", "required": False},
    
    # Partition Key 
    "YYYY": {"type": "integer", "required": False},
    PARTITION_COL_YYYYMM: {"type": "integer", "required": True}, 

    INPUT_FILE_COLUMN: {"type": "string", "required": False}, 
    
    # Platform lineage and audit fields 
    BRONZE_RUN_ID_COLUMN: {"type": "string", "required": True},
    BRONZE_LOAD_TIMESTAMP_COLUMN: {"type": "timestamp", "required": True},
    SILVER_RUN_ID_COLUMN: {"type": "string", "required": True},
    SILVER_LOAD_TIMESTAMP_COLUMN: {"type": "timestamp", "required": True}

}

EXPECTED_SILVER_COLS: List[str] = list(SILVER_SCHEMA.keys())

