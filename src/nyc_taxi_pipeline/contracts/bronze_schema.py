from typing import Dict, Any, List 
from nyc_taxi_pipeline.metadata.system_columns import (
    BRONZE_RUN_ID_COLUMN, 
    BRONZE_LOAD_TIMESTAMP_COLUMN, 
    INPUT_FILE_COLUMN, 
    PARTITION_COL_YYYYMM  
) 

from nyc_taxi_pipeline.metadata.business_columns import *

# ==========================================
# 1. 业务字段契约 (专供 Transform 层读取源文件并 Cast)
# ==========================================
BUSINESS_SCHEMA: Dict[str, Dict[str, Any]] = {
    COL_VENDOR_ID: {"type": "string", "required": False},
    COL_PU_DATETIME: {"type": "timestamp", "required": False},
    COL_DO_DATETIME: {"type": "timestamp", "required": False},
    COL_PASSENGER_COUNT: {"type": "long", "required": False},
    COL_TRIP_DISTANCE: {"type": "double", "required": False},
    COL_RATE_CODE: {"type": "long", "required": False},
    COL_STORE_FWD_FLAG: {"type": "string", "required": False},
    COL_PU_LNG: {"type": "double", "required": False},
    COL_PU_LAT: {"type": "double", "required": False},
    COL_DO_LNG: {"type": "double", "required": False},
    COL_DO_LAT: {"type": "double", "required": False},
    COL_PU_LOCATION_ID: {"type": "long", "required": False},
    COL_DO_LOCATION_ID: {"type": "long", "required": False},
    COL_PAYMENT_TYPE: {"type": "string", "required": False},
    COL_FARE_AMOUNT: {"type": "double", "required": False},
    COL_SURCHARGE: {"type": "double", "required": False},
    COL_MTA_TAX: {"type": "double", "required": False},
    COL_TIP_AMOUNT: {"type": "double", "required": False},
    COL_TOLLS_AMOUNT: {"type": "double", "required": False},
    COL_IMPROVEMENT_SURCHARGE: {"type": "double", "required": False},
    COL_CONGESTION_SURCHARGE: {"type": "double", "required": False},
    COL_AIRPORT_FEE: {"type": "double", "required": False},
    COL_CBD_CONGESTION_FEE: {"type": "double", "required": False},
    COL_TOTAL_AMOUNT: {"type": "double", "required": True}
} 

# ==========================================
# 2. 派生与审计字段 (Transformer 代码自动注入，源文件中不存在)
# ==========================================
DERIVED_AUDIT_SCHEMA: Dict[str, Dict[str, Any]] = {
    "YYYY": {"type": "integer", "required": False},
    PARTITION_COL_YYYYMM: {"type": "integer", "required": True},
    BRONZE_RUN_ID_COLUMN: {"type": "string", "required": False},
    BRONZE_LOAD_TIMESTAMP_COLUMN: {"type": "timestamp", "required": False},
    INPUT_FILE_COLUMN: {"type": "string", "required": False}
}

# ==========================================
# 3. Bronze 层最终输出全量契约 (1 和 2 的合并)
# ========================================== 
BRONZE_SCHEMA = {**BUSINESS_SCHEMA, **DERIVED_AUDIT_SCHEMA} 
EXPECTED_BRONZE_COLS: List[str] = list(BRONZE_SCHEMA.keys()) 

# ==========================================
# 4. Schema Evolution 演进映射表
# ========================================== 
RENAME_MAP = {
    "VendorID": "vendor_id", 
    "tpep_pickup_datetime": "pickup_datetime", 
    "tpep_dropoff_datetime": "dropoff_datetime", 
    "RatecodeID": "rate_code", 
    "extra": "surcharge", 
    "Airport_fee": "airport_fee", 
}
