# src/nyc_taxi_pipeline/metadata/system_columns.py

# ==========================================
# System Metadata Constants
# Global platform-level field standards that do not change with the environment (Dev/Prod).
# ========================================== 

# Batch tracing lineage 
BRONZE_RUN_ID_COLUMN = "_bronze_run_id" 

# Source of original documents 
INPUT_FILE_COLUMN = "_input_file" 

# Data load timestamp 
BRONZE_LOAD_TIMESTAMP_COLUMN = "_bronze_load_timestamp" 

# Data Quality Status 
DQ_STATUS_COLUMN = "_dq_status"

# Partition column
PARTITION_COL_YYYYMM = "YYYYMM"


# --- Silver Layer Audit ---
SILVER_RUN_ID_COLUMN = "_silver_run_id"
SILVER_LOAD_TIMESTAMP_COLUMN = "_silver_load_timestamp" 

