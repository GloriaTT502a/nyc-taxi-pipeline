# src/nyc_taxi_pipeline/bronze/transformations.py 

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# import target table schema and rename map 
from nyc_taxi_pipeline.contracts.bronze_schema import BUSINESS_SCHEMA, RENAME_MAP
from nyc_taxi_pipeline.metadata.system_columns import INPUT_FILE_COLUMN, PARTITION_COL_YYYYMM 
from nyc_taxi_pipeline.observability.lineage import inject_bronze_audit_columns 


def normalize_dataframe(
    df: DataFrame,
    run_id: str 
) -> DataFrame:
    """
    Normalize raw dataframe into Bronze Contract.

    Features
    --------
    1. Rename old columns
    2. Cast all contract columns
    3. Fill missing columns with NULL
    4. Preserve unknown columns (Schema Evolution)
    5. Generate YYYY / YYYYMM from source filename
    6. Inject audit columns
    """
    final_exprs = []
    columns_set = set(df.columns)

    # record the processed source columns 
    processed_source_cols = set()

    file_path_col = F.col("_temp_file_path")
    
    # 1. Get the date from parquet file name
    # File name is: yellow_tripdata_2010-01.parquet
    date_str = F.regexp_extract(file_path_col, r"(\d{4})-(\d{2})", 0)
    final_exprs.append(F.substring(date_str, 1, 4).cast("integer").alias("YYYY"))
    final_exprs.append(F.regexp_replace(date_str, "-", "").cast("integer").alias(PARTITION_COL_YYYYMM)) 

    final_exprs.append(file_path_col.alias(INPUT_FILE_COLUMN))
    
    # ==========================================
    # 2. Process Baseline Contract (Handle Missing & Renames)
    # ==========================================
    for target_name, attributes in BUSINESS_SCHEMA.items():
        
        dtype = attributes["type"] 
        old_name = next((k for k, v in RENAME_MAP.items() if v == target_name), None) 
        
        if target_name in columns_set:
            final_exprs.append(F.col(target_name).cast(dtype).alias(target_name))
            processed_source_cols.add(target_name) 
        elif old_name and old_name in columns_set:
            final_exprs.append(F.col(old_name).cast(dtype).alias(target_name))
            processed_source_cols.add(old_name)
        else:
            # If any missing column, fill null. 
            final_exprs.append(F.lit(None).cast(dtype).alias(target_name))

    # ==========================================
    # 3. Schema Evolution Enabler: Preserve Unknown Columns
    # ========================================== 
    for raw_col in columns_set: 
        if raw_col not in processed_source_cols and raw_col not in RENAME_MAP and raw_col != "_temp_file_path":
            final_exprs.append(F.col(raw_col))
    
    # ==========================================
    # 4. Apply Projection & Inject Lineage
    # ==========================================
    business_df = df.select(*final_exprs) 

    return inject_bronze_audit_columns(business_df, run_id)

