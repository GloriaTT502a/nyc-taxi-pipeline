from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# import target table schema and rename map 
from nyc_taxi_pipeline.bronze.schema import CANONICAL_SCHEMA, RENAME_MAP

def normalize_dataframe(
    df: DataFrame,
    run_id: str,
    lineage_col: str,
    run_id_col: str
) -> DataFrame:
    """
    Transfer the schema from source table to target table according to the schema defined in schema.py
    """
    final_exprs = []
    columns_set = set(df.columns)

    # 1. Get the date from parquet file name
    # File name is: yellow_tripdata_2010-01.parquet
    date_str = F.regexp_extract(F.col("_metadata.file_path"), r"(\d{4})-(\d{2})", 0)
    final_exprs.append(F.substring(date_str, 1, 4).cast("int").alias("YYYY"))
    final_exprs.append(F.regexp_replace(date_str, "-", "").cast("int").alias("YYYYMM"))

    # 2. Transfer the schema from source table to target table (tolerate column missing)
    for name, dtype in CANONICAL_SCHEMA:
        old_name = next((k for k, v in RENAME_MAP.items() if v == name), None)
        
        if name in columns_set:
            final_exprs.append(F.col(name).cast(dtype).alias(name))
        elif old_name and old_name in columns_set:
            final_exprs.append(F.col(old_name).cast(dtype).alias(name))
        else:
            # If any missing column, fill null. 
            final_exprs.append(F.lit(None).cast(dtype).alias(name))

    # 3. Input metadata for data lineage (Observability & Lineage)
    final_exprs.extend([
        F.col("_metadata.file_path").alias(lineage_col),
        F.lit(run_id).alias(run_id_col),
        F.current_timestamp().alias("_load_timestamp")
    ])

    return df.select(*final_exprs)

