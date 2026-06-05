# src/nyc_taxi_pipeline/silver/standardization.py

import logging
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

# Input schema and dq rules 
from .schema import BRONZE_SCHEMA, EXPECTED_BRONZE_COLS, NY_TZ
from .dq_rules import get_silver_dq_rules

logger = logging.getLogger(__name__)

def ensure_bronze_schema(df: DataFrame) -> DataFrame:
    """Ensure Schema Integrity (Add NULL if no columns)"""
    normalized_df = df
    for col_name, meta in BRONZE_SCHEMA.items():
        if col_name not in normalized_df.columns:
            if meta["required"]:
                raise ValueError(f"致命错误: 缺少必填列 {col_name}")
            normalized_df = normalized_df.withColumn(
                col_name, F.lit(None).cast(meta["type"])
            )
    return normalized_df

def apply_transformations(df: DataFrame, run_id: str) -> DataFrame:
    """Add duration_min and temp_eff columns for data quality check"""
    base_cols = [
        F.col(c).alias("bronze_run_id") if c == "_run_id" else 
        F.col(c).alias("bronze_load_timestamp") if c == "_load_timestamp" else 
        F.col(c) for c in EXPECTED_BRONZE_COLS 
    ]
    
    # convert pickup timestamp and dropoff timestamp to UTC
    
    pickup_utc = F.to_utc_timestamp(F.col("pickup_datetime"), NY_TZ)
    dropoff_utc = F.to_utc_timestamp(F.col("dropoff_datetime"), NY_TZ) 

    return (
        df.select(*base_cols, 
                  pickup_utc.alias("pickup_datetime_utc"), 
                  dropoff_utc.alias("dropoff_datetime_utc")
                  ((dropoff_utc.cast("long") - pickup_utc.cast("long")) / 60.0).alias("duration_min"),
                  F.lit(run_id).alias("_run_id"), 
                  F.current_timestamp().alias("_processed_at"))
          .withColumn("temp_eff", 
                      F.when(F.col("duration_min") > 0, F.col("fare_amount") / F.col("duration_min"))
                       .otherwise(F.lit(0.0)))
    )

def apply_dq_and_split(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Get DQ rules"""
    # Get data quality check rules 
    rules = get_silver_dq_rules()
    
    rule_evaluations = [
        F.when(condition, F.lit(rule_name)).otherwise(F.lit(None).cast("string"))
        for rule_name, condition in rules.items()
    ]

    dq_df = df.withColumn("raw_rules_array", F.array(*rule_evaluations)) \
              .withColumn("violated_rules", F.filter(F.col("raw_rules_array"), lambda x: x.isNotNull())) \
              .withColumn("is_valid", F.size(F.col("violated_rules")) == 0) \
              .drop("raw_rules_array")

    valid_df = dq_df.filter(F.col("is_valid") == True).drop("violated_rules", "is_valid")
    rejected_df = dq_df.filter(F.col("is_valid") == False)

    return valid_df, rejected_df
