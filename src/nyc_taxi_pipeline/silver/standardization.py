# src/nyc_taxi_pipeline/silver/standardization.py

import logging
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

# Input schema and dq rules 
from nyc_taxi_pipeline.config.settings import PipelineSettings 

from nyc_taxi_pipeline.contracts.bronze_schema import BRONZE_SCHEMA, EXPECTED_BRONZE_COLS
from nyc_taxi_pipeline.silver.dq_rules import get_silver_dq_rules

from nyc_taxi_pipeline.metadata.system_columns import (
    BRONZE_RUN_ID_COLUMN, 
    BRONZE_LOAD_TIMESTAMP_COLUMN,
    SILVER_RUN_ID_COLUMN, 
    SILVER_LOAD_TIMESTAMP_COLUMN
)

from nyc_taxi_pipeline.metadata.business_columns import (
    COL_PU_DATETIME, 
    COL_DO_DATETIME,
    COL_PU_DATETIME_UTC, 
    COL_DO_DATETIME_UTC,
    COL_FARE_AMOUNT, 
    COL_FARE_PER_MINUTE, 
    COL_DURATION_MIN 
)


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

def apply_transformations(df: DataFrame, run_id: str, settings: PipelineSettings) -> DataFrame:
    """Add duration_min and temp_eff columns for data quality check"""
    base_cols = [F.col(c) for c in EXPECTED_BRONZE_COLS]
    
    # convert pickup timestamp and dropoff timestamp to UTC
    
    local_tz = settings.timezone

    pickup_utc = F.to_utc_timestamp(F.col(COL_PU_DATETIME), local_tz)
    dropoff_utc = F.to_utc_timestamp(F.col(COL_DO_DATETIME), local_tz) 

    return (
        df.select(*base_cols, 
                  pickup_utc.alias(COL_PU_DATETIME_UTC), 
                  dropoff_utc.alias(COL_DO_DATETIME_UTC), 
                  ((dropoff_utc.cast("long") - pickup_utc.cast("long")) / 60.0).alias(COL_DURATION_MIN),
                  F.lit(run_id).alias(SILVER_RUN_ID_COLUMN), 
                  F.current_timestamp().alias(SILVER_LOAD_TIMESTAMP_COLUMN))
          .withColumn(COL_FARE_PER_MINUTE, 
                      F.when(F.col(COL_DURATION_MIN) > 0, F.col(COL_FARE_AMOUNT) / F.col(COL_DURATION_MIN))
                       .otherwise(F.lit(0.0)))
    )

def apply_dq_and_split(df: DataFrame, settings: PipelineSettings) -> tuple[DataFrame, DataFrame]:
    """Get DQ rules"""
    # Get data quality check rules 
    rules = get_silver_dq_rules(settings)
    
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
