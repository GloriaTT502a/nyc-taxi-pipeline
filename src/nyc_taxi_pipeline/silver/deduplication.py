import pyspark.sql.functions as F 
from pyspark.sql import DataFrame 
from pyspark.sql.window import Window 

from nyc_taxi_pipeline.metadata.system_columns import SILVER_LOAD_TIMESTAMP_COLUMN

from nyc_taxi_pipeline.metadata.business_columns import COL_PU_DATETIME

def deduplicate_trips(df: DataFrame, partition_cols: list[str] = None) -> tuple[DataFrame, DataFrame]: 
    
    # 默认按 trip_key 去重，但也支持传入多个列（如 ["vendor_id", "pickup_datetime"]）
    if partition_cols is None:
        partition_cols = ["trip_key"] 

    window_spec = (
        Window.partitionBy(*partition_cols)
              .orderBy(
                  F.col(SILVER_LOAD_TIMESTAMP_COLUMN).desc_nulls_last(),
                  F.col(COL_PU_DATETIME).desc_nulls_last() # Tie-breaker 打断平局
              )
    )

    flagged_df = df.withColumn("_rn", F.row_number().over(window_spec)) 

    clean_df = flagged_df.filter(F.col("_rn") == 1).drop("_rn") 

    rejected_df = (
            flagged_df.filter(F.col("_rn") > 1)
                    .withColumn("reject_reason", F.lit(f"duplicate_on_{'_'.join(partition_cols)}")) 
                    .withColumn("rejected_at", F.current_timestamp())
                    .drop("_rn")
    )
    return clean_df, rejected_df 
