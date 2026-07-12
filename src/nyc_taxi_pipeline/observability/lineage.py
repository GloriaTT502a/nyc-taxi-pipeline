import pyspark.sql.functions as F 
from pyspark.sql import DataFrame 
from nyc_taxi_pipeline.metadata.system_columns import (
    BRONZE_RUN_ID_COLUMN, 
    INPUT_FILE_COLUMN, 
    BRONZE_LOAD_TIMESTAMP_COLUMN 
) 

def inject_bronze_audit_columns(df: DataFrame, run_id: str) -> DataFrame: 
    return (
        df.withColumn(BRONZE_RUN_ID_COLUMN, F.lit(run_id)) 
          .withColumn(BRONZE_LOAD_TIMESTAMP_COLUMN, F.current_timestamp()) 
    )
