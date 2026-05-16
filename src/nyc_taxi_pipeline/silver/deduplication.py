import pyspark.sql.functions as F 
from pyspark.sql import DataFrame 
from pyspark.sql.window import Window 

def deduplicate_trips(df: DataFrame) -> tuple[DataFrame, DataFrame]: 
    window_spec = Window.partitionBy("trip_key").orderBy(F.col("bronze_load_timestamp").desc_nulls_last()) 

    flagged_df = df.withColumn("_rn", F.row_number().over(window_spec)) 

    clean_df = flagged_df.filter(F.col("_rn") == 1).drop("_rn") 

    rejected_df = (
            flagged_df.filter(F.col("_rn") > 1)
                    .withColumn("reject_reason", F.lit("duplicate_trip_key")) 
                    .withColumn("rejected_at", F.current_timestamp())
    )
    return clean_df, rejected_df 
