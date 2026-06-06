import logging 
from delta.tables import DeltaTable 
from pyspark.sql import DataFrame, SparkSession 

logger = logging.getLogger(__name__) 

class SilverDeltaWriter: 
    @staticmethod 
    def upsert(spark: SparkSession, df: DataFrame, table_name: str, partition_col: str = "YYYYMM"): 
        # Check if table exists 
        table_exists = spark.catalog.tableExists(table_name) 

        # Check if table empty 
        is_empty = True 

        if table_exists: 
            is_empty = spark.table(table_name).limit(1).count() == 0 

        # If table doesn't exist or is empty 
        if not table_exists or is_empty: 
            logger.info(f"table {table_name} is empty or doesn't exist. Process initial write") 

            (
                df.write
                    .format("delta") 
                    .mode("overwrite") 
                    .partitionBy(partition_col)
                    .saveAsTable(table_name)
            )
            return 
        
        # Dynamic Partition (DPP) 
        distinct_partitions = [row[partition_col] for row in df.select(partition_col).distinct().collect()]
        if not distinct_partitions: 
            return 
        
        partition_values_str = ", ".join([f"'{p}'" if isinstance(p, str) else str(p) for p in distinct_partitions]) 

        merge_condition = (
            f"t.{partition_col} = s.{partition_col} AND " 
            f"t.{partition_col} IN ({partition_values_str}) AND "
            f"t.trip_key = s.trip_key"
        )

        logger.info(f"Process Merge, dynamic partition: [{partition_values_str}]")
        

        df.createOrReplaceTempView("source_updates") 

        merge_query = f"""
        MERGE INTO {table_name} AS t
        USING source_updates AS s
        ON {merge_condition}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """ 
        
        spark.sql(merge_query) 

        logger.info(f"Upsert SQL executed successfully on {table_name}")
        
        