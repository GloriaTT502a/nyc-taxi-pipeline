# src/nyc_taxi_pipeline/silver/writer.py

import logging 
import uuid
from delta.tables import DeltaTable 
from pyspark.sql import DataFrame, SparkSession 

from nyc_taxi_pipeline.metadata.business_columns import COL_TRIP_KEY 

logger = logging.getLogger(__name__) 

class SilverDeltaWriter:
    """
    Industrial-grade Silver Layer Delta Writer:
    1. Uses declarative SQL Merge to adapt to Liquid Clustering physical storage.
    2. Removes all hardcoded physical partitioning, allowing Databricks 
       to automatically optimize I/O and data skipping.
    """ 
    @staticmethod 
    def upsert(spark: SparkSession, df: DataFrame, table_name: str) -> None: 
        # 1. Handle initial write for new tables
        if not spark.catalog.tableExists(table_name):
            logger.info(f"Target table {table_name} does not exist, performing initial full write...")
            (
                df.write
                .format("delta")
                .mode("overwrite")
                .saveAsTable(table_name)
            )
            return

        # 2. Prepare source data view
        # Using a hashed view name to prevent collisions during concurrent job runs
        view_name = f"source_updates_{uuid.uuid4().hex}"
        df.createOrReplaceTempView(view_name)

        # 3. Construct native SQL Merge statement
        # No manual partition calculation required; Databricks engine automatically 
        # utilizes Liquid Clustering indices for optimized data skipping.
        merge_query = f"""
            MERGE INTO {table_name} AS target
            USING {view_name} AS source
            ON target.{COL_TRIP_KEY} = source.{COL_TRIP_KEY}
            WHEN MATCHED THEN 
                UPDATE SET *
            WHEN NOT MATCHED THEN 
                INSERT *
        """

        try:
            logger.info(f"Executing native SQL Merge write: {table_name}")
            spark.sql(merge_query)
            logger.info("Upsert SQL executed successfully.")
        except Exception as e:
            logger.error(f"SQL Merge failed! Possible schema mismatch or drift: {e}")
            raise e
        finally:
            try:
                spark.catalog.dropTempView(view_name)
            except Exception:
                pass  
        