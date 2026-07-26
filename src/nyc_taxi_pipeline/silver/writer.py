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
    def overwrite_month(
        spark: SparkSession, 
        df: DataFrame, 
        table_name: str, 
        target_month: str 
        ) -> None: 
        # 1. Handle initial write for new tables
        if not spark.catalog.tableExists(table_name):
            logger.info(f"Target table {table_name} does not exist, performing initial full write...")
            (
                df.write
                .format("delta")
                .mode("overwrite")
                .partitionBy("YYYYMM")
                .saveAsTable(table_name)
            )
            return

        # 2. 如果表已存在，使用 replaceWhere 进行幂等覆写
        try:
            logger.info(f"Run replaceWhere: {table_name} | 月份: {target_month}")
            
            # replaceWhere 会指示 Delta 引擎：
            # "找到并物理删除该表中 YYYYMM = target_month 的所有旧数据，并将 df 中的新数据写入"
            (
                df.write
                .format("delta")
                .mode("overwrite")
                .option("replaceWhere", f"YYYYMM = '{target_month}'")
                .saveAsTable(table_name)
            )
            
            logger.info("replaceWhere successful")
            
        except Exception as e:
            logger.error(f"replaceWhere failed! 可能是 Schema 发生不兼容变更 (Drift) 或数据约束冲突: {e}")
            raise e   
        