import uuid
import logging
import os
from datetime import datetime
from pyspark.sql import SparkSession 

# 导入你刚刚重构好的双轨制空间构建函数
from nyc_taxi_pipeline.spatial.build_zone_lookup import build_spatial_tables
from nyc_taxi_pipeline.config.settings import DIM_H3_TABLE, BRIDGE_H3_TABLE, SHP_PATH

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    logger.info("wake up Databricks computer")
    spark = SparkSession.builder.getOrCreate()

    # ==========================================
    # 1. 准备基础运行参数
    # ==========================================
    
    
    RESET_ENVIRONMENT = True 

    if RESET_ENVIRONMENT:
        logger.warning("[开发模式] 正在清理上一次的空间测试表...")
        spark.sql(f"DROP TABLE IF EXISTS {DIM_H3_TABLE}")
        spark.sql(f"DROP TABLE IF EXISTS {BRIDGE_H3_TABLE}")
        logger.info(f"{DIM_H3_TABLE} and {BRIDGE_H3_TABLE} have been dropped") 
    
    # ==========================================
    # 2. 执行空间构建流水线
    # ==========================================
    logger.info(f" Will read Shapefile: {SHP_PATH}")
    
    try:
        build_spatial_tables(
            spark=spark,
            shp_path=SHP_PATH,
            dim_target_table=DIM_H3_TABLE,
            bridge_target_table=BRIDGE_H3_TABLE
        )
        logger.info("⚡ dim table and bridge table have been built")
        logger.info(f"Please check table {DIM_H3_TABLE} and {BRIDGE_H3_TABLE} ")
        
    except FileNotFoundError:
        logger.error(f"Cannot find Shapefile: {SHP_PATH}")
        logger.error("Please confirm the location of the shapefile")

if __name__ == "__main__":
    main()