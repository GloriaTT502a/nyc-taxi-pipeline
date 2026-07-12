# src/nyc_taxi_pipeline/spatial/zone_lookup.py

import pandas as pd
import shapefile  # pyshp
from shapely.geometry import shape
from shapely.ops import transform
from pyproj import Transformer
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from nyc_taxi_pipeline.observability.app_logging import get_logger
from nyc_taxi_pipeline.config.settings import PipelineSettings
from nyc_taxi_pipeline.config.tables import Table, get_table_name
from nyc_taxi_pipeline.observability.metrics import PipelineAuditor 
from nyc_taxi_pipeline.contracts.dim_zone_schema import (
    ZONE_LOCATION_ID_COL, 
    ZONE_H3_CELL_COL
)


logger = get_logger(__name__) 

def process_raw_taxi_zone(shp_path: str) -> pd.DataFrame: 
    """
    Serverless-Safe: Reads shapefile, transforms CRS to EPSG:4326 (Lat/Lng), 
    and extracts standard WKT string.
    """
    logger.info("Transforming CRS from EPSG:2263 (State Plane) to EPSG:4326 (WGS84 Lat/Lng)...")
    
    sf = shapefile.Reader(shp_path)
    fields = [f[0] for f in sf.fields[1:]]
    
    # 建立投影转换器 (2263 -> 4326)
    transformer = Transformer.from_crs("EPSG:2263", "EPSG:4326", always_xy=True)
    
    records = []
    
    for sr in sf.shapeRecords():
        attrs = dict(zip(fields, sr.record))
        # 1. 读取原生长岛平面坐标的多边形 (EPSG:2263)
        geom_2263 = shape(sr.shape.__geo_interface__)
        
        # 2. 核心修复：转换整个多边形的所有顶点到经纬度 (EPSG:4326)
        geom_4326 = transform(transformer.transform, geom_2263)
        
        records.append({
            "LocationID": int(attrs.get("LocationID", 0)),
            "borough": str(attrs.get("borough", "")),
            "zone": str(attrs.get("zone", "")),
            # 此时的 WKT 就是完美标准的 Lat/Lng 多边形了！
            "raw_boundary_wkt": geom_4326.wkt 
        })
        
    return pd.DataFrame(records)  


def build_spatial_tables(spark: SparkSession, settings: PipelineSettings, auditor: PipelineAuditor, run_id: str):
    """
    Extracts shapefile and loads it as a Bronze staging table for dbt.
    """
    shp_path = settings.shp_path
    resolution = settings.h3_resolution 

    # 注意：我们现在将目标表改为 Bronze/Staging 层的表
    staging_target_table = get_table_name(settings, Table.STG_TAXI_ZONES_RAW) 
    silver_target_table = get_table_name(settings, Table.SILVER_ZONE_H3)

    # ==========================================
    # 阶段 1：Extract & Load (提取并落盘 Bronze)
    # ==========================================
    logger.info("Starting Spatial Extraction (Python Load -> Bronze)") 
    pdf_raw = process_raw_taxi_zone(shp_path)
    raw_df = spark.createDataFrame(pdf_raw)
    
    logger.info(f"Writing raw WKT to {staging_target_table}...")
    raw_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(staging_target_table) 

    # ==========================================
    # 阶段 2：Transform (计算 H3 并落盘 Silver)
    # ==========================================
    logger.info("Executing Spatial Transformation to compute H3 indices...")
    
    if settings.runtime_env == "local":
        # 本地开源 PySpark 环境没有 Databricks 专有空间函数，使用 Mock 数据保证运行不报错
        logger.warning("[Local Dev Mode] Bypassing native spatial functions, generating mock H3 cells.")
        silver_zone_df = raw_df.select(
            F.col("LocationID").alias(ZONE_LOCATION_ID_COL),
            F.col("borough"),
            F.col("zone"),
            F.col("raw_boundary_wkt"),
            F.lit(None).cast("string").alias(ZONE_H3_CELL_COL)
        )
    else:
        # Databricks 集群环境：调用底层原生 C++ 空间函数
        # st_geomfromwkt -> 转为 Geometry 对象
        # st_centroid -> 获取区域中心点
        # st_x / st_y -> 提取中心点的经度(Lng)和纬度(Lat)
        # h3_longlatash3string -> 计算 H3 字符串
        h3_sql_expr = f"""
            h3_longlatash3string(
                st_x(st_centroid(st_geomfromwkt(raw_boundary_wkt))), 
                st_y(st_centroid(st_geomfromwkt(raw_boundary_wkt))), 
                {resolution}
            )
        """
        
        silver_zone_df = raw_df.selectExpr(
            f"LocationID as {ZONE_LOCATION_ID_COL}",
            "borough",
            "zone",
            "raw_boundary_wkt",
            f"{h3_sql_expr} as {ZONE_H3_CELL_COL}"
        )

    logger.info(f"Writing enriched spatial reference data to {silver_target_table}...")
    silver_zone_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(silver_target_table)

    # ==========================================
    # 阶段 3：自动化遥测打点
    # ==========================================
    auditor.log_run_metrics(
        run_id=run_id,
        layer="Bronze/Silver_Spatial",
        target_table=silver_target_table,
        valid_count=len(pdf_raw),
        rejected_count=0
    )
    logger.info("Spatial data pipeline completed successfully.") 

    
