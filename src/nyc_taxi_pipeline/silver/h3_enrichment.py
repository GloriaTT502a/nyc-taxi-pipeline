import logging
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from nyc_taxi_pipeline.config.settings import PipelineSettings 
from nyc_taxi_pipeline.observability.metric_keys import METRIC_PU_FALLBACK, METRIC_DO_FALLBACK

from nyc_taxi_pipeline.contracts.dim_zone_schema import (
    ZONE_LOCATION_ID_COL, 
    ZONE_H3_CELL_COL, 
    ZONE_WKT_COL 
)

from nyc_taxi_pipeline.metadata.business_columns import (
    COL_PU_LOCATION_ID, COL_DO_LOCATION_ID,
    COL_PU_LAT, COL_PU_LNG, 
    COL_DO_LAT, COL_DO_LNG,
    COL_H3_PU, COL_H3_DO
)

logger = logging.getLogger(__name__)


def _enrich_single_point(
    trip_df: DataFrame, zone_df: DataFrame,
    loc_col: str, lng_col: str, lat_col: str,
    target_h3_col: str, metric_col: str, resolution: int, 
    is_local_env: bool 
) -> DataFrame:
    """
    极简流式匹配：先修补缺失的 LocationID，再统一 Join 获取 H3。
    """
    # === 新增：强制给维度表打上 Broadcast Hint ===
    # 这是比 F.broadcast() 更底层的一种提示方式，适用于 Spark Connect
    
    zone_df_hinted = zone_df.hint("broadcast") 


    # ==========================================
    # 步骤 1: 空间反查补齐 LocationID (按需触发)
    # 你的逻辑: "如果为 null, 直接将两个表 join; 如果不为 null, 用以前的数据"
    # ==========================================
    if not is_local_env: 
        spatial_dim = zone_df_hinted.select(
            F.col(ZONE_LOCATION_ID_COL).alias("spatial_loc_id"),
            F.col(ZONE_WKT_COL).alias("wkt")
        )
    
        # 条件：原本没 ID，且经纬度在多边形内
        spatial_cond = F.expr(
            f"""
            (YYYY == 2010) 
            AND {loc_col} IS NULL
            AND CAST({lng_col} AS DOUBLE) <> 0
            AND CAST({lat_col} AS DOUBLE) <> 0
            AND st_intersects(
                st_geomfromwkt(wkt),
                st_point(
                    CAST({lng_col} AS DOUBLE),
                    CAST({lat_col} AS DOUBLE)
                )
            )
            """
        )
    
        trip_df = trip_df.join(spatial_dim, spatial_cond, "left")
        
        # 新增安全层：防止由于边界重叠导致一个点匹配到多个 Zone 从而引发数据膨胀
        # 注意：这里去掉了 _silver_run_id，避免所有数据跑到同一个分区导致全局倾斜
        # 如果你的表有唯一的主键 (比如 trip_key)，应该加上主键；这里用时间+空间坐标作为分区标识
        window_spec = Window.partitionBy("pickup_datetime", lng_col, lat_col).orderBy("spatial_loc_id")
        
        trip_df = (
            trip_df
            .withColumn("_rn", F.row_number().over(window_spec))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
        # 安全层结束
        
        trip_df = trip_df.withColumn(loc_col, F.coalesce(F.col(loc_col), F.col("spatial_loc_id")))
        trip_df = trip_df.drop("spatial_loc_id", "wkt")
    else:
        logger.warning(f"[Local Dev] 环境未开启 Databricks Native Engine，跳过 {loc_col} 空间反查。") 


    # ==========================================
    # 步骤 2: 等值关联获取 H3 (大道至简)
    # 你的逻辑: "拿到 location id 对应的 h3_cell 就行"
    # ==========================================
    h3_dim = zone_df_hinted.select(
        F.col(ZONE_LOCATION_ID_COL).alias("h3_loc_id"),
        F.col(ZONE_H3_CELL_COL).alias("dim_h3")
    )
    
    # 拿着刚才修补好的 LocationID，直接极速等值 Join
    trip_df = trip_df.join(h3_dim, F.col(loc_col).cast("int") == F.col("h3_loc_id").cast("int"), "left")

    if not is_local_env:
        native_h3_fallback = F.when(
            F.col(lng_col).isNotNull() & F.col(lat_col).isNotNull(),
            F.expr(f"h3_longlatash3string(CAST({lng_col} AS DOUBLE), CAST({lat_col} AS DOUBLE), {resolution})")
        ).otherwise(F.lit(None).cast("string"))
    else:
        native_h3_fallback = F.lit("mock_fallback_h3").cast("string")

    trip_df = (
        trip_df
        .withColumn(target_h3_col, F.coalesce(F.col("dim_h3"), native_h3_fallback))
        # 统计指标：只要是从维度表里没拿到的（dim_h3 is null），统统计入 fallback metric
        .withColumn(metric_col, F.when(F.col("dim_h3").isNotNull(), 0).otherwise(1))
        .drop("h3_loc_id", "dim_h3")
    )

    return trip_df


# ==========================================
# 核心流水线入口
# ==========================================
def enrich_h3_cells(
    fact_df: DataFrame, 
    zone_dim_df: DataFrame, 
    settings: PipelineSettings,
    _silver_run_id: str 
) -> DataFrame:
    resolution = settings.h3_resolution 
    is_local = (settings.runtime_env == "local")
    
    logger.info(f"[RunID: {_silver_run_id}] 开始执行流式 H3 缝合 (Resolution: {resolution})...")
    
    df_pu = _enrich_single_point(
        fact_df, zone_dim_df, COL_PU_LOCATION_ID, COL_PU_LNG, COL_PU_LAT,
        COL_H3_PU, METRIC_PU_FALLBACK, resolution, is_local
    )

    df_final = _enrich_single_point(
        df_pu, zone_dim_df, COL_DO_LOCATION_ID, COL_DO_LNG, COL_DO_LAT,
        COL_H3_DO, METRIC_DO_FALLBACK, resolution, is_local
    )
    
    logger.info(f"[RunID: {_silver_run_id}] 空间维度缝合完毕。")
    return df_final 