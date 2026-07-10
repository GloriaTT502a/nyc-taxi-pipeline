import os
import logging
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
from nyc_taxi_pipeline.config.settings import PipelineSettings 
from nyc_taxi_pipeline.observability.metric_keys import METRIC_PU_FALLBACK, METRIC_DO_FALLBACK


from nyc_taxi_pipeline.contracts.dim_zone_schema import (
    ZONE_LOCATION_ID_COL, 
    ZONE_H3_CELL_COL
)

from nyc_taxi_pipeline.metadata.business_columns import (
    COL_PU_LOCATION_ID, COL_DO_LOCATION_ID,
    COL_PU_LAT, COL_PU_LNG, 
    COL_DO_LAT, COL_DO_LNG,
    COL_H3_PU, COL_H3_DO
)

logger = logging.getLogger(__name__)

# ==========================================
# UDF Function
# ==========================================
def create_latlng_to_h3_udf(settings: PipelineSettings):
    import h3  # 延迟导入，防止集群 Worker 节点冷启动冲突

    resolution = settings.h3_resolution
    use_pandas = settings.runtime_env == "databricks"

    # 1. 生产环境：向量化 Pandas UDF (依赖 Apache Arrow)
    if use_pandas:
        @F.pandas_udf(StringType())
        def h3_pandas_engine(lat_series: pd.Series, lng_series: pd.Series) -> pd.Series:
            def get_h3(x, y):
                try:
                    if pd.isna(x) or pd.isna(y):
                        return None
                    if hasattr(h3, 'latlng_to_cell'):
                        return h3.latlng_to_cell(x, y, resolution)
                    return h3.geo_to_h3(x, y, resolution)
                except Exception:
                    return None
            # 列表推导式在小批量 Series 迭代中往往比 .combine 更稳健
            return pd.Series([get_h3(x, y) for x, y in zip(lat_series, lng_series)])
        return h3_pandas_engine

    # 2. 本地测试/CI：标准行级 UDF (100% 绕过 Arrow，免疫 Java 21+ 内存冲突)
    else:
        @F.udf(StringType())
        def h3_python_engine(lat: float, lng: float) -> str:
            try:
                if lat is None or lng is None:
                    return None
                if hasattr(h3, 'latlng_to_cell'):
                    return h3.latlng_to_cell(lat, lng, resolution)
                return h3.geo_to_h3(lat, lng, resolution)
            except Exception:
                return None
        return h3_python_engine


# ==========================================
# 核心逻辑：带去重缓存与监控指标的缝合算子
# ==========================================
def enrich_h3_cells(fact_df: DataFrame, zone_dim_df: DataFrame, settings: PipelineSettings) -> DataFrame:
    # 动态获取引擎策略
    use_pandas = settings.runtime_env == "databricks"
    engine_name = "Pandas UDF (Arrow)" if use_pandas else "Standard UDF (Pickle)"
    resolution = settings.h3_resolution 

    logger.info(f"🚀 开始执行 H3 空间缝合计算 (Engine: {engine_name}, Resolution: {resolution})...")
    
    h3_udf = create_latlng_to_h3_udf(settings)

    # Define temp field name 
    ALIAS_PU_LOC = "PU_LocID"
    ALIAS_DO_LOC = "DO_LocID"
    ALIAS_DIM_H3_PU = "dim_h3_pickup"
    ALIAS_DIM_H3_DO = "dim_h3_dropoff"

    # ---------------------------------------------------------
    # 步骤一：处理上车地点 (Pickup) - 极速去重缓存逻辑
    # ---------------------------------------------------------
    dim_zone_pu = zone_dim_df.select(
        F.col(ZONE_LOCATION_ID_COL).alias(ALIAS_PU_LOC), 
        F.col(ZONE_H3_CELL_COL).alias(ALIAS_DIM_H3_PU) 
    )
    
    df_step1_pu = fact_df.join(
        F.broadcast(dim_zone_pu), 
        fact_df[COL_PU_LOCATION_ID] == F.col(ALIAS_PU_LOC),
        "left"
    ).drop(ALIAS_PU_LOC)

    distinct_pu_coords = (
        df_step1_pu.filter(F.col(ALIAS_DIM_H3_PU).isNull() & F.col(COL_PU_LAT).isNotNull())
        .select(COL_PU_LAT, COL_PU_LNG) # 替换为业务常量
        .distinct()
    )
    
    calc_pu_df = distinct_pu_coords.withColumn(
        "calc_h3_pickup", 
        h3_udf(F.col(COL_PU_LAT), F.col(COL_PU_LNG)) 
    )

    df_with_pickup = (
        df_step1_pu.join(
            calc_pu_df, 
            [COL_PU_LAT, COL_PU_LNG], 
            "left"
        )
        .withColumn(
            COL_H3_PU, 
            F.coalesce(F.col(ALIAS_DIM_H3_PU), F.col("calc_h3_pickup"))
        )
       
        .withColumn(METRIC_PU_FALLBACK, F.when(F.col(ALIAS_DIM_H3_PU).isNotNull(), 0).otherwise(1))
        .drop(ALIAS_DIM_H3_PU, "calc_h3_pickup")
    )

    # ---------------------------------------------------------
    # 步骤二：处理下车地点 (Dropoff) - 极速去重缓存逻辑
    # ---------------------------------------------------------
    dim_zone_do = zone_dim_df.select(
        F.col(ZONE_LOCATION_ID_COL).alias(ALIAS_DO_LOC), 
        F.col(ZONE_H3_CELL_COL).alias(ALIAS_DIM_H3_DO)
    )
    
    df_step1_do = df_with_pickup.join(
        F.broadcast(dim_zone_do),
        df_with_pickup[COL_DO_LOCATION_ID] == F.col(ALIAS_DO_LOC),
        "left"
    ).drop(ALIAS_DO_LOC) 

    distinct_do_coords = (
        df_step1_do.filter(F.col(ALIAS_DIM_H3_DO).isNull() & F.col(COL_DO_LAT).isNotNull())
        .select(COL_DO_LAT, COL_DO_LNG) 
        .distinct()
    )
    
    calc_do_df = distinct_do_coords.withColumn(
        "calc_h3_dropoff", 
        h3_udf(F.col(COL_DO_LAT), F.col(COL_DO_LNG)) 
    )

    df_final = (
        df_step1_do.join(
            calc_do_df, 
            [COL_DO_LAT, COL_DO_LNG], 
            "left"
        )
        .withColumn(
            COL_H3_DO, 
            F.coalesce(F.col(ALIAS_DIM_H3_DO), F.col("calc_h3_dropoff"))
        )
    
        .withColumn(METRIC_DO_FALLBACK, F.when(F.col(ALIAS_DIM_H3_DO).isNotNull(), 0).otherwise(1))
        .drop(ALIAS_DIM_H3_DO, "calc_h3_dropoff")
    )

    logger.info("H3 空间维度去重计算与缝合完毕。")
    return df_final 