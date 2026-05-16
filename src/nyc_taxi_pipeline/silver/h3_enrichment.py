import os
import logging
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
from nyc_taxi_pipeline.config.settings import H3_RESOLUTION 

# 🌟 核心开关：默认开启 Pandas UDF 确保线上性能；但允许通过环境变量在本地测试中安全降级
USE_PANDAS_UDF = os.environ.get("USE_PANDAS_UDF", "true").lower() == "true"

logger = logging.getLogger(__name__)


# ==========================================
# 工业级双引擎 UDF 工厂：动态切换执行策略
# ==========================================
def create_latlng_to_h3_udf(resolution: int):
    """
    根据运行环境，动态返回高性能 Pandas UDF 或是高稳定性标准 UDF。
    """
    import h3  # 延迟导入，防止分布式集群 Worker 节点冷启动冲突

    # 1. 生产环境/线上集群使用：向量化 Pandas UDF (强依赖 Apache Arrow)
    if USE_PANDAS_UDF:
        @F.pandas_udf(StringType())
        def h3_pandas_engine(lat_series: pd.Series, lng_series: pd.Series) -> pd.Series:
            def get_h3(x, y):
                try:
                    if pd.isnull(x) or pd.isnull(y):
                        return None
                    # 兼容 h3-py 不同版本的 API 变动
                    if hasattr(h3, 'latlng_to_cell'):
                        return h3.latlng_to_cell(x, y, resolution)
                    else:
                        return h3.geo_to_h3(x, y, resolution)
                except Exception:
                    return None
            return lat_series.combine(lng_series, get_h3)
        return h3_pandas_engine

    # 2. 本地测试/CI 流程使用：标准行级 UDF (100% 绕过 Arrow，免疫 Java 21+ 内存冲突)
    else:
        @F.udf(StringType())
        def h3_python_engine(lat: float, lng: float) -> str:
            try:
                if lat is None or lng is None:
                    return None
                if hasattr(h3, 'latlng_to_cell'):
                    return h3.latlng_to_cell(lat, lng, resolution)
                else:
                    return h3.geo_to_h3(lat, lng, resolution)
            except Exception:
                return None
        return h3_python_engine


# ==========================================
# 核心逻辑：带去重缓存与监控指标的缝合算子
# ==========================================
def enrich_h3_cells(fact_df: DataFrame, zone_dim_df: DataFrame, resolution: int = H3_RESOLUTION) -> DataFrame:
    engine_name = "Pandas UDF (Arrow)" if USE_PANDAS_UDF else "Standard UDF (Pickle)"
    logger.info(f"开始执行 H3 空间缝合计算 (Engine: {engine_name}, Resolution: {resolution})...")
    
    # 动态工厂实例化最匹配当前环境的 UDF 引擎
    h3_udf = create_latlng_to_h3_udf(resolution)

    # ---------------------------------------------------------
    # 步骤一：处理上车地点 (Pickup) - 维持你原本的极速去重缓存逻辑
    # ---------------------------------------------------------
    df_step1_pu = fact_df.join(
        F.broadcast(
            zone_dim_df.select(
                F.col("LocationID").alias("PU_LocID"), 
                F.col("h3_cell").alias("dim_h3_pickup")
            )
        ),
        fact_df["PULocationID"] == F.col("PU_LocID"),
        "left"
    ).drop("PU_LocID")

    distinct_pu_coords = (
        df_step1_pu.filter(F.col("dim_h3_pickup").isNull() & F.col("pickup_latitude").isNotNull())
        .select("pickup_latitude", "pickup_longitude")
        .distinct()
    )
    
    calc_pu_df = distinct_pu_coords.withColumn(
        "calc_h3_pickup", 
        h3_udf(F.col("pickup_latitude"), F.col("pickup_longitude"))
    )

    df_with_pickup = (
        df_step1_pu.join(
            calc_pu_df, 
            ["pickup_latitude", "pickup_longitude"], 
            "left"
        )
        .withColumn(
            "h3_pickup",
            F.coalesce(F.col("dim_h3_pickup"), F.col("calc_h3_pickup"))
        )
        .withColumn(
            "is_pickup_fallback",
            F.when(F.col("dim_h3_pickup").isNotNull(), F.lit(0)).otherwise(F.lit(1))
        )
        .drop("dim_h3_pickup", "calc_h3_pickup")
    )

    # ---------------------------------------------------------
    # 步骤二：处理下车地点 (Dropoff) - 维持你原本的极速去重缓存逻辑
    # ---------------------------------------------------------
    df_step1_do = df_with_pickup.join(
        F.broadcast(
            zone_dim_df.select(
                F.col("LocationID").alias("DO_LocID"), 
                F.col("h3_cell").alias("dim_h3_dropoff")
            )
        ),
        df_with_pickup["DOLocationID"] == F.col("DO_LocID"),
        "left"
    ).drop("DO_LocID")

    distinct_do_coords = (
        df_step1_do.filter(F.col("dim_h3_dropoff").isNull() & F.col("dropoff_latitude").isNotNull())
        .select("dropoff_latitude", "dropoff_longitude")
        .distinct()
    )
    
    calc_do_df = distinct_do_coords.withColumn(
        "calc_h3_dropoff", 
        h3_udf(F.col("dropoff_latitude"), F.col("dropoff_longitude"))
    )

    df_final = (
        df_step1_do.join(
            calc_do_df, 
            ["dropoff_latitude", "dropoff_longitude"], 
            "left"
        )
        .withColumn(
            "h3_dropoff",
            F.coalesce(F.col("dim_h3_dropoff"), F.col("calc_h3_dropoff"))
        )
        .withColumn(
            "is_dropoff_fallback",
            F.when(F.col("dim_h3_dropoff").isNotNull(), F.lit(0)).otherwise(F.lit(1))
        )
        .drop("dim_h3_dropoff", "calc_h3_dropoff")
    )

    logger.info("H3 空间维度去重计算与缝合完毕。")
    return df_final