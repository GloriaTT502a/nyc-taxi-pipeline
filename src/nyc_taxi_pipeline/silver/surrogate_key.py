import pyspark.sql.functions as F 
from pyspark.sql import DataFrame 


from nyc_taxi_pipeline.metadata.business_columns import (
    COL_VENDOR_ID,
    COL_PU_DATETIME_UTC,   # 🌟 必须使用统一后的 UTC 时间保证主键幂等性
    COL_DO_DATETIME_UTC,
    COL_PU_LOCATION_ID,
    COL_DO_LOCATION_ID,
    COL_PASSENGER_COUNT,
    COL_TRIP_DISTANCE,
    COL_TOTAL_AMOUNT,
    COL_PU_LAT, COL_PU_LNG,
    COL_DO_LAT, COL_DO_LNG,
    COL_TRIP_KEY
)


def generate_trip_key(df: DataFrame) -> DataFrame: 
    
    """
    生成业务代理主键 (Surrogate Key)
    
    架构特性：
    1. 幂等性：基于 UTC 标准时间与核心业务要素进行 SHA-256 哈希。
    2. 历史兼容：动态兼容 NYC Taxi 新老数据模式 (Location ID 优先，经纬度兜底)。
    3. 防御性：处理了 NULL 值污染 Hash 算法的风险。
    """ 
    
    # Generate surrogate key based on latitude and longitude for 2010 and Location ID for others 
    pickup_loc = F.coalesce(
        F.col(COL_PU_LOCATION_ID).cast("string"), 
        F.concat_ws("_", F.col(COL_PU_LAT).cast("string"), F.col(COL_PU_LNG).cast("string"))
    )

    dropoff_loc = F.coalesce(
        F.col(COL_DO_LOCATION_ID).cast("string"), 
        F.concat_ws("_", F.col(COL_DO_LAT).cast("string"), F.col(COL_DO_LNG).cast("string"))
    )

    # Identity Columns 
    core_business_cols = [
        F.col(COL_VENDOR_ID).cast("string"),
        F.col(COL_PU_DATETIME_UTC).cast("string"), # 🌟 强依赖 UTC
        F.col(COL_DO_DATETIME_UTC).cast("string"),
        F.col(COL_PASSENGER_COUNT).cast("string"),
        F.col(COL_TRIP_DISTANCE).cast("string"),
        F.col(COL_TOTAL_AMOUNT).cast("string")
    ] 

    all_factors = [*core_business_cols, pickup_loc, dropoff_loc]

    normalized_cols = [F.coalesce(c, F.lit("NULL")) for c in all_factors] 

    # hash SHA-256 
    return df.withColumn(
        COL_TRIP_KEY, 
        F.sha2(F.concat_ws("||", *normalized_cols), 256)
    )
