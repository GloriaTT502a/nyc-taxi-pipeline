# tests/silver/test_surrogate_key.py
import pytest
from datetime import datetime
from pyspark.sql import Row
# 🌟 引入显式 Schema 类型定义组件
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType
from nyc_taxi_pipeline.silver.surrogate_key import generate_trip_key

def test_generate_trip_key(spark):
    """
    验证代理键（Surrogate Key）生成的确定性与哈希长度规格。
    显式声明 Schema 以规避全 None 字段导致的 PySpark 类型推导失败。
    """
    
    # 1. 🌟 工业级最佳实践：显式定义元数据契约（Schema）
    # 即使测试数据中包含大量 None，Spark 也能通过此配置直接在内存中完成对齐
    test_schema = StructType([
        StructField("vendor_id", StringType(), True),
        StructField("pickup_datetime", TimestampType(), True),
        StructField("dropoff_datetime", TimestampType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        # 显式将经纬度锁定为 DoubleType，完美容忍 None 值的注入
        StructField("pickup_latitude", DoubleType(), True),
        StructField("pickup_longitude", DoubleType(), True),
        StructField("dropoff_latitude", DoubleType(), True),
        StructField("dropoff_longitude", DoubleType(), True)
    ])

    # 2. 构建包含完全相同业务核心字段的测试数据集
    data = [
        Row(vendor_id="1", pickup_datetime=datetime(2026, 5, 1), dropoff_datetime=datetime(2026, 5, 1),
            PULocationID=10, DOLocationID=20, passenger_count=1, trip_distance=2.5, total_amount=15.0,
            pickup_latitude=None, pickup_longitude=None, dropoff_latitude=None, dropoff_longitude=None),
        Row(vendor_id="1", pickup_datetime=datetime(2026, 5, 1), dropoff_datetime=datetime(2026, 5, 1),
            PULocationID=10, DOLocationID=20, passenger_count=1, trip_distance=2.5, total_amount=15.0,
            pickup_latitude=None, pickup_longitude=None, dropoff_latitude=None, dropoff_longitude=None)
    ]
    
    # 3. 🌟 实例化 DataFrame 时显式挂载刚刚定义好的 schema
    df = spark.createDataFrame(data, schema=test_schema)
    
    # 4. 调用 Silver 层的代理键生成算子
    res_df = generate_trip_key(df)
    rows = res_df.collect()
    
    # 5. 严格验证断言
    # 验证生成的 trip_key 是确定性的（相同的输入必须产生绝对相同的哈希键）
    assert rows[0]["trip_key"] == rows[1]["trip_key"], "❌ 错误：相同输入产生的代理键不一致！"
    
    # 验证生成的哈希符合标准规格（SHA-256 算法生成的十六进制字符串固定为 64 位）
    assert len(rows[0]["trip_key"]) == 64, f"❌ 错误：生成的 trip_key 长度为 {len(rows[0]['trip_key'])}，不符合标准 SHA-256 的 64 位规格！"
    