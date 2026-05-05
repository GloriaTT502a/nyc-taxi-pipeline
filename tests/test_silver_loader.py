import pytest
from pyspark.sql.types import *
from datetime import datetime
import pyspark.sql.functions as F

from nyc_taxi_pipeline.silver.nyc_taxi_silver import NYC_Taxi_Silver_Loader

import os
import pytest

@pytest.fixture(scope="session")
def spark():
    """
    工业级终极自适应 Spark Session：
    涵盖 Notebook、Terminal 和 本地 三大环境，自动切换。
    """
    # 🌟 尝试 1：Databricks Notebook 原生环境 (白嫖策略：直接借用全局 spark)
    try:
        from databricks.sdk.runtime import spark as db_spark
        if db_spark is not None:
            print("\n[Init] Detected Databricks Notebook. Borrowing existing Spark engine...")
            return db_spark
    except ImportError:
        pass

    # 🌟 尝试 2：Databricks Terminal 终端或本地 Connect 环境
    try:
        from databricks.connect import DatabricksSession
        print("\n[Init] Using Databricks Connect...")
        cluster_id = os.environ.get("DATABRICKS_CLUSTER_ID")
        builder = DatabricksSession.builder
        if cluster_id:
            builder = builder.clusterId(cluster_id)
        else:
            builder = builder.serverless()
        return builder.getOrCreate()
        
    except ImportError:
        # 🌟 尝试 3：彻底的本地离线电脑环境
        from pyspark.sql import SparkSession
        print("\n[Init] Detected Local Environment. Using local SparkSession...")
        return SparkSession.builder \
            .master("local[1]") \
            .appName("unit-tests") \
            .getOrCreate()

def test_all_dq_rules(spark):
    """工业级全覆盖测试矩阵：测试所有边界条件和 DQ 规则"""
    
    schema = StructType([
        StructField("vendor_id", StringType(), True),
        StructField("pickup_datetime", TimestampType(), True),
        StructField("dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("YYYYMM", IntegerType(), True),
        StructField("_run_id", StringType(), True)
    ])

    # 基准完美数据 (用于对比)
    # 行驶 30 分钟 (10:00-10:30)，距离 5.0，车费 15.0，效率 15/30 = 0.5
    valid_pickup = datetime(2023, 1, 1, 10, 0, 0)
    valid_dropoff = datetime(2023, 1, 1, 10, 30, 0)
    
    # 🧪 毒药配方开始：(vendor_id, pickup, dropoff, passenger, fare, total, distance, YYYYMM, run_id)
    mock_data = [
        ("V1", valid_pickup, valid_dropoff, 2, 15.0, 18.0, 5.0, 202301, "test_001"),
        
        # [1] missing_pickup: pickup_datetime 为 None
        ("V1", None, valid_dropoff, 2, 15.0, 18.0, 5.0, 202301, "test_001"),
        
        # [2] missing_dropoff: dropoff_datetime 为 None
        ("V1", valid_pickup, None, 2, 15.0, 18.0, 5.0, 202301, "test_001"),
        
        # [3] dropoff_before_pickup: 下车(10:00)比上车(11:00)早
        ("V1", datetime(2023, 1, 1, 11, 0, 0), datetime(2023, 1, 1, 10, 0, 0), 2, 15.0, 18.0, 5.0, 202301, "test_001"),
        
        # [4] passenger_count_invalid (过大): > 9
        ("V1", valid_pickup, valid_dropoff, 10, 15.0, 18.0, 5.0, 202301, "test_001"),
        
        # [5] total_amount_negative: < 0
        ("V1", valid_pickup, valid_dropoff, 2, 15.0, -1.0, 5.0, 202301, "test_001"),
        
        # [6] invalid_YYYYMM (过小): < 190001
        ("V1", valid_pickup, valid_dropoff, 2, 15.0, 18.0, 5.0, 189912, "test_001"),
        
        # [7] duration_out_of_range (太短): 1 分钟 (< 2.0)
        ("V1", valid_pickup, datetime(2023, 1, 1, 10, 1, 0), 2, 15.0, 18.0, 5.0, 202301, "test_001"),
        
        # [8] duration_out_of_range (太长): 4 小时 (> 180)
        ("V1", valid_pickup, datetime(2023, 1, 1, 14, 0, 0), 2, 15.0, 18.0, 5.0, 202301, "test_001"),
        
        # [9] distance_too_small: <= 0.1
        ("V1", valid_pickup, valid_dropoff, 2, 15.0, 18.0, 0.05, 202301, "test_001"),
        
        # [10] efficiency_too_high: 行驶10分钟，收费200块 (200/10 = 20 >= 15.0)
        ("V1", valid_pickup, datetime(2023, 1, 1, 10, 10, 0), 2, 200.0, 210.0, 5.0, 202301, "test_001"),
        
        # [11] fare_too_low: <= 2.5
        ("V1", valid_pickup, valid_dropoff, 2, 2.0, 2.0, 5.0, 202301, "test_001")
    ]
    
    poisoned_df = spark.createDataFrame(mock_data, schema)
    
    # 模拟环境补齐源列
    loader = NYC_Taxi_Silver_Loader(spark=spark, run_id="test_dq_all", target_table="dummy")
    for col_name in loader.EXPECTED_BRONZE_COLS:
        if col_name not in poisoned_df.columns:
            poisoned_df = poisoned_df.withColumn(col_name, F.lit(None))

    # 执行引擎
    enriched_df = loader.apply_transformations(poisoned_df)
    result_df = loader.apply_dq_rules(enriched_df)
    results = result_df.collect()

    # ---------------- 严密的 Assert 质检环节 ----------------
    
    # [0] 完美数据必须放行
    assert results[0]["is_valid"] == True
    assert len(results[0]["violated_rules"]) == 0

    # [1] 测试 missing_pickup
    assert "missing_pickup" in results[1]["violated_rules"]

    # [2] 测试 missing_dropoff
    assert "missing_dropoff" in results[2]["violated_rules"]

    # [3] 测试 dropoff_before_pickup (顺带还会触发 duration_out_of_range，因为时间是负的)
    assert "dropoff_before_pickup" in results[3]["violated_rules"]

    # [4] 测试 passenger_count_invalid
    assert "passenger_count_invalid" in results[4]["violated_rules"]

    # [5] 测试 total_amount_negative
    assert "total_amount_negative" in results[5]["violated_rules"]

    # [6] 测试 invalid_YYYYMM
    assert "invalid_YYYYMM" in results[6]["violated_rules"]

    # [7] 测试 duration 太短
    assert "duration_out_of_range" in results[7]["violated_rules"]

    # [8] 测试 duration 太长
    assert "duration_out_of_range" in results[8]["violated_rules"]

    # [9] 测试 distance_too_small
    assert "distance_too_small" in results[9]["violated_rules"]

    # [10] 测试 efficiency_too_high
    assert "efficiency_too_high" in results[10]["violated_rules"]

    # [11] 测试 fare_too_low
    assert "fare_too_low" in results[11]["violated_rules"]
    
    print("All rules tests passed!")