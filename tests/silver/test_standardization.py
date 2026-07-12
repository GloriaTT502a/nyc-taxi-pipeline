import pytest
from datetime import datetime
from pyspark.sql import Row
from unittest.mock import patch
import pyspark.sql.functions as F
from nyc_taxi_pipeline.silver.standardization import ensure_bronze_schema, apply_transformations, apply_dq_and_split
from nyc_taxi_pipeline.config.settings import PipelineSettings
from nyc_taxi_pipeline.metadata.system_columns import SILVER_RUN_ID_COLUMN
from nyc_taxi_pipeline.metadata.business_columns import COL_FARE_PER_MINUTE

@pytest.fixture
def test_settings():
    """提供通用的测试配置对象"""
    return PipelineSettings(
        runtime_env="local",
        catalog="nyc",
        base_path="/tmp/test",
        shp_path="/tmp/test/zone.shp",
        bronze_db="bronze",
        silver_db="silver",
        gold_db="gold",
        timezone="America/New_York"
    )

def test_apply_transformations_summer_time(spark, test_settings):
    """测试夏令时 (EDT: UTC-4) 的转换"""
    df = spark.createDataFrame([
        Row(pickup_datetime=datetime(2026, 7, 1, 10, 0, 0), 
            dropoff_datetime=datetime(2026, 7, 1, 10, 30, 0), 
            fare_amount=30.0, _run_id="old_id", _load_timestamp=datetime.now(), total_amount=35.0, YYYYMM=202607)
    ]) 

    aligned_df = ensure_bronze_schema(df)
    res_df = apply_transformations(aligned_df, run_id="new_run_123", settings=test_settings)
    row = res_df.collect()[0]
    
    assert row["pickup_datetime_utc"] == datetime(2026, 7, 1, 14, 0, 0)
    assert row["dropoff_datetime_utc"] == datetime(2026, 7, 1, 14, 30, 0)
    assert row["duration_min"] == 30.0


def test_ensure_bronze_schema_missing_required(spark):
    """测试缺失必填列时的异常抛出"""
    df = spark.createDataFrame([Row(vendor_id="1", YYYYMM=202601)])
    with pytest.raises(ValueError, match="致命错误: 缺少必填列 total_amount"):
        ensure_bronze_schema(df)

def test_ensure_bronze_schema_fill_optional(spark):
    """测试非必填列缺失时，是否自动补全"""
    df = spark.createDataFrame([Row(total_amount=10.5, YYYYMM=202601)])
    res_df = ensure_bronze_schema(df)
    
    assert "passenger_count" in res_df.columns
    assert res_df.schema["passenger_count"].dataType.simpleString() == "bigint"

def test_apply_transformations(spark, test_settings):
    """测试基础转换逻辑"""
    df = spark.createDataFrame([
        Row(pickup_datetime=datetime(2026, 1, 1, 10, 0, 0), 
            dropoff_datetime=datetime(2026, 1, 1, 10, 30, 0), 
            fare_amount=30.0, _run_id="old_id", _load_timestamp=datetime.now(), total_amount=35.0, YYYYMM=202601)
    ])
    
    aligned_df = ensure_bronze_schema(df)
    res_df = apply_transformations(aligned_df, run_id="new_run_123", settings=test_settings)
    row = res_df.collect()[0]
    
    assert row["pickup_datetime_utc"] == datetime(2026, 1, 1, 15, 0, 0)
    assert row["dropoff_datetime_utc"] == datetime(2026, 1, 1, 15, 30, 0)
    
    assert row["duration_min"] == 30.0
    # 修复：使用业务列常量名
    assert row[COL_FARE_PER_MINUTE] == 1.0
    # 修复：使用系统列常量名 _silver_run_id
    assert row[SILVER_RUN_ID_COLUMN] == "new_run_123"

@patch("nyc_taxi_pipeline.silver.standardization.get_silver_dq_rules")
def test_apply_dq_and_split(mock_get_rules, spark, test_settings):
    """测试 DQ 规则的分流逻辑"""
    mock_get_rules.return_value = {"duration_must_gt_zero": F.col("duration_min") > 0}
    
    data = [
        Row(duration_min=15.0, fare_amount=10.0), # 合法
        Row(duration_min=-5.0, fare_amount=10.0)  # 违规
    ]
    df = spark.createDataFrame(data)
    
    valid_df, rejected_df = apply_dq_and_split(df, settings=test_settings)
    
    assert valid_df.count() == 1
    assert rejected_df.count() == 1
    
    rejected_row = rejected_df.collect()[0]
    assert "duration_must_gt_zero" in rejected_row["violated_rules"]
    assert rejected_row["is_valid"] is False