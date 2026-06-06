# tests/silver/test_standardization.py
import pytest
from datetime import datetime
from pyspark.sql import Row
from unittest.mock import patch
import pyspark.sql.functions as F
from nyc_taxi_pipeline.silver.standardization import ensure_bronze_schema, apply_transformations, apply_dq_and_split


def test_apply_transformations_summer_time(spark):
    """测试夏令时 (EDT: UTC-4) 的转换（防止时间拨号翻车）"""
    df = spark.createDataFrame([
        # 纽约时间：2026年7月1日 (夏令时)
        Row(pickup_datetime=datetime(2026, 7, 1, 10, 0, 0), 
            dropoff_datetime=datetime(2026, 7, 1, 10, 30, 0), 
            fare_amount=30.0, _run_id="old_id", _load_timestamp=datetime.now(), total_amount=35.0, YYYYMM=202607)
    ]) 

    aligned_df = ensure_bronze_schema(df)
    res_df = apply_transformations(aligned_df, run_id="new_run_123")
    row = res_df.collect()[0]
    
    # UTC 断言：夏令时相差 4 个小时 (10:00 + 4小时 = 14:00)
    assert row["pickup_datetime_utc"] == datetime(2026, 7, 1, 14, 0, 0)
    assert row["dropoff_datetime_utc"] == datetime(2026, 7, 1, 14, 30, 0)
    assert row["duration_min"] == 30.0


def test_ensure_bronze_schema_missing_required(spark):
    # 缺少必填列 total_amount
    df = spark.createDataFrame([Row(vendor_id="1", YYYYMM=202601)])
    with pytest.raises(ValueError, match="致命错误: 缺少必填列 total_amount"):
        ensure_bronze_schema(df)

def test_ensure_bronze_schema_fill_optional(spark):
    # 提供了必填列，缺少非必填列 `passenger_count`
    df = spark.createDataFrame([Row(total_amount=10.5, YYYYMM=202601)])
    res_df = ensure_bronze_schema(df)
    
    assert "passenger_count" in res_df.columns
    assert res_df.schema["passenger_count"].dataType.simpleString() == "int"

def test_apply_transformations(spark):
    df = spark.createDataFrame([
        Row(pickup_datetime=datetime(2026, 1, 1, 10, 0, 0), 
            dropoff_datetime=datetime(2026, 1, 1, 10, 30, 0), 
            fare_amount=30.0, _run_id="old_id", _load_timestamp=datetime.now(), total_amount=35.0, YYYYMM=202601)
    ])
    
    # 为了通过 EXPECTED_BRONZE_COLS 的对齐，先做填充
    aligned_df = ensure_bronze_schema(df)
    res_df = apply_transformations(aligned_df, run_id="new_run_123")
    row = res_df.collect()[0]
    
    assert row["pickup_datetime_utc"] == datetime(2026, 1, 1, 15, 0, 0)
    assert row["dropoff_datetime_utc"] == datetime(2026, 1, 1, 15, 30, 0)
    
    assert row["duration_min"] == 30.0  # 30分钟
    assert row["temp_eff"] == 1.0       # 30.0 / 30.0 = 1.0
    assert row["_run_id"] == "new_run_123"

@patch("nyc_taxi_pipeline.silver.standardization.get_silver_dq_rules")
def test_apply_dq_and_split(mock_get_rules, spark):
    # Mock 数据质量规则：打车时间必须大于 0
    mock_get_rules.return_value = {"duration_must_gt_zero": F.col("duration_min") > 0}
    
    data = [
        Row(duration_min=15.0, fare_amount=10.0), # 合法
        Row(duration_min=-5.0, fare_amount=10.0)  # 违规
    ]
    df = spark.createDataFrame(data)
    
    valid_df, rejected_df = apply_dq_and_split(df)
    
    assert valid_df.count() == 1
    assert rejected_df.count() == 1
    
    rejected_row = rejected_df.collect()[0]
    assert "duration_must_gt_zero" in rejected_row["violated_rules"]
    assert rejected_row["is_valid"] is False