# tests/silver/test_deduplication.py
import pytest
from datetime import datetime
from pyspark.sql import Row
from nyc_taxi_pipeline.silver.deduplication import deduplicate_trips

def test_deduplicate_trips(spark):
    # 准备测试数据：包含一条重复数据（trip_1）
    data = [
        Row(trip_key="trip_1", bronze_load_timestamp=datetime(2026, 1, 1, 10, 0, 0)),
        Row(trip_key="trip_1", bronze_load_timestamp=datetime(2026, 1, 1, 11, 0, 0)), # 这条最新，应该保留
        Row(trip_key="trip_2", bronze_load_timestamp=datetime(2026, 1, 1, 0, 0, 0)),  # 唯一数据
    ]
    df = spark.createDataFrame(data)

    clean_df, rejected_df = deduplicate_trips(df)

    # 验证清洗后的数据
    clean_data = clean_df.collect()
    assert len(clean_data) == 2
    
    # 验证保留的是时间戳最新的那条
    trip_1_clean = [r for r in clean_data if r["trip_key"] == "trip_1"][0]
    assert trip_1_clean["bronze_load_timestamp"] == datetime(2026, 1, 1, 11, 0, 0)

    # 验证被拒绝的数据
    rejected_data = rejected_df.collect()
    assert len(rejected_data) == 1
    assert rejected_data[0]["trip_key"] == "trip_1"
    assert rejected_data[0]["reject_reason"] == "duplicate_trip_key"
    assert "rejected_at" in rejected_df.columns
    