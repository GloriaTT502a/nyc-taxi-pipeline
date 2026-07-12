import pytest
from datetime import datetime
from pyspark.sql import Row
from nyc_taxi_pipeline.silver.deduplication import deduplicate_trips

# 导入你的元数据常量
from nyc_taxi_pipeline.metadata.system_columns import SILVER_LOAD_TIMESTAMP_COLUMN
from nyc_taxi_pipeline.metadata.business_columns import COL_PU_DATETIME

def test_deduplicate_trips(spark):
    # 准备测试数据：必须包含窗口函数 orderBy 中用到的所有列
    # 使用常量名确保与生产代码完全一致
    data = [
        Row(trip_key="trip_1", **{SILVER_LOAD_TIMESTAMP_COLUMN: datetime(2026, 1, 1, 10, 0, 0), COL_PU_DATETIME: datetime(2026, 1, 1, 10, 0, 0)}),
        Row(trip_key="trip_1", **{SILVER_LOAD_TIMESTAMP_COLUMN: datetime(2026, 1, 1, 11, 0, 0), COL_PU_DATETIME: datetime(2026, 1, 1, 11, 0, 0)}), # 保留这条
        Row(trip_key="trip_2", **{SILVER_LOAD_TIMESTAMP_COLUMN: datetime(2026, 1, 1, 0, 0, 0), COL_PU_DATETIME: datetime(2026, 1, 1, 0, 0, 0)}),
    ]
    df = spark.createDataFrame(data)

    clean_df, rejected_df = deduplicate_trips(df)

    # 验证清洗后的数据
    clean_data = clean_df.collect()
    assert len(clean_data) == 2
    
    # 验证保留的是 _silver_load_timestamp 最新的那条
    trip_1_clean = [r for r in clean_data if r["trip_key"] == "trip_1"][0]
    assert trip_1_clean[SILVER_LOAD_TIMESTAMP_COLUMN] == datetime(2026, 1, 1, 11, 0, 0)

    # 验证被拒绝的数据
    rejected_data = rejected_df.collect()
    assert len(rejected_data) == 1
    assert rejected_data[0]["trip_key"] == "trip_1"
    
    # 🌟 修复：匹配源码中的 f"duplicate_on_{'_'.join(partition_cols)}" 逻辑
    assert rejected_data[0]["reject_reason"] == "duplicate_on_trip_key"
    assert "rejected_at" in rejected_df.columns