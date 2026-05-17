# tests/silver/test_h3_enrichment.py
import pytest
from pyspark.sql import Row
from nyc_taxi_pipeline.silver.h3_enrichment import enrich_h3_cells

def test_enrich_h3_cells(spark):
    # 1. 准备事实表 Mock 数据 (PULocationID=1 命中维度表，PULocationID=999 走降级计算)
    fact_data = [
        Row(PULocationID=1, DOLocationID=2, pickup_latitude=40.7128, pickup_longitude=-74.0060, dropoff_latitude=40.7306, dropoff_longitude=-73.9352),
        Row(PULocationID=999, DOLocationID=2, pickup_latitude=40.7128, pickup_longitude=-74.0060, dropoff_latitude=40.7306, dropoff_longitude=-73.9352)
    ]
    fact_df = spark.createDataFrame(fact_data)

    # 2. 准备维度表 Mock 数据
    zone_data = [
        Row(LocationID=1, h3_cell="852a1007fffffff"),
        Row(LocationID=2, h3_cell="852a100bfffffff")
    ]
    zone_dim_df = spark.createDataFrame(zone_data)

    # 3. 执行测试 (指定 resolution=5 避免高分辨率计算过慢)
    result_df = enrich_h3_cells(fact_df, zone_dim_df, resolution=5)
    results = result_df.collect()

    assert len(results) == 2
    
    # 4. 断言验证
    # 第一条记录：应该命中维度表缓存 (fallback 应为 0)
    row1 = [r for r in results if r["PULocationID"] == 1][0]
    assert row1["h3_pickup"] == "852a1007fffffff"
    assert row1["is_pickup_fallback"] == 0

    # 第二条记录：维度表无记录，触发 Fallback 降级 UDF 算出来的 H3 
    row2 = [r for r in results if r["PULocationID"] == 999][0]
    assert row2["h3_pickup"] is not None
    assert row2["is_pickup_fallback"] == 1