import os 
import pytest
from dataclasses import replace 
from pyspark.sql import Row
from nyc_taxi_pipeline.silver.h3_enrichment import enrich_h3_cells
from nyc_taxi_pipeline.config.settings import PipelineSettings

# 引入官方契约常量，彻底告别魔法字符串
from nyc_taxi_pipeline.contracts.dim_zone_schema import (
    ZONE_LOCATION_ID_COL, 
    ZONE_H3_CELL_COL,
    ZONE_WKT_COL
)
from nyc_taxi_pipeline.observability.metric_keys import METRIC_PU_FALLBACK, METRIC_DO_FALLBACK

is_local_env = os.environ.get("APP_ENV", "local") == "local"

@pytest.mark.skipif(
    is_local_env, 
    reason="Requires Databricks Photon engine for st_contains and native H3 functions"
)
def test_enrich_h3_cells(spark):
    # 1. 准备事实表 Mock 数据 (加入 trip_id 方便断言取值)
    fact_data = [
        # Case 1 (现代数据): 有明确 ID，走极速匹配
        Row(trip_id=1, PULocationID=1, DOLocationID=2, pickup_latitude=40.7128, pickup_longitude=-74.0060, dropoff_latitude=40.7306, dropoff_longitude=-73.9352),
        
        # Case 2 (2010老数据): 缺失 ID，但坐标 (-74, 40) 落在 Location 1 的多边形内部！
        Row(trip_id=2, PULocationID=None, DOLocationID=2, pickup_latitude=40.7128, pickup_longitude=-74.0060, dropoff_latitude=40.7306, dropoff_longitude=-73.9352),
        
        # Case 3 (脏数据): 乱填的 ID 999，触发底层的 C++ 原生 H3 兜底计算
        Row(trip_id=3, PULocationID=999, DOLocationID=2, pickup_latitude=40.7128, pickup_longitude=-74.0060, dropoff_latitude=40.7306, dropoff_longitude=-73.9352)
    ]
    fact_df = spark.createDataFrame(fact_data)

    # 2. 准备维度表 Mock 数据 (严格遵循 Schema 契约)
    zone_data = [
        # 构建一个巨大的 WKT 多边形，故意把测试坐标 (-74.0060, 40.7128) 包裹进去
        Row(**{
            ZONE_LOCATION_ID_COL: 1, 
            ZONE_H3_CELL_COL: "852a1007fffffff",
            ZONE_WKT_COL: "POLYGON((-75 40, -75 42, -73 42, -73 40, -75 40))" 
        }),
        Row(**{
            ZONE_LOCATION_ID_COL: 2, 
            ZONE_H3_CELL_COL: "852a100bfffffff",
            ZONE_WKT_COL: "POLYGON((-73 40, -73 42, -71 42, -71 40, -73 40))"
        })
    ]
    zone_dim_df = spark.createDataFrame(zone_data)

    # 3. 构造配置
    base_settings = PipelineSettings(
        runtime_env="local", catalog="nyc", base_path="/tmp/test",
        shp_path="/tmp/test/zone.shp", bronze_db="bronze", 
        silver_db="silver", gold_db="gold"
    )
    settings = replace(base_settings, h3_resolution=8)

    # 4. 执行核心测试逻辑
    result_df = enrich_h3_cells(fact_df, zone_dim_df, settings=settings)
    results = result_df.collect()

    assert len(results) == 3
    
    # 5. 断言验证
    # --- Case 1 验证：现代数据极速关联 ---
    row1 = [r for r in results if r["trip_id"] == 1][0]
    assert row1["h3_pickup"] == "852a1007fffffff"
    assert row1[METRIC_PU_FALLBACK] == 0

    # --- Case 2 验证：老数据空间反查修复 (核心高光时刻) ---
    row2 = [r for r in results if r["trip_id"] == 2][0]
    assert row2["PULocationID"] == 1  # ✨ 魔法生效：原本为 None 的 ID 被 WKT 找回来了！
    assert row2["h3_pickup"] == "852a1007fffffff" # 顺手也拿到了正确的 H3
    assert row2[METRIC_PU_FALLBACK] == 0          # 因为查表成功了，所以没触发 Fallback

    # --- Case 3 验证：脏数据兜底计算 ---
    row3 = [r for r in results if r["trip_id"] == 3][0]
    assert row3["h3_pickup"] is not None          # 虽然 ID 是 999 没匹配上，但也现场算出了 H3
    assert row3[METRIC_PU_FALLBACK] == 1          # 成功触发 Fallback 打点警告