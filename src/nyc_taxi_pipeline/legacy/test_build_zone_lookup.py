import pytest
import pandas as pd
from shapely.geometry import Polygon

# 🌟 导入重构后的全新函数名称
from nyc_taxi_pipeline.spatial.build_zone_lookup import (
    process_dim_taxi_zone, 
    process_bridge_taxi_zone_h3
)

@pytest.fixture
def mock_spatial_data():
    """
    构造完全脱离 GeoPandas 的 Mock 空间数据。
    这里直接使用纯 Pandas DataFrame + Shapely 原生多边形对象，
    完美模拟我们业务代码中 pyshp 读取出的数据结构。
    """
    # 模拟两个纽约本地平面坐标系 (EPSG:2263) 的多边形
    poly1 = Polygon([(980000, 200000), (981000, 200000), (981000, 201000), (980000, 201000)])
    poly2 = Polygon([(990000, 220000), (991000, 220000), (991000, 221000), (990000, 221000)])
    
    df = pd.DataFrame({
        "LocationID": [132, 288], 
        "borough": ["Manhattan", "Queens"],
        "zone": ["Midtown East", "Tiny Pocket Park"],
        "geometry": [poly1, poly2]
    })
    return df

def test_process_dim_taxi_zone(mock_spatial_data):
    """测试维度表生成逻辑"""
    pdf_dim = process_dim_taxi_zone(mock_spatial_data)
    
    assert len(pdf_dim) == 2
    assert "h3_cell" in pdf_dim.columns
    assert pdf_dim["LocationID"].dtype == "int64"

def test_process_bridge_taxi_zone_h3(mock_spatial_data):
    """测试桥接表生成逻辑 (包含财务权重守恒校验)"""
    pdf_bridge = process_bridge_taxi_zone_h3(mock_spatial_data)
    
    # 验证桥接表不为空且包含所需列
    assert not pdf_bridge.empty
    assert "h3_cell" in pdf_bridge.columns
    assert "cell_weight" in pdf_bridge.columns
    
    # 🌟 验证财务权重守恒定律 (所有 H3 切片的权重相加必须等于 1.0)
    for loc_id in mock_spatial_data["LocationID"]:
        total_weight = pdf_bridge[pdf_bridge["LocationID"] == loc_id]["cell_weight"].sum()
        assert pytest.approx(total_weight, abs=1e-6) == 1.0