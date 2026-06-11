import pytest
import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon
from unittest.mock import patch # 引入 Mock 库

# 导入空间双轨制构建模块 (请确保路径与你的实际项目一致)
from nyc_taxi_pipeline.spatial.build_zone_lookup import (
    process_dim_taxi_zone,
    process_bridge_taxi_zone_h3,
    build_spatial_tables
)

@pytest.fixture
def mock_shapefile_data():
    """构造 Mock 地理边界数据"""
    normal_polygon = Polygon([
        (-73.98, 40.75), (-73.96, 40.75), 
        (-73.96, 40.77), (-73.98, 40.77)
    ])
    tiny_polygon = Polygon([
        (-74.00, 40.71), (-74.0001, 40.71), 
        (-74.0001, 40.7101), (-74.00, 40.7101)
    ])
    
    gdf = gpd.GeoDataFrame({
        "LocationID": [132, 288], 
        "borough": ["Manhattan", "Queens"],
        "zone": ["Midtown East", "Tiny Pocket Park"],
        "geometry": [normal_polygon, tiny_polygon]
    }, crs="EPSG:4326")
    
    return gdf

# ==========================================
# 修复点 1 & 2: 移除 resolution=8 参数传参
# ==========================================
def test_process_dim_taxi_zone(mock_shapefile_data):
    """测试维度表生成逻辑 (1对1)"""
    # 修复：直接传 mock 数据，不再传 resolution
    pdf_dim = process_dim_taxi_zone(mock_shapefile_data)
    
    assert len(pdf_dim) == 2
    expected_columns = {"LocationID", "borough", "zone", "centroid_lat", "centroid_lng", "center_h3_cell"}
    assert set(pdf_dim.columns) == expected_columns
    assert pd.api.types.is_integer_dtype(pdf_dim["LocationID"])

def test_process_bridge_taxi_zone_h3(mock_shapefile_data):
    """测试桥接表生成逻辑 (1对N) 及财务分摊守恒定律"""
    # 修复：直接传 mock 数据，不再传 resolution
    pdf_bridge = process_bridge_taxi_zone_h3(mock_shapefile_data)
    
    midtown_cells = pdf_bridge[pdf_bridge["LocationID"] == 132]
    assert len(midtown_cells) > 1
    
    midtown_weight_sum = midtown_cells["cell_weight"].sum()
    assert pytest.approx(midtown_weight_sum, abs=1e-6) == 1.0

# ==========================================
# 修复点 3: 拦截物理写入动作，解决 DBFS_DISABLED
# ==========================================
def test_build_spatial_tables_integration(spark, mock_shapefile_data, monkeypatch):
    """
    测试流水线编排逻辑。利用 Mock 拦截物理写入，避免远程 Databricks 存储权限报错。
    """
    monkeypatch.setattr("geopandas.read_file", lambda x: mock_shapefile_data)
    
    # 🌟 核心工业级 Fix：精准狙击 Databricks Connect 专属的 Writer
    # 在 Spark Connect 架构下，必须拦截 connect.readwriter 而不是传统的 classic.DataFrameWriter
    patch_target = "pyspark.sql.connect.readwriter.DataFrameWriter.saveAsTable"
    
    with patch(patch_target) as mock_save:
        # 此时执行代码，数据在内存中计算完毕后，写入指令会被成功拦截
        build_spatial_tables(
            spark=spark,
            shp_path="fake_system_path/taxi_zones.shp",
            dim_target_table="dev_catalog.taxi_schema.dim_taxi_zone",
            bridge_target_table="dev_catalog.taxi_schema.brg_taxi_zone_h3"
        )
        
        # 验证是否正确触发了两次 Delta 表的保存动作
        assert mock_save.call_count == 2
        
        # 验证传入的表名是否正确
        calls = mock_save.call_args_list
        assert calls[0][0][0] == "dev_catalog.taxi_schema.dim_taxi_zone"
        assert calls[1][0][0] == "dev_catalog.taxi_schema.brg_taxi_zone_h3" 