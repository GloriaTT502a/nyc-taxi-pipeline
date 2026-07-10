import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from shapely.geometry import Polygon

# 🌟 只导入现在真实存在的提取函数
from nyc_taxi_pipeline.spatial.zone_lookup import process_raw_taxi_zone

@pytest.fixture
def mock_shapefile_reader():
    """
    构造一个 Mock 对象，模拟 pyshp (shapefile.Reader) 的行为，
    拦截对物理文件的读取。
    """
    mock_sf = MagicMock()
    
    # 模拟两条解析出来的地理记录
    mock_record_1 = MagicMock()
    mock_record_1.record.as_dict.return_value = {'LocationID': 132, 'borough': 'Manhattan', 'zone': 'Midtown East'}
    # 模拟一个 EPSG:2263 坐标的纯正方形
    mock_record_1.shape.__geo_interface__ = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)]).__geo_interface__

    mock_record_2 = MagicMock()
    mock_record_2.record.as_dict.return_value = {'LocationID': 288, 'borough': 'Queens', 'zone': 'Tiny Pocket Park'}
    mock_record_2.shape.__geo_interface__ = Polygon([(20, 20), (30, 20), (30, 30), (20, 30)]).__geo_interface__

    mock_sf.shapeRecords.return_value = [mock_record_1, mock_record_2]
    return mock_sf


@patch('nyc_taxi_pipeline.spatial.zone_lookup.shapefile.Reader')
def test_process_raw_taxi_zone(mock_reader_class, mock_shapefile_reader):
    """测试 Bronze 层的纯粹数据提取逻辑"""
    # 注入 mock 对象
    mock_reader_class.return_value = mock_shapefile_reader
    
    # 执行函数（传入假路径，因为底层已经被我们 Mock 拦截了）
    pdf_raw = process_raw_taxi_zone("fake/path/to/taxi_zones.shp")
    
    # 校验结果
    assert len(pdf_raw) == 2
    assert "raw_boundary_wkt" in pdf_raw.columns
    assert "LocationID" in pdf_raw.columns
    
    # 校验第一条记录的数据是否正确提取
    assert pdf_raw.iloc[0]["LocationID"] == 132
    assert pdf_raw.iloc[0]["borough"] == "Manhattan"
    assert "POLYGON" in pdf_raw.iloc[0]["raw_boundary_wkt"]
    