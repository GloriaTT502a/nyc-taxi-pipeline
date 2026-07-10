import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from shapely.geometry import Polygon

# 🌟 只导入现在真实存在的提取函数
from nyc_taxi_pipeline.spatial.zone_lookup import process_raw_taxi_zone

@pytest.fixture
def mock_shapefile_reader():
    """
    构造一个全方位兼容的 Mock 对象，模拟 pyshp (shapefile.Reader) 的行为，
    彻底拦截对物理文件的读取。
    """
    mock_sf = MagicMock()
    
    def create_mock_shape_record(loc_id, borough, zone, coords):
        # 1. 模拟地理几何体 (Shape)
        mock_shape = MagicMock()
        mock_shape.__geo_interface__ = Polygon(coords).__geo_interface__
        
        # 2. 模拟属性数据 (Record)
        mock_record_data = {'LocationID': loc_id, 'borough': borough, 'zone': zone}
        mock_record = MagicMock()
        
        # 🛡️ 终极防御：覆盖 pyshp 支持的所有三种数据读取习惯
        # 习惯 A: 通过 sr.record.as_dict() 读取
        mock_record.as_dict.return_value = mock_record_data
        
        # 习惯 B: 通过对象属性 sr.record.LocationID 直接读取
        mock_record.LocationID = loc_id
        mock_record.borough = borough
        mock_record.zone = zone
        
        # 习惯 C: 通过字典索引 sr.record['LocationID'] 读取
        mock_record.__getitem__.side_effect = mock_record_data.__getitem__
        
        # 3. 组合成 ShapeRecord 对象
        mock_sr = MagicMock()
        mock_sr.shape = mock_shape
        mock_sr.record = mock_record
        
        return mock_sr

    # 生成假数据记录
    mock_record_1 = create_mock_shape_record(
        132, 'Manhattan', 'Midtown East', [(0, 0), (10, 0), (10, 10), (0, 10)]
    )
    mock_record_2 = create_mock_shape_record(
        288, 'Queens', 'Tiny Pocket Park', [(20, 20), (30, 20), (30, 30), (20, 30)]
    )

    # 挂载到 mock_sf 上，响应 sf.shapeRecords() 的遍历调用
    mock_sf.shapeRecords.return_value = [mock_record_1, mock_record_2]
    
    # 兜底：兼容如果底层代码单独调用 sf.records() 或 sf.shapes() 的情况
    mock_sf.records.return_value = [mock_record_1.record, mock_record_2.record]
    mock_sf.shapes.return_value = [mock_record_1.shape, mock_record_2.shape]
    
    return mock_sf


@patch('nyc_taxi_pipeline.spatial.zone_lookup.shapefile.Reader')
def test_process_raw_taxi_zone(mock_reader_class, mock_shapefile_reader):
    """测试 Bronze 层的纯粹数据提取逻辑"""
    
    # 注入 mock 对象
    mock_reader_class.return_value = mock_shapefile_reader
    
    # 执行函数（传入假路径，因为底层已经被我们 Mock 拦截了）
    pdf_raw = process_raw_taxi_zone("fake/path/to/taxi_zones.shp")
    
    # 校验结果长度与必填列
    assert len(pdf_raw) == 2
    assert "raw_boundary_wkt" in pdf_raw.columns
    assert "LocationID" in pdf_raw.columns
    
    # 校验第一条记录的数据是否被完美提取与映射
    assert pdf_raw.iloc[0]["LocationID"] == 132
    assert pdf_raw.iloc[0]["borough"] == "Manhattan"
    assert "POLYGON" in pdf_raw.iloc[0]["raw_boundary_wkt"] 