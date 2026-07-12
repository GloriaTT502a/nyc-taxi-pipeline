import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from shapely.geometry import Polygon

from nyc_taxi_pipeline.spatial.zone_lookup import process_raw_taxi_zone

# ==========================================
# 🛡️ 纯 Python 伪造类，彻底骗过 Pandas 的解析机制
# ==========================================
class MockRecord:
    def __init__(self, loc_id, borough, zone):
        # 满足属性访问 record.LocationID
        self.LocationID = loc_id
        self.borough = borough
        self.zone = zone
        # 满足列表形式访问
        self._list = [loc_id, borough, zone]
        # 满足字典形式访问
        self._dict = {'LocationID': loc_id, 'borough': borough, 'zone': zone}

    def as_dict(self):
        return self._dict

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._list[key]
        return self._dict[key]

    def __iter__(self):
        return iter(self._list)
        
    def __len__(self):
        return len(self._list)

class MockShape:
    def __init__(self, coords):
        self.points = coords
        self.shapeType = 5 # 代表 Polygon
        self.__geo_interface__ = Polygon(coords).__geo_interface__

class MockShapeRecord:
    def __init__(self, loc_id, borough, zone, coords):
        self.record = MockRecord(loc_id, borough, zone)
        self.shape = MockShape(coords)

# ==========================================
# 测试 Fixture 与用例
# ==========================================
@pytest.fixture
def mock_shapefile_reader():
    """构造一个全方位兼容、支持 with 语法、且能被 Pandas 完美解析的 Mock Reader"""
    mock_sf = MagicMock()
    
    # 构造必须闭合的多边形坐标 (首尾坐标一致)
    coords_1 = [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]
    coords_2 = [(20, 20), (30, 20), (30, 30), (20, 30), (20, 20)]
    
    # 使用纯 Python 对象
    sr1 = MockShapeRecord(132, 'Manhattan', 'Midtown East', coords_1)
    sr2 = MockShapeRecord(288, 'Queens', 'Tiny Pocket Park', coords_2)
    
    # 拦截所有可能的数据读取方法
    mock_sf.shapeRecords.return_value = [sr1, sr2]
    mock_sf.records.return_value = [sr1.record, sr2.record]
    mock_sf.shapes.return_value = [sr1.shape, sr2.shape]
    
    # 模拟底层的 fields 属性（第一列通常是代表删除标记的 DeletionFlag）
    mock_sf.fields = [
        ("DeletionFlag", "C", 1, 0), 
        ["LocationID", "N", 10, 0], 
        ["borough", "C", 50, 0], 
        ["zone", "C", 50, 0]
    ]
    
    # 🛡️ 关键：完美支持 `with shapefile.Reader(...) as sf:` 语法！
    mock_sf.__enter__.return_value = mock_sf
    mock_sf.__exit__.return_value = None
    
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
    
    # 这次 LocationID 绝对是被完美提取出来的 132！
    assert pdf_raw.iloc[0]["LocationID"] == 132
    assert pdf_raw.iloc[0]["borough"] == "Manhattan"
    assert "POLYGON" in pdf_raw.iloc[0]["raw_boundary_wkt"]