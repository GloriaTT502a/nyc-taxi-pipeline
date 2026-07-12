import h3
import pandas as pd
import shapefile  # 纯 Python 库 pyshp, 替代 geopandas
from pyspark.sql import SparkSession
from shapely.geometry import shape
from shapely.ops import transform
from pyproj import Transformer
from nyc_taxi_pipeline.common.logger import get_logger
from nyc_taxi_pipeline.config.settings import H3_RESOLUTION

logger = get_logger(__name__)

# =========================================================================
# 🌟 新增模块：使用 pyshp 将原生 Shapefile 转化为普通 Pandas DataFrame
# =========================================================================
def read_shp_to_pandas(shp_path: str) -> pd.DataFrame:
    """使用纯 Python 读取 Shapefile，并组装为包含 shapely 对象的 DataFrame"""
    sf = shapefile.Reader(shp_path)
    records = []
    
    for sr in sf.shapeRecords():
        attrs = sr.record.as_dict()
        # 提取原生的纽约平面 (EPSG:2263) 几何图形
        geom = shape(sr.shape.__geo_interface__)
        records.append({
            "LocationID": int(attrs.get('LocationID', 0)),
            "borough": str(attrs.get('borough', '')),
            "zone": str(attrs.get('zone', '')),
            "geometry": geom 
        })
    return pd.DataFrame(records)

# =========================================================================
# 保持函数 1：处理质心维度表 (输入 pd.DataFrame，输出 pd.DataFrame)
# =========================================================================
def process_dim_taxi_zone(pdf_raw: pd.DataFrame) -> pd.DataFrame:
    # 初始化投影转换器 (EPSG:2263 -> EPSG:4326)
    transformer_to_wgs = Transformer.from_crs("EPSG:2263", "EPSG:4326", always_xy=True)
    fallback_func = h3.latlng_to_cell if hasattr(h3, 'latlng_to_cell') else h3.geo_to_h3
    
    # 彻底告别 iterrows，使用 zip 提取底层数组，提升 10 倍速度
    loc_ids = pdf_raw["LocationID"].values
    boros = pdf_raw["borough"].values
    zones = pdf_raw["zone"].values
    geoms = pdf_raw["geometry"].values
    
    dim_records = []
    
    for loc_id, boro, zone, geom_ny in zip(loc_ids, boros, zones, geoms):
        # 计算物理平面质心并投影回全球经纬度
        centroid_wgs = transform(transformer_to_wgs.transform, geom_ny.centroid)
        lng, lat = centroid_wgs.x, centroid_wgs.y
        
        center_h3 = fallback_func(lat, lng, H3_RESOLUTION)
        
        dim_records.append({
            "LocationID": loc_id,
            "borough": boro,
            "zone": zone,
            "centroid_lat": lat,
            "centroid_lng": lng,
            "h3_cell": center_h3
        })
        
    return pd.DataFrame(dim_records)

# =========================================================================
# 保持函数 2：处理 H3 桥接表 (输入 pd.DataFrame，输出 pd.DataFrame)
# =========================================================================
def process_bridge_taxi_zone_h3(pdf_raw: pd.DataFrame) -> pd.DataFrame:
    transformer_to_wgs = Transformer.from_crs("EPSG:2263", "EPSG:4326", always_xy=True)
    fallback_func = h3.latlng_to_cell if hasattr(h3, 'latlng_to_cell') else h3.geo_to_h3

    def _extract_cells(polygon_geom):
        outer = [(y, x) for x, y in polygon_geom.exterior.coords]
        holes = [[(y, x) for x, y in interior.coords] for interior in polygon_geom.interiors]
        
        if hasattr(h3, 'LatLngPoly'):
            h3_poly = h3.LatLngPoly(outer, *holes)
            if hasattr(h3, 'polygon_to_cells'):
                return h3.polygon_to_cells(h3_poly, res=H3_RESOLUTION)
            return h3.polyfill(h3_poly, res=H3_RESOLUTION)
        else:
            return h3.polyfill(polygon_geom.__geo_interface__, res=H3_RESOLUTION, geo_json_conformant=True)

    loc_ids = pdf_raw["LocationID"].values
    geoms = pdf_raw["geometry"].values
    
    bridge_records = []
    
    for loc_id, geom_ny in zip(loc_ids, geoms):
        # 必须将整个多边形转换为经纬度，才能用于 H3 网格填充
        geom_wgs84 = transform(transformer_to_wgs.transform, geom_ny)
        
        h3_cells = set()
        if geom_wgs84.geom_type == 'Polygon':
            h3_cells.update(_extract_cells(geom_wgs84))
        elif geom_wgs84.geom_type == 'MultiPolygon':
            for sub_poly in geom_wgs84.geoms:
                h3_cells.update(_extract_cells(sub_poly))
                
        # 极小区域保底
        if not h3_cells:
            c = geom_wgs84.centroid
            h3_cells.add(fallback_func(c.y, c.x, H3_RESOLUTION))
            
        weight = 1.0 / len(h3_cells)
        bridge_records.extend([(loc_id, cell, weight) for cell in h3_cells])
        
    return pd.DataFrame(bridge_records, columns=["LocationID", "h3_cell", "cell_weight"])

# =========================================================================
# 保持原有的 I/O 流水线编排不变
# =========================================================================
def build_spatial_tables(spark: SparkSession, shp_path: str, dim_target_table: str, bridge_target_table: str):
    logger.info(f"Reading and processing shapefile: {shp_path} using pure python pyshp") 
    
    # 1. 读文件 (I/O)，无缝替换掉 gpd.read_file
    pdf_raw = read_shp_to_pandas(shp_path)
    
    # 2. Process dim taxi zone 
    logger.info("Processing process_dim_taxi_zone for centralized h3 cell")
    pdf_dim = process_dim_taxi_zone(pdf_raw) 
    spark.createDataFrame(pdf_dim).write.format("delta").mode("overwrite").saveAsTable(dim_target_table) 

    # 3. process bridge taxi 
    logger.info("Processing process_bridge_taxi_zone_h3") 
    pdf_bridge = process_bridge_taxi_zone_h3(pdf_raw) 
    spark.createDataFrame(pdf_bridge).write.format("delta").mode("overwrite").saveAsTable(bridge_target_table) 

    logger.info("process_dim_taxi_zone and process_bridge_taxi_zone_h3 finished") 
