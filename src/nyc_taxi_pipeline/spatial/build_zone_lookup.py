import h3 
import geopandas as gpd 
from pyspark.sql import SparkSession 
import pandas as pd 
from nyc_taxi_pipeline.common.logger import get_logger 

logger = get_logger(__name__) 

def process_spatial_to_h3(gdf_raw: gpd.GeoDataFrame) -> pd.DataFrame:
    
    gdf_projected = gdf_raw.to_crs("EPSG:2263")
    centroids = gdf_projected.geometry.centroid.to_crs("EPSG:4326")
    
    gdf_raw["centroid_lat"] = centroids.y
    gdf_raw["centroid_lng"] = centroids.x
    
    gdf_raw["h3_cell"] = [
        (h3.latlng_to_cell(lat, lng, 8) if hasattr(h3, 'latlng_to_cell') else h3.geo_to_h3(lat, lng, 8))
        for lat, lng in zip(gdf_raw["centroid_lat"], gdf_raw["centroid_lng"])
    ]
    
    pdf = pd.DataFrame(gdf_raw[["LocationID", "borough", "zone", "centroid_lat", "centroid_lng", "h3_cell"]])
    pdf['LocationID'] = pdf['LocationID'].astype(int)
    return pdf

# --- I/O ---
def build_dim_taxi_zone_h3(spark: SparkSession, shp_path: str, target_table: str):
    logger.info(f"Reading and processing shapefile: {shp_path}") 
    # 1. 读文件 (I/O)
    gdf_raw = gpd.read_file(shp_path)
    
    # 2. core calcuation
    pdf = process_spatial_to_h3(gdf_raw)
    
    # 3. write to Delta (I/O)
    spark_df = spark.createDataFrame(pdf)
    spark_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
