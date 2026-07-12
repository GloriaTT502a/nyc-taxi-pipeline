import pytest
import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon
from pyspark.sql import SparkSession
from nyc_taxi_pipeline.spatial.build_zone_lookup import process_spatial_to_h3, build_dim_taxi_zone_h3
from unittest.mock import patch, MagicMock

# 1. Create a Mock spactial GeoDataFrame
@pytest.fixture
def mock_geodataframe():
    """Construct a fake GeoDataFrame containing a square to simulate the EPSG:4326 coordinates of New York City."""
    # Construct a fake polygon (roughly around New York's latitude and longitude: lat ~40.7, lng ~-74.0).
    polygon = Polygon([(-74.0, 40.7), (-74.0, 40.8), (-73.9, 40.8), (-73.9, 40.7)])
    
    data = {
        "LocationID": [1],
        "borough": ["Manhattan"],
        "zone": ["Test Zone"],
        "geometry": [polygon]
    }
    # Initialize EPSG:4326 
    gdf = gpd.GeoDataFrame(data, crs="EPSG:4326")
    return gdf

# 2. Test core logic (runs in seconds, Spark not required)
def test_process_spatial_to_h3(mock_geodataframe):
    # Execute business logic
    result_pdf = process_spatial_to_h3(mock_geodataframe)
    
    # Assert
    assert isinstance(result_pdf, pd.DataFrame)
    assert len(result_pdf) == 1
    assert "h3_cell" in result_pdf.columns
    assert result_pdf.iloc[0]["LocationID"] == 1
    # Validate the H3 string format (a hexadecimal string of approximately 15 characters in length).
    assert isinstance(result_pdf.iloc[0]["h3_cell"], str)
    assert len(result_pdf.iloc[0]["h3_cell"]) > 10

# 3. Testing Spark I/O (Integration Testing)
@patch("nyc_taxi_pipeline.spatial.build_zone_lookup.gpd.read_file")
def test_build_dim_taxi_zone_h3(mock_read_file, mock_geodataframe, spark):
    """
    `spark_session` is the global pytest fixture defined in your `conftest.py` file.
    """
    # Let read_file return our fake GeoDataFrame
    mock_read_file.return_value = mock_geodataframe
    
    # Intercept Spark DataFrame write operations to prevent the table from being actually created on the local disk.
    with patch.object(SparkSession, 'createDataFrame') as mock_create_df:
        mock_df = MagicMock()
        mock_create_df.return_value = mock_df
        
        # Run the main function
        build_dim_taxi_zone_h3(spark, "dummy/path.shp", "test_schema.test_table")
        
        # Assertion: Verify whether the method for saving the Delta table was called correctly.
        mock_read_file.assert_called_once_with("dummy/path.shp")
        mock_create_df.assert_called_once()
        mock_df.write.format.assert_called_with("delta")
        mock_df.write.format().mode.assert_called_with("overwrite")
        mock_df.write.format().mode().saveAsTable.assert_called_with("test_schema.test_table")