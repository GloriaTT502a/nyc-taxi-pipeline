import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """
    Industrial-grade local test engine: Configures Delta Lake support 
    and drastically compresses memory footprint for local execution.
    """
    try:
        # Get spark variable from Databricks 
        from pyspark.dbutils import DBUtils
        return SparkSession.builder.getOrCreate()
    except ImportError:
        # or use local SparkSession 
        return (SparkSession.builder
                .master("local[*]")
                .appName("SilverExploration")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate())
        
@pytest.fixture
def mock_silver_path(tmp_path):
    """
    Create a temp path for silver exploratory testing
    """
    path = str(tmp_path / "silver_table")
    return path