import pytest
from pyspark.sql import SparkSession
import sys
from unittest.mock import MagicMock


# ---------------------------------------------------
# Mock Databricks SDK
# 防止 CI 环境触发真实 Databricks 认证
# ---------------------------------------------------

mock_db = MagicMock()

mock_db.WorkspaceClient.return_value = mock_db

sys.modules["databricks"] = mock_db
sys.modules["databricks.sdk"] = mock_db
sys.modules["databricks.sdk.core"] = mock_db


# ---------------------------------------------------
# Spark Fixture
# ---------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    """
    Industrial-grade local Spark test session
    for CI/CD and local development.
    """

    try:
        # Databricks Runtime 环境
        from pyspark.dbutils import DBUtils

        spark = SparkSession.builder.getOrCreate()

    except ImportError:
        # Local / GitHub Actions 环境

        spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("nyc-taxi-pipeline-tests")

            # CI 稳定性优化
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.default.parallelism", "2")

            # Spark networking fix
            .config("spark.driver.host", "127.0.0.1")

            # Timezone consistency
            .config("spark.sql.session.timeZone", "UTC")

            # Delta Lake
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension"
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )

            # Delta package
            .config(
                "spark.jars.packages",
                "io.delta:delta-spark_2.12:3.1.0"
            )

            .getOrCreate()
        )

    yield spark

    spark.stop()


# ---------------------------------------------------
# Temp path fixture
# ---------------------------------------------------

@pytest.fixture
def mock_silver_path(tmp_path):
    """
    Temporary path for local silver-layer testing.
    """

    return str(tmp_path / "silver_table")