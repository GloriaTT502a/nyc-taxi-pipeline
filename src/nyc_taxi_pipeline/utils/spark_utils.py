import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_spark_session(
    runtime_env: str,
    app_name: str = "nyc-taxi-pipeline",
):
    """
    Enterprise SparkSession Factory

    Supported runtime environments:

        local  -> Local PySpark
        ci     -> Local PySpark
        dev    -> Databricks Connect
        qa     -> Databricks Connect
        prod   -> Databricks Connect

    If running inside Databricks Runtime,
    always reuse the built-in SparkSession.
    """

    runtime_env = runtime_env.lower()

    is_on_cluster = "DATABRICKS_RUNTIME_VERSION" in os.environ

    #
    # Case 1
    # Running inside Databricks Job Cluster
    #
    if is_on_cluster:

        logger.info(
            "Detected Databricks Runtime. "
            "Using built-in SparkSession."
        )

        from databricks.sdk.runtime import spark

        return spark

    #
    # Case 2
    # Local / CI
    #
    if runtime_env in ("local", "ci"):

        logger.info(
            "Starting Local PySpark Session..."
        )

        os.environ.pop("SPARK_REMOTE", None)
        os.environ["SPARK_LOCAL_TESTING"] = "1"

        try:

            try:
                from pyspark.sql.classic.session import SparkSession
            except ImportError:
                from pyspark.sql import SparkSession

            spark = (
                SparkSession.builder
                .master("local[*]")
                .appName(f"{app_name}-local")
                .config(
                    "spark.jars.packages",
                    "io.delta:delta-spark_2.12:3.1.0",
                )
                .config(
                    "spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension",
                )
                .config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                )
                .config(
                    "spark.sql.sources.default",
                    "delta",
                )
                .config(
                    "spark.sql.warehouse.dir",
                    "spark-warehouse",
                )
                .config(
                    "spark.sql.execution.arrow.pyspark.enabled",
                    "false",
                )
                .config(
                    "spark.ui.enabled",
                    "false",
                )
                .config(
                    "spark.sql.shuffle.partitions",
                    "2",
                )
                .config(
                    "spark.default.parallelism",
                    "2",
                )
                .getOrCreate()
            )

            return spark

        except Exception:

            logger.exception(
                "Failed to create local SparkSession."
            )

            raise

    #
    # Case 3
    # Databricks Connect
    #
    logger.info(
        "Using Databricks Connect "
        "(runtime=%s)",
        runtime_env,
    )

    try:

        from databricks.connect import DatabricksSession

        spark = (
            DatabricksSession.builder
            .serverless()
            .getOrCreate()
        )

        return spark

    except ImportError as ex:

        logger.exception(
            "Databricks Connect is not installed."
        )

        raise RuntimeError(
            "Please install project dependencies:\n"
            'pip install -e ".[dev]"'
        ) from ex 
    