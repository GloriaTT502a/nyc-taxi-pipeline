import os 
import logging 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__) 

def get_spark_session(app_name="nyc-taxi-pipeline"): 
    """
    纯净版工业级 SparkSession 路由工厂
    """
    # 场景 1：代码运行在 Databricks 真实集群上
    try: 
        from databricks.sdk.runtime import spark 
        if spark is not None:
            logger.info("✅ 检测到云端 Databricks 运行时，接管内置 SparkSession。") 
            return spark 
    except ImportError: 
        pass 

    # 场景 2：CI 或 本地单元测试 (100% 确定性优先)
    is_ci = os.environ.get("GITHUB_ACTIONS") == "true"
    use_local_spark = os.environ.get("USE_LOCAL_SPARK", "false").lower() == "true"

    if is_ci or use_local_spark: 
        logger.info("🚀 启动纯本地原生 PySpark 引擎 (已建立物理隔离屏障)...")
        
        os.environ.pop("SPARK_REMOTE", None)
        os.environ.pop("DATABRICKS_RUNTIME_VERSION", None)
        os.environ["SPARK_LOCAL_TESTING"] = "1"

        try:
            # 🌟【工业级防弹衣核心】：强行绕过模块劫持，直接从底层导入经典版 JVM SparkSession
            try:
                # PySpark 3.5+ 专有的物理隔离路径
                from pyspark.sql.classic.session import SparkSession
                logger.info("🛡️ 已成功锁定 PySpark 3.5 Classic Session 命名空间")
            except ImportError:
                # 兼容老版本
                from pyspark.sql import SparkSession

            return (
                SparkSession.builder
                    .master("local[*]")  
                    .appName(f"{app_name}-local-testing")
                    # 强行覆盖远端配置
                    .config("spark.remote", "false")
                    .config("spark.sql.execution.arrow.pyspark.enabled", "false")
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
                    .config("spark.sql.session.timeZone", "UTC")
                    .config("spark.ui.enabled", "false")
                    .config("spark.sql.shuffle.partitions", "2")
                    .config("spark.default.parallelism", "2")
                    .getOrCreate()
            )
        except Exception as e:
            logger.error(f"❌ 本地 PySpark 启动失败: {e}")
            raise e

# ==========================================
    # 场景 3：本地开发机连 Databricks 远程集群 (Remote Debugging)
    # ==========================================
    logger.info("🌐 尝试通过 Databricks Connect 桥接云端远程集群...")
    try:
        from databricks.connect import DatabricksSession
        return (
            DatabricksSession.builder
            .appName(f"{app_name}-remote-debug")
            .getOrCreate()
        )
    except ImportError:
        error_msg = (
            "❌ 无法路由到有效的 Spark 计算环境！\n"
            "👉 如果你想跑本地快速测试，请在终端执行: export USE_LOCAL_SPARK=true\n"
            "👉 如果你想连云端集群做集成联调，请先执行: pip install -r requirements/databricks.txt"
        )
        logger.error(error_msg)
        raise RuntimeError(error_msg)