import os 
import logging 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__) 

def get_spark_session(app_name="nyc-taxi-pipeline"): 
    """
    纯净版工业级 SparkSession 路由工厂
    支持 Databricks 集群内、本地纯净 JVM 测试沙箱 以及 Databricks Connect 远程桥接。
    """
    # ==========================================
    # 场景 1：代码运行在 Databricks 真实集群上
    # ==========================================
    try: 
        from databricks.sdk.runtime import spark 
        if spark is not None:
            logger.info("✅ 检测到云端 Databricks 运行时，无缝接管内置 SparkSession。") 
            return spark 
    except ImportError: 
        pass 

    # ==========================================
    # 场景 2：CI 或 本地单元测试 (100% 确定性优先)
    # ==========================================
    is_ci = os.environ.get("GITHUB_ACTIONS") == "true"
    use_local_spark = os.environ.get("USE_LOCAL_SPARK", "false").lower() == "true"

    if is_ci or use_local_spark: 
        logger.info("🚀 启动纯本地原生 PySpark 引擎 (已建立物理隔离屏障)...")
        
        # 彻底隔离远程连接信号，预防 gRPC 连接挂起
        os.environ.pop("SPARK_REMOTE", None)
        os.environ.pop("DATABRICKS_RUNTIME_VERSION", None)
        os.environ["SPARK_LOCAL_TESTING"] = "1"

        try:
            # 锁定 PySpark 3.5 专有的经典物理隔离路径，防御 monkey patch 劫持
            try:
                from pyspark.sql.classic.session import SparkSession
                logger.info("🛡️ 已成功锁定 PySpark 3.5 Classic Session 命名空间")
            except ImportError:
                from pyspark.sql import SparkSession

            return (
                SparkSession.builder
                    .master("local[*]")  
                    .appName(f"{app_name}-local-testing")
                    
                    # 🌟【核心依赖加载】：必须最先声明物理 Jar 包依赖
                    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
                    
                    # 🌟【事务级别对齐】：强制全局表格式、Catalog 目录和核心扩展完美对齐 DeltaLake 规范
                    # 彻底解决本地临时表/托管表执行 .process() 时不支持 truncate/merge/delete 的断言异常
                    .config("spark.sql.sources.default", "delta")
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                    
                    # 性能与兼容：关闭本地不稳固的 Arrow 内存直接读写，杜绝 Java 17+ 模块封装闪退
                    .config("spark.sql.execution.arrow.pyspark.enabled", "false")
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