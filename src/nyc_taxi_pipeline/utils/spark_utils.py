import os 
import logging 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__) 

def get_spark_session(): 
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
        
        try: 
            from pyspark.sql import SparkSession 
            return (
                SparkSession.builder 
                    .master("local[2]")  
                    .appName("nyc-taxi-pipeline-local-testing") 
                    
                    # 关闭全局非必要 Arrow
                    .config("spark.sql.execution.arrow.pyspark.enabled", "false")
                    
                    # Delta Lake 核心组件支持配置
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
                    
                    # 本地性能调优
                    .config("spark.sql.session.timeZone", "UTC")       
                    .config("spark.ui.enabled", "false")               
                    .config("spark.sql.shuffle.partitions", "2")       
                    .getOrCreate()
            )
        except Exception as e: 
            logger.error(f"本地 PySpark 启动失败: {e}")
            raise e

    # 场景 3：未定义的运行上下文兜底保护
    error_msg = "无法路由到有效的 Spark 计算环境！请加上 USE_LOCAL_SPARK=true 运行。"
    logger.error(error_msg)
    raise RuntimeError(error_msg)