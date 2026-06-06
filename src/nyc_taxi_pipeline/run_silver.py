import uuid
import logging
from datetime import datetime
#from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession 

# 导入你精心封装的 Pipeline 类
from nyc_taxi_pipeline.silver.pipeline import NYCTaxiSilverPipeline

# 配置本地终端的日志打印格式，方便你在 VS Code 实时观察 Pipeline 的运行轨迹
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    logger.info("🚀 正在唤醒 Databricks Serverless 算力...")
    spark = SparkSession.builder.getOrCreate()

    # ==========================================
    # 1. 准备基础运行参数
    # ==========================================
    # 生成全局唯一的运行批次号，用于血缘追踪和对账
    run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    
    # 🌟 强烈建议：在本地首次跑通时，将 target 改为你个人的 dev 库，避免弄脏生产环境
    # 等代码合并到 main 分支后，由 pipeline.py 里的 PipelineConfig 自动接管生产路径
    bronze_table = "nyc.process_bronze.brz_yellow_nyc_taxi"
    target_silver_table = "nyc.process_silver.slv_yellow_nyc_taxi"  # 你的 Silver 目标表
    audit_table = "nyc.process_silver.pipeline_audit_log"           # 审计日志表
    checkpoint_schema = "nyc.process_silver"                        # Checkpoint 临时库存放地
    zone_dim_table = "nyc.process_gold.dim_taxi_zone_h3"                           # H3 空间维度表 (请根据你真实的表名替换)

    
    RESET_ENVIRONMENT = True 

    if RESET_ENVIRONMENT:
        logger.warning("[开发模式] 正在清理上一次的测试数据和表结构...")
        
        # 使用 Spark SQL 彻底删除目标表和审计表
        spark.sql(f"DROP TABLE IF EXISTS {target_silver_table}")
        spark.sql(f"DROP TABLE IF EXISTS {audit_table}")

        quarantine_table = target_silver_table + "_quarantine" 
        spark.sql(f"DROP TABLE IF EXISTS {quarantine_table}")
        
        logger.info("旧数据清理完毕，本次运行将从零开始构建全新表结构！") 
    
    # ==========================================
    # 2. 读取输入数据
    # ==========================================
    logger.info(f"📥 正在读取源数据: {bronze_table}")
    bronze_df = spark.read.table(bronze_table)
    
    logger.info(f"🗺️ 正在读取空间维度表: {zone_dim_table}")
    zone_dim_df = spark.read.table(zone_dim_table)

    # ==========================================
    # 3. 🌟 探路摸底模式 (强烈建议开启)
    # ==========================================
    # 第一次用真实数据跑时，不要拉取千万级全量数据。
    # 先抽取 1 万条数据验证逻辑是否崩溃，等落盘成功后，再注释掉这行跑全量。
    logger.info("⚠️ 当前处于探路模式，仅抽取 10,000 条数据进行跑批测试")
    
    TEST_MODE = False 


    if TEST_MODE:
        bronze_df = bronze_df.limit(10000)

    # ==========================================
    # 4. 实例化工业级编排器并执行
    # ==========================================
    pipeline = NYCTaxiSilverPipeline(
        spark=spark,
        run_id=run_id,
        zone_dim_df=zone_dim_df,
        target_table=target_silver_table,
        audit_table=audit_table,
        checkpoint_schema=checkpoint_schema
    )

    logger.info("⚡ 引擎点火，Pipeline 正式启动！")
    pipeline.process(bronze_df)
    
    logger.info(f"🎉 运行圆满结束！请前往 Databricks 网页端查询表 {target_silver_table} 验货。")

if __name__ == "__main__":
    main()