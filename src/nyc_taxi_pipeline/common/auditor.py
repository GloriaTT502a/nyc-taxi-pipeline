import logging
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

class PipelineAuditor:
    """
    企业级 Pipeline 审计器
    负责集中处理数据可观测性 (Data Observability) 埋点，支持双写机制。
    """
    
    def __init__(self, spark: SparkSession, audit_table: str):
        self.spark = spark
        self.audit_table = audit_table

    def log_run_metrics(
        self, 
        run_id: str, 
        layer: str,           # 标明是 Bronze, Silver 还是 Gold
        target_table: str, 
        valid_count: int, 
        rejected_count: int
    ) -> None:
        """
        记录 Pipeline 批次运行质量指标
        """
        # 1. 计算拒绝率
        total_processed = valid_count + rejected_count
        rejected_ratio = (rejected_count / total_processed) if total_processed > 0 else 0.0

        # 2. 写入标准控制台日志 (给开发看)
        logger.info(
            f"📊 审计指标 [{layer}] | RunID: {run_id} | "
            f"表: {target_table} | "
            f"成功: {valid_count} | 拒绝: {rejected_count} | "
            f"拒绝率: {rejected_ratio:.2%}"
        )

        # 3. 持久化到 Delta 审计表 (给看板和系统看)
        try:
            # 加入了 layer 字段，方便以后在看板上按层级筛选
            schema = "run_id STRING, layer STRING, target_table STRING, valid_count LONG, rejected_count LONG, rejected_ratio DOUBLE"
            
            metrics_df = self.spark.createDataFrame(
                [(run_id, layer, target_table, valid_count, rejected_count, float(rejected_ratio))], 
                schema=schema
            ).withColumn("created_at", F.current_timestamp())

            (
                metrics_df.write
                .format("delta")
                .mode("append")
                .saveAsTable(self.audit_table)   
            )
            logger.info(f"✅ 审计指标已成功写入 Delta 表: {self.audit_table}")
            
        except Exception as e:
            logger.warning(f"⚠️ 审计表写入失败，跳过以保证主流程不中断: {e}")