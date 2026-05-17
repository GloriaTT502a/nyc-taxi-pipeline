import logging
import re
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException 

# ==========================================
# 导入各个领域驱动的子模块
# ==========================================
from .standardization import ensure_bronze_schema, apply_transformations, apply_dq_and_split
from .h3_enrichment import enrich_h3_cells
from .surrogate_key import generate_trip_key
from .deduplication import deduplicate_trips
from .writer import SilverDeltaWriter

# 🌟 导入跨层复用的公共组件
from nyc_taxi_pipeline.common.auditor import PipelineAuditor
from nyc_taxi_pipeline.config.settings import PipelineConfig 

class NYCTaxiSilverPipeline:
    """
    工业级 NYC Taxi Silver 层核心编排器
    
    架构特性：
    1. 关注点分离 (SoC): 纯调度逻辑，业务规则下放至 domain 模块。
    2. 防御性编程: Checkpoint 截断超长血缘，防止 Executor OOM。
    3. 全方位观测: 集成 PipelineAuditor 实现数据质量(DQ)与运行指标的双写监控。
    """

    def __init__(
        self, 
        spark: SparkSession, 
        run_id: str, 
        zone_dim_df: DataFrame,
        target_table: str = None, 
        audit_table: str = None, 
        checkpoint_schema: str = None
    ):
        self.spark = spark
        self.run_id = run_id
        
        # 依赖注入：维度表从外部传入，解耦 Pipeline 对外部存储的强依赖
        self.zone_dim_df = zone_dim_df  
        
        # 路由配置 (可由外部覆写，默认读取 Config)
        self.target_table = target_table or PipelineConfig.get_table_path("target_silver", "silver")
        self.audit_table = audit_table or PipelineConfig.get_table_path("pipeline_metrics", "silver")
        self.quarantine_table = f"{self.target_table}_quarantine"
        
        # Checkpoint 配置
        self.checkpoint_schema = checkpoint_schema or "default"
        if "." in self.target_table and not checkpoint_schema:
            self.checkpoint_schema = ".".join(self.target_table.split(".")[:-1])
            
        # 实例化公共组件
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        self.auditor = PipelineAuditor(spark=self.spark, audit_table=self.audit_table)

    def process(self, bronze_df: DataFrame) -> None:
        """
        执行 Silver 层的核心数据流水线
        """
        self.logger.info(f"🚀 启动 Silver Pipeline | RunID: {self.run_id}")
        
        # 准备 Checkpoint 物理环境
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.checkpoint_schema}") 
        base_target = str(self.target_table).split("/")[-1].split(".")[-1]
        clean_chk_name = re.sub(r'[^a-zA-Z0-9_]', '_', f"chk_{base_target}_{self.run_id}")
        checkpoint_table = f"{self.checkpoint_schema}.{clean_chk_name}" 

        try:
            # ==========================================
            # 阶段 1：Schema 契约校验与基础转换 (含 total_amount 合成)
            # ==========================================
            self.logger.info("执行 Schema 校验与特征衍生...")
            schema_aligned_df = ensure_bronze_schema(bronze_df)
            transformed_df = apply_transformations(schema_aligned_df, self.run_id)

            # ==========================================
            # 阶段 2：数据质量校验 (DQ) 与数据分流 (基于 rules.yaml)
            # ==========================================
            self.logger.info("应用数据质量 (DQ) 规则...")
            valid_df, dq_rejected_df = apply_dq_and_split(transformed_df)

            # ==========================================
            # 阶段 3：空间维度增强 (H3) 与 代理主键生成
            # ==========================================
            self.logger.info("执行 H3 空间缝合与业务主键生成...")
            # 仅对通过了 DQ 校验的合法数据执行高成本的计算
            h3_enriched_df = enrich_h3_cells(valid_df, self.zone_dim_df)
            keyed_df = generate_trip_key(h3_enriched_df)

            # ==========================================
            # 阶段 4：强制物化 / DAG 截断 (Checkpointing)
            # ==========================================
            self.logger.info(f"🔄 触发 Checkpoint, 截断计算血缘: {checkpoint_table}")
            (
                keyed_df.write
                .mode("overwrite")
                .format("delta")
                .saveAsTable(checkpoint_table)
            )
            
            # 重新加载物化后的数据，后续的 Window 去重和 Merge 扫描将变得极快
            materialized_df = self.spark.read.table(checkpoint_table)

            # ==========================================
            # 阶段 5：跨批次级去重
            # ==========================================
            self.logger.info("执行绝对去重逻辑 (Deduplication)...")
            clean_df, dup_rejected_df = deduplicate_trips(materialized_df)

            # ==========================================
            # 阶段 6：性能指标聚合与 SLA 告警
            # ==========================================
            # 此处直接基于 Checkpoint 后的 DataFrame 计算，几乎是秒级返回
            metrics = materialized_df.select(
                F.count("*").alias("total_valid"),
                F.sum("is_pickup_fallback").alias("pu_fallback_count"),
                F.sum("is_dropoff_fallback").alias("do_fallback_count")
            ).collect()[0]

            valid_count = metrics["total_valid"] or 0
            dq_rejected_count = dq_rejected_df.count()   # 不符合业务规则的数据
            dup_rejected_count = dup_rejected_df.count() # 业务合规但是重复的数据
            total_rejected = dq_rejected_count + dup_rejected_count

            # H3 降级日志监控 (可选)
            self.logger.info(
                f"H3 降级计算统计 -> 上车点: {metrics['pu_fallback_count']} | 下车点: {metrics['do_fallback_count']}"
            )

            # ==========================================
            # 阶段 7：安全落盘写入
            # ==========================================
            if valid_count > 0:
                self.logger.info(f"开始执行 Silver 表 Upsert 写入: {self.target_table}")
                SilverDeltaWriter.upsert(
                    spark=self.spark, 
                    df=clean_df, 
                    table_name=self.target_table,
                    partition_col="YYYYMM"
                )

            # 统一将脏数据(DQ失败 + 去重失败)写入隔离区 (Quarantine Table)
            if total_rejected > 0:
                self.logger.info(f"将 {total_rejected} 条异常数据写入隔离区: {self.quarantine_table}")
                # unionByName 容忍两边 DataFrame 列顺序不一致或存在缺失列
                rejected_combined = dq_rejected_df.unionByName(dup_rejected_df, allowMissingColumns=True)
                rejected_combined.write.format("delta").mode("append").saveAsTable(self.quarantine_table)

            # ==========================================
            # 阶段 8：写入系统审计日志 (双写)
            # ==========================================
            self.auditor.log_run_metrics(
                run_id=self.run_id,
                layer="Silver",
                target_table=self.target_table,
                valid_count=valid_count - dup_rejected_count, # 最终真正干净的数据量
                rejected_count=total_rejected
            )
            
            self.logger.info(f"✅ Silver Pipeline 完美收官 | RunID={self.run_id}")

        except AnalysisException as e:
            self.logger.error(f"Spark 分析计划构建失败: {str(e)}")
            raise
        except Exception as e:
            self.logger.exception(f"Pipeline 执行期间发生致命错误: {str(e)}")
            raise
        finally: 
            # ==========================================
            # 阶段 9：垃圾回收 (GC)
            # ==========================================
            self.logger.info(f"🧹 正在清理临时 Checkpoint 资产...")
            try:
                self.spark.sql(f"DROP TABLE IF EXISTS {checkpoint_table}")
            except Exception as e:
                self.logger.warning(f"清理 Checkpoint 表失败，请稍后手动排查: {e}")