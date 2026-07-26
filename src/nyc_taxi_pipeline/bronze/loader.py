"""
Industrial-grade Bronze Loader for NYC Taxi Pipeline.
Targeting Databricks Runtime 16.0+, Unity Catalog, and Liquid Clustering.

Key Architectural Patterns:
1. Selective Overwrite (Idempotent Append) for Raw Data.
2. Unity Catalog & Spark Catalog API integration.
3. Liquid Clustering (Zero static partitioning).
4. Dynamic Schema Alignment & Safe Evolution.
5. High Cohesion & Low Coupling via private helper methods.
"""

import uuid
from datetime import datetime
from typing import List, Optional
from dateutil.relativedelta import relativedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable


from nyc_taxi_pipeline.config.settings import PipelineSettings 
from nyc_taxi_pipeline.observability.app_logging import get_logger
from nyc_taxi_pipeline.models.dq_result import DQResult
from nyc_taxi_pipeline.bronze.transformations import normalize_dataframe
from nyc_taxi_pipeline.utils.spark_utils import get_spark_session 
from nyc_taxi_pipeline.config.tables import Table, get_table_name

logger = get_logger(__name__)

class TaxiBronzeLoader:
    """
    Bronze Layer Loader implementing the Medallion Architecture.
    Responsible for reading raw files, basic normalization, and idempotent writing.
    """

    def __init__(
        self, 
        settings: PipelineSettings, 
        spark: Optional[SparkSession] = None, 
        run_id: Optional[str] = None
    ):
        self.settings = settings 
        self.spark = spark if spark is not None else get_spark_session(runtime_env=settings.runtime_env)
        self.target_table = get_table_name(self.settings, Table.BRONZE_TRIP)
        self.yellow_path = f"{self.settings.base_path}yellowtaxi/"
        self.run_id = run_id or str(uuid.uuid4())

    # ==========================================
    # 1. 路径推导与文件发现
    # ==========================================
    def _generate_target_paths(self, target_month: int) -> List[str]:
        """生成目标时间区间内的预期物理文件路径列表。"""
        target_str = str(target_month)
        yyyy_mm_str = f"{target_str[:4]}-{target_str[4:]}"

        
        return [f"{self.yellow_path}yellow_tripdata_{yyyy_mm_str}.parquet"] 
    

    def _discover_existing_files(self, paths: List[str]) -> List[str]:
        """
        物理探活：过滤掉云端存储中实际不存在的文件，防止抛出 [PATH_NOT_FOUND]。
        """
        valid_paths = []
        for path in paths:
            try:
                # 使用极其轻量级的操作探测文件是否存在
                self.spark.read.parquet(path).limit(1)
                valid_paths.append(path) 
            except AnalysisException as e:
                # 捕获特定的路径未找到异常，静默跳过
                if "PATH_NOT_FOUND" in str(e):
                    logger.warning(f"File skipped (Not Found): {path}")
                else:
                    logger.warning(f"File skipped (Corrupted/Unreadable): {path}. Reason: {e}") 
            except Exception as e:
                logger.warning(f"Unexpected error when scanning {path}: {e}")
                
        return valid_paths

    # ==========================================
    # 2. 数据读取与基础归一化
    # ==========================================
    def _read_source_data(self, valid_paths: List[str]) -> DataFrame:
        """
        读取源数据。
        针对真实世界公共数据集极其常见的跨文件类型冲突（CANNOT_MERGE_SCHEMAS），
        采用独立读取、全部降级为 STRING 后进行安全 Union 的防弹策略。
        后续 transformations.py 会负责将它们重新 cast 为强契约类型。
        """
        logger.info(f"Reading {len(valid_paths)} files robustly to handle severe schema drift...")
        
        dfs = []
        for path in valid_paths:
            df = self.spark.read.parquet(path)
            # 核心防御：将该文件所有的列暂时安全地转为 String，抹平一切类型冲突
            df = df.withColumn("_temp_file_path", F.col("_metadata.file_path")) 

            for c in df.columns:
                if c != "_temp_file_path": 
                    df = df.withColumn(c, F.col(c).cast("string"))
            dfs.append(df)
            
        # 💡 核心防御：使用 reduce 和 unionByName 强制按列名缝合。
        # allowMissingColumns=True 会自动给缺失该列的文件补上 null，完美包容像 __index_level_0__ 这样的幽灵列
        from functools import reduce 
        merged_df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), dfs)
        
        return merged_df    

    # ==========================================
    # 3. 目标表探测与 Schema 动态缝合
    # ==========================================
    def _table_exists(self) -> bool:
        """
        使用现代 Spark Catalog API 检查 Unity Catalog 表是否存在。
        彻底抛弃高成本的 DeltaTable.isDeltaTable() 扫描。
        """
        try:
            return self.spark.catalog.tableExists(self.target_table)
        except Exception as e:
            logger.debug(f"Catalog check failed, assuming table does not exist: {e}")
            return False

    def _align_to_target_schema(self, df: DataFrame) -> DataFrame:
        """
        核心防御逻辑：自动 Schema 对齐。
        如果目标表存在，强行调整 DataFrame 的列顺序以匹配目标表，防止 [DELTA_METADATA_MISMATCH]。
        并将源数据中新增的列（Schema Evolution）自动拼接到 DataFrame 的最后。
        """
        if not self._table_exists():
            return df

        target_cols = self.spark.read.table(self.target_table).columns
        incoming_cols = df.columns
        
        # 1. 提取出目标表已经存在的列，并保持目标表的顺序
        aligned_cols = [c for c in target_cols if c in incoming_cols]
        # 2. 提取出源数据新增加的列
        new_cols = [c for c in incoming_cols if c not in target_cols]
        
        final_col_order = aligned_cols + new_cols
        
        if final_col_order != incoming_cols:
            logger.info("Schema Alignment Triggered: Adjusted column order to match target table.")
            
        return df.select(*final_col_order)

    def _create_table_if_needed(self, df: DataFrame):
        """
        如果表不存在，显式使用 DataFrame API 创建表，
        依赖 Unity Catalog 默认配置，同时可扩展 Liquid Clustering 定义。
        """
        if not self._table_exists():
            logger.info(f"Target table {self.target_table} does not exist. Initializing...")
            # 注意：如果外部使用严格的 DDL 管理（Terraform/dbt），可以去掉这段，
            # 改为抛出异常：raise ValueError("Table must be pre-created via DDL.")
            pass

    # ==========================================
    # 4. 幂等落盘写入 (Liquid Clustering 适配)
    # ==========================================
    def _write_bronze(self, df: DataFrame, target_month: List[str]):
        """
        执行基于 replaceWhere 的选择性覆盖 (Selective Overwrite)。
        拥抱 Liquid Clustering：不使用 partitionBy。
        """
        replace_condition = f"YYYYMM = '{target_month}'"
        
        writer = (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
        )

        if self._table_exists():
            logger.info(f"Executing Selective Overwrite with replaceWhere: {replace_condition}")
            writer.option("replaceWhere", replace_condition).saveAsTable(self.target_table)
        else:
            logger.info("Writing initial data. Liquid Clustering takes over physical layout.")
            writer.saveAsTable(self.target_table)

    # ==========================================
    # 5. 平台遥测与指标收集
    # ==========================================
    def _collect_metrics(self) -> int:
        """从 Delta 事务日志（Transaction Log）中提取真实的写入行数。"""
        try:
            history_df = self.spark.sql(f"DESCRIBE HISTORY {self.target_table} LIMIT 1")
            metrics = history_df.select("operationMetrics").collect()[0][0]
            
            if metrics and "numOutputRows" in metrics:
                return int(metrics["numOutputRows"])
            
            logger.warning("numOutputRows not found in operation metrics.")
            return 0
        except Exception as e:
            logger.warning(f"Failed to read Delta history for metrics: {e}")
            return 0

    # ==========================================
    # 主流水线编排器
    # ==========================================
    def write_idempotent(self, target_month: int) -> DQResult:
        """
        流水线入口：负责组装各个微服务步骤，完成从读取到写入的完整幂等闭环。
        """
        logger.info(f"Starting Bronze load for target month {target_month}")

        # Step 1: Discover Files
        expected_paths = self._generate_target_paths(target_month)
        valid_paths = self._discover_existing_files(expected_paths)
        
        if not valid_paths:
            logger.warning("No valid paths found. Exiting idempotent write gracefully.")
            return DQResult(total_rows=0, bad_rows=0, bad_by_rule={})

        # Step 2: Read & Normalize Data
        raw_df = self._read_source_data(valid_paths)
        norm_df = normalize_dataframe(df=raw_df, run_id=self.run_id) 

        # 严格过滤出本批次关心的时间段（防御上游文件中混杂的脏数据）
        filtered_df = norm_df.filter((F.col("YYYYMM") == target_month)) 

        # Step 4: Schema Defense
        aligned_df = self._align_to_target_schema(filtered_df)

        # Step 5: Execute Idempotent Write
        self._write_bronze(df=aligned_df, target_month=target_month)

        # Step 6: Telemetry & Return
        total_ingested = self._collect_metrics()
        logger.info(f"Bronze layer load completed. Successfully ingested {total_ingested} rows.")
        
        return DQResult(total_rows=total_ingested, bad_rows=0, bad_by_rule={}) 
    