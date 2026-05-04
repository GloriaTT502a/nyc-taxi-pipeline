import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from pyspark.sql import SparkSession
from datetime import datetime, timezone
import uuid
from pyspark.sql.utils import AnalysisException 
import logging 
import re
from src.config import PipelineConfig 

class NYC_Taxi_Silver_Loader:
    """
    Production-grade NYC Taxi Silver Loader

    Architectural Highlights:
    - DAG Truncation via Checkpoint (Fault tolerance for deep lineage)
    - Single-Pass Metric Aggregation (Optimized DAG execution)
    - SLA Monitoring & Audit Logging (Data Observability)
    - Safe Dynamic Partition Overwrite (Idempotent writes)
    """
    def __init__(
        self, 
        spark, 
        run_id: str, 
        target_table: str = None, 
        audit_table: str = None, 
        checkpoint_schema: str = None,
        ):
        self.spark = spark
        self.target_table = target_table or PipelineConfig.get_table_path("target_silver")
        self.audit_table = audit_table or PipelineConfig.get_table_path("pipeline_metrics")
        
        self.quarantine_table = f"{self.target_table}_quarantine"
        self.run_id = run_id

        if checkpoint_schema is None:
            if "." in self.target_table:
                checkpoint_schema = ".".join(self.target_table.split(".")[:-1])
            else:
                checkpoint_schema = "default"

        self.checkpoint_schema = checkpoint_schema 

        #self.spark.sparkContext.setCheckpointDir(checkpoint_dir) 

        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        
        self.BRONZE_SCHEMA = {
            "vendor_id": {"type": "string", "required": False},
            "pickup_datetime": {"type": "timestamp", "required": False},
            "dropoff_datetime": {"type": "timestamp", "required": False},
            "passenger_count": {"type": "integer", "required": False},
            "trip_distance": {"type": "double", "required": False},
            "rate_code": {"type": "integer", "required": False},
            "store_and_fwd_flag": {"type": "string", "required": False},
            "pickup_longitude": {"type": "double", "required": False},
            "pickup_latitude": {"type": "double", "required": False},
            "dropoff_longitude": {"type": "double", "required": False},
            "dropoff_latitude": {"type": "double", "required": False},
            "PULocationID": {"type": "integer", "required": False},
            "DOLocationID": {"type": "integer", "required": False},
            "payment_type": {"type": "string", "required": False},
            "fare_amount": {"type": "double", "required": False},
            "surcharge": {"type": "double", "required": False},
            "mta_tax": {"type": "double", "required": False},
            "tip_amount": {"type": "double", "required": False},
            "tolls_amount": {"type": "double", "required": False},
            "improvement_surcharge": {"type": "double", "required": False},
            "congestion_surcharge": {"type": "double", "required": False},
            "airport_fee": {"type": "double", "required": False},
            "cbd_congestion_fee": {"type": "double", "required": False},
            "total_amount": {"type": "double", "required": True},
            "YYYY": {"type": "integer", "required": False},
            "YYYYMM": {"type": "integer", "required": True},
            "_run_id": {"type": "string", "required": False},
            "_load_timestamp": {"type": "timestamp", "required": False},
            "_input_file": {"type": "string", "required": False}
        }

        self.EXPECTED_BRONZE_COLS = list(self.BRONZE_SCHEMA.keys())

        # Optimization: Encapsulate business rules for easier dynamic loading as configuration files (such as JSON/YAML) later. 
        self.BASE_RULES = {
            "missing_pickup": F.col("pickup_datetime").isNull(),
            "missing_dropoff": F.col("dropoff_datetime").isNull(),
            "dropoff_before_pickup": (F.col("pickup_datetime").isNotNull()) & 
                                     (F.col("dropoff_datetime").isNotNull()) & 
                                     (F.col("dropoff_datetime") < F.col("pickup_datetime")),
            "passenger_count_invalid": (F.col("passenger_count") < 0) | (F.col("passenger_count") > 9),
            "total_amount_negative": F.col("total_amount") < 0,
            "invalid_YYYYMM": (F.col("YYYYMM") < 190001) | (F.col("YYYYMM") > 300012),
            "duration_out_of_range": (F.col("duration_min") < 2.0) | (F.col("duration_min") > 180),
            "distance_too_small": F.col("trip_distance") <= 0.1,
            "efficiency_too_high": F.col("temp_eff") >= 15.0,
            "fare_too_low": F.col("fare_amount") <= 2.5
        }

    def apply_transformations(self, bronze_df: DataFrame) -> DataFrame: 
        # Check whether any columns missing
        bronze_df = self._ensure_schema(bronze_df)
        
        base_cols = [
            F.col(c).alias("bronze_run_id") if c == "_run_id" else 
            F.col(c).alias("bronze_load_timestamp") if c == "_load_timestamp" else 
            F.col(c) for c in self.EXPECTED_BRONZE_COLS 
        ]
        
        # Get Features and Audit Metadata
        enriched_df = (
            bronze_df.select(*base_cols,
                            ((F.col("dropoff_datetime").cast("long") - F.col("pickup_datetime").cast("long"))/60).alias("duration_min"),
                            F.lit(self.run_id).alias("_run_id"), 
                            F.current_timestamp().alias("_processed_at")
                    ).withColumn(
                        "temp_eff", 
                        F.when(
                            F.col("duration_min") > 0, F.col("fare_amount") / F.col("duration_min")
                        ).otherwise(F.lit(0.0))
                    )
        )

        return enriched_df 
    

    def apply_dq_rules(self, enriched_df: DataFrame) -> DataFrame: 
        rule_evaluations = [
            F.when(condition, F.lit(rule_name)).otherwise(F.lit(None).cast("string"))
            for rule_name, condition in self.BASE_RULES.items()
        ]

        # 2. Use filter to clean data
        dq_plan = enriched_df.withColumn(
            "raw_rules_array", 
            F.array(*rule_evaluations) # generate ["fare_negative", null, "passenger_invalid"]
        ).withColumn(
            "violated_rules", 
            # Use lambda to remain not null values
            F.filter(F.col("raw_rules_array"), lambda x: x.isNotNull())
        ).withColumn(
            "is_valid", 
            # Generate is_valid flag base on size 
            F.size(F.col("violated_rules")) == 0
        ).drop("raw_rules_array") 

        return dq_plan 
    
    def _split_data(self, dq_df: DataFrame): 
        # split valid and invalid data
        valid_df = dq_df.filter(F.col("is_valid") == True).drop("violated_rules", "is_valid")
        rejected_df = dq_df.filter(F.col("is_valid") == False) 

        return valid_df, rejected_df 
    
    
    def process(self, bronze_df: DataFrame) -> None: 
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.checkpoint_schema}") 

        self.logger.info(f"Starting silver pipeline. run_id={self.run_id}")
        """
        主处理流程：特征提取 -> 规则校验 -> 持久化缓存 -> 数据分流写入
        """

        # 1. 剥离可能存在的路径前缀或 catalog.schema，只保留最后的表名核心
        base_target = str(self.target_table).split("/")[-1].split(".")[-1]
        
        # 2. 拼接原始表名 (包含 run_id)
        raw_chk_name = f"chk_{base_target}_{self.run_id}"
        
        # 3. 核心净化：将所有非字母、数字、下划线的字符全部替换为下划线 "_"
        clean_chk_name = re.sub(r'[^a-zA-Z0-9_]', '_', raw_chk_name)
        
        # 4. 组装成最终的 UC 临时表名
        self.checkpoint_table = f"{self.checkpoint_schema}.{clean_chk_name}" 


        try: 
            enriched_df = self.apply_transformations(bronze_df) 
            dq_df = self.apply_dq_rules(enriched_df) 

            self.logger.info("Triggering Checkpoint to truncate lineage and materialize data...") 
            
            (
                dq_df.write
                .mode("overwrite")
                .format("delta")
                .saveAsTable(self.checkpoint_table)
            )

            checkpointed_dq_df = self.spark.read.table(self.checkpoint_table)

            metrics_rows = checkpointed_dq_df.groupBy("is_valid").count().collect() 

            valid_count, rejected_count = 0, 0
            for row in metrics_rows:
                if row["is_valid"]: valid_count = row["count"]
                else: rejected_count = row["count"]
            
            self.logger.info(f"DQ completed | valid={valid_count} | rejected={rejected_count}") 

            # 4. SLA and monitor data quality
            total_count = valid_count + rejected_count
            rejected_ratio = (rejected_count / total_count) if total_count > 0 else 0 

            if rejected_ratio > 0.2: 
                self.logger.warning(f"High rejection ratio detected: {rejected_ratio:.2%}")

            valid_df, rejected_df = self._split_data(checkpointed_dq_df)  
            
            # Split data and write to delta table
            if valid_count > 0: 
                self._write_to_delta(valid_df, self.target_table) 
            if rejected_count > 0: 
                self._write_to_delta(rejected_df, self.quarantine_table) 

            self._write_metrics(valid_count, rejected_count, rejected_ratio) 
            self.logger.info(f"Silver pipeline completed successfully for run_id={self.run_id}")
        except AnalysisException as e:
            self.logger.error(f"Spark analysis exception: {str(e)}")
            raise
        except Exception as e:
            self.logger.exception(f"Pipeline failed: {str(e)}")
            raise
        finally: 
            self.logger.info(f"Cleaning up temporary checkpoint table: {self.checkpoint_table}")
            try:
                # 核心修复：清理表而不是清理目录
                self.spark.sql(f"DROP TABLE IF EXISTS {self.checkpoint_table}")
                self.logger.info("Cleanup successful. Checkpoint table dropped.")
            except Exception as e:
                self.logger.warning(f"Failed to clean up checkpoint table: {e}")


    def _write_to_delta(self, df: DataFrame, table_name: str) -> None:
        # 此时 df 已经被 persist，这里的 collect() 是极速的，只扫描内存中的数据
        # partitions_rows = df.select("YYYYMM").distinct().collect()
        
        partition_type = self.BRONZE_SCHEMA["YYYYMM"]["type"] 

        partitions_rows = [
            row["YYYYMM"]
            for row in (
                df.select("YYYYMM")
                .distinct()
                .toLocalIterator()
            )
        ]
        
        if not partitions_rows:
            self.logger.warning(f"No data to write for {table_name}")
            return

        if partition_type == "string":
            partition_values = ",".join(
                [f"'{p}'" for p in partitions_rows]
            )
        else:
            partition_values = ",".join(
                map(str, partitions_rows)
            )     

        replace_cond = f"YYYYMM IN ({partition_values})"                              
        
        # 使用 Spark 3.x / Delta Lake 的原生动态分区覆盖也是一种选择
        # 但在生产环境中，显式指定 replaceWhere 更加安全，能防止意料之外的全局覆盖
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("replaceWhere", replace_cond)
            .saveAsTable(table_name)   # ✅ 永远用表
        ) 

        self.logger.info(f"Successfully loaded data to {table_name} for partitions {partition_values}") 

    def _write_metrics(self, valid_count: int, rejected_count: int, rejected_ratio: float) -> None:
        metrics_df = self.spark.createDataFrame(
            [(self.run_id, self.target_table, valid_count, rejected_count, rejected_ratio)], 
            schema="run_id STRING, target_table STRING, valid_count LONG, rejected_count LONG, rejected_ratio DOUBLE"
        ).withColumn("created_at", F.current_timestamp())

        (
            metrics_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(self.audit_table)   
        )   

    def _ensure_schema(self, bronze_df: DataFrame) -> DataFrame:

        normalized_df = bronze_df

        for col_name, meta in self.BRONZE_SCHEMA.items():

            col_type = meta["type"]
            required = meta["required"]

            if col_name not in normalized_df.columns:

                if required:
                    raise ValueError(
                        f"Missing required column: {col_name}"
                    )

                normalized_df = normalized_df.withColumn(
                    col_name,
                    F.lit(None).cast(col_type)
                )

        return normalized_df
        






