import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from pyspark.sql import SparkSession
from datetime import datetime, timezone
import uuid
from pyspark.sql.utils import AnalysisException 
import logging 

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
        target_table: str, 
        audit_table: str = "nyc.process_silver.pipeline_metrics", 
        checkpoint_dir_prefix: str = "/Volumes/nyc/process_silver/checkpoint"
        ):
        self.spark = spark
        self.target_table = target_table
        self.quarantine_table = f"{target_table}_quarantine"
        self.audit_table = audit_table 
        self.run_id = run_id

        self.checkpoint_dir = f"{checkpoint_dir_prefix}/run_id={self.run_id}" 

        #self.spark.sparkContext.setCheckpointDir(checkpoint_dir) 

        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        
        self.EXPECTED_BRONZE_COLS = [
            "vendor_id",
            "pickup_datetime",
            "dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "rate_code",
            "store_and_fwd_flag",
            "pickup_longitude",
            "pickup_latitude",
            "dropoff_longitude",
            "dropoff_latitude",
            "PULocationID",
            "DOLocationID",
            "payment_type",
            "fare_amount",
            "surcharge",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "improvement_surcharge",
            "congestion_surcharge",
            "airport_fee",
            "cbd_congestion_fee",
            "total_amount",
            "YYYY",
            "YYYYMM",
            "_run_id",
            "_load_timestamp",
            "_input_file"
        ]
        # 优化：将业务规则封装，便于后续作为配置文件(如 JSON/YAML)动态加载
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
        self._validate_schema(bronze_df)
        
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
        self.logger.info(f"Starting silver pipeline. run_id={self.run_id}")
        """
        主处理流程：特征提取 -> 规则校验 -> 持久化缓存 -> 数据分流写入
        """
        try: 
            enriched_df = self.apply_transformations(bronze_df) 
            dq_df = self.apply_dq_rules(enriched_df) 

            self.logger.info("Triggering Checkpoint to truncate lineage and materialize data...") 
            # Write to checkpoint path
            (
                dq_df.write
                .format("delta")
                .mode("overwrite")
                .save(self.checkpoint_dir)
            ) 
            # Read data from checkpoint path 
            checkpointed_dq_df = self.spark.read.format("delta").load(self.checkpoint_dir) 

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
            self.logger.info(f"Cleaning up temporary checkpoint directory: {self.checkpoint_dir}")
            try:
                # 尝试使用 dbutils 删除
                from pyspark.dbutils import DBUtils
                dbutils = DBUtils(self.spark)
                dbutils.fs.rm(self.checkpoint_dir, recurse=True)
                self.logger.info("Cleanup successful. Checkpoint folder removed.")
            except Exception as e:
                self.logger.warning(f"Failed to clean up checkpoint dir: {e}")


    def _write_to_delta(self, df: DataFrame, table_name: str) -> None:
        # 此时 df 已经被 persist，这里的 collect() 是极速的，只扫描内存中的数据
        # partitions_rows = df.select("YYYYMM").distinct().collect()
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

        partitions = [str(p) for p in partitions_rows]
        replace_cond = (
            f"YYYYMM IN ({','.join(partitions)})"
        )
        
        # 使用 Spark 3.x / Delta Lake 的原生动态分区覆盖也是一种选择
        # 但在生产环境中，显式指定 replaceWhere 更加安全，能防止意料之外的全局覆盖
        (
            df.write 
                .format("delta") 
                .mode("overwrite") 
                .option("replaceWhere", replace_cond) 
                .saveAsTable(table_name)
        )
        self.logger.info(f"Successfully loaded data to {table_name} for partitions {partitions}") 

    def _write_metrics(self, valid_count: int, rejected_count: int, rejected_ratio: float) -> None:
        metrics_df = self.spark.createDataFrame(
            [(self.run_id, self.target_table, valid_count, rejected_count, rejected_ratio)], 
            schema="run_id STRING, target_table STRING, valid_count LONG, rejected_count LONG, rejected_ratio DOUBLE"
        ).withColumn("created_at", F.current_timestamp())

        metrics_df.write.format("delta").mode("append").saveAsTable(self.audit_table)

    def _validate_schema(self, bronze_df: DataFrame) -> None: 
        missing_cols = set(self.EXPECTED_BRONZE_COLS) - set(bronze_df.columns)
        if missing_cols: 
            raise ValueError(f"CRITICAL ERROR: Missing columns: {missing_cols}")
        






