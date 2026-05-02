import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window # 如果需要复杂的去重逻辑备用

from pyspark.sql import SparkSession
from datetime import datetime, timezone
import uuid

class NYC_Taxi_Silver_Loader:
    def __init__(self, spark, run_id, target_table: str):
        self.spark = spark
        self.target_table = target_table
        self.quarantine_table = f"{target_table}_quarantine"
        self.run_id = run_id
        
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
        missing_cols = set(self.EXPECTED_BRONZE_COLS) - set(bronze_df.columns)
        if missing_cols: 
            raise ValueError(f"CRITICAL ERROR: missing columns: {missing_cols}")
        
        base_cols = []
        for c in self.EXPECTED_BRONZE_COLS:
            if c == "_run_id":
                base_cols.append(F.col(c).alias("brz_run_id"))
            elif c == "_load_timestamp": 
                base_cols.append(F.col(c).alias("bnz_load_timestamp"))
            else:
                base_cols.append(F.col(c)) 

  
        
        # Get Features and Audit Metadata
        enriched_df = (
            bronze_df.select(*base_cols,
                            ((F.col("dropoff_datetime").cast("long") - F.col("pickup_datetime").cast("long"))/60).alias("duration_min"),
                            F.lit(self.run_id).alias("_run_id"), 
                            F.current_timestamp().alias("_processed_at")
        )
        )

        enriched_df = enriched_df.withColumn(
            "temp_eff", 
            F.when(
                F.col("duration_min") > 0, 
                F.col("fare_amount") / F.col("duration_min")
            ).otherwise(F.lit(0.0))
        ) 

        return enriched_df 
    

    def apply_dq_rules(self, enriched_df: DataFrame) -> DataFrame: 
        rule_evaluations = [
            F.when(condition, F.lit(rule_name)).otherwise(F.lit(None))
            for rule_name, condition in self.BASE_RULES.items()
        ]

        # 2. 生成原始大数组，并用高阶函数 F.filter 清洗
        dq_plan = enriched_df.withColumn(
            "raw_rules_array", 
            F.array(*rule_evaluations) # 生成如 ["fare_negative", null, "passenger_invalid"]
        ).withColumn(
            "violated_rules", 
            # 核心魔法：使用 Lambda 表达式遍历数组，只保留不为 null 的元素
            F.filter(F.col("raw_rules_array"), lambda x: x.isNotNull())
        ).withColumn(
            "is_valid", 
            # 此时 violated_rules 绝对是纯正的 ArrayType，可以安全使用 size
            F.size(F.col("violated_rules")) == 0
        ).drop("raw_rules_array") 

        return dq_plan 
    
    def _split_data(self, dq_df: DataFrame): 
        # split valid and invalid data
        valid_df = dq_df.filter(F.col("is_valid") == True).drop("violated_rules", "is_valid")
        rejected_df = dq_df.filter(F.col("is_valid") == False) 

        return valid_df, rejected_df 
    
    
    def process(self, bronze_df: DataFrame) -> None:
        """
        主处理流程：特征提取 -> 规则校验 -> 持久化缓存 -> 数据分流写入
        """
        enriched_df = self.apply_transformations(bronze_df) 
        dq_df = self.apply_dq_rules(enriched_df) 

        temp_checkpoint_path = f"/Volumes/nyc/process_silver/checkpoint/{self.run_id}"

        dq_df.write.format("delta").mode("overwrite").save(temp_checkpoint_path) 

        materialized_dq_df = self.spark.read.format("delta").load(temp_checkpoint_path)

        try: 
            valid_df, rejected_df = self._split_data(materialized_dq_df) 

            valid_count = valid_df.count()
            rejected_count = rejected_df.count()

            print(f"Valid count: {valid_count}")
            print(f"Rejected count: {rejected_count}") 
            
            if valid_count > 0: 
                self._write_to_delta(valid_df, self.target_table) 
            if rejected_count > 0: 
                self._write_to_delta(rejected_df, self.quarantine_table) 

        finally: 
            print(">> Clean the materialized data")
            try:
                # 尝试使用 dbutils 删除
                from pyspark.dbutils import DBUtils
                dbutils = DBUtils(self.spark)
                dbutils.fs.rm(temp_checkpoint_path, recurse=True)
            except Exception as e:
                print(f"Warning: auto clean failed. Please check later: {e}") 


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
            print(f"No data to write for {table_name}. Skipping.")
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
        
        
        
        # 优化：去掉了原版的 df.count()，因为它会再次触发 Action
        print(f"Successfully loaded data to {table_name} for partitions {partitions}")





