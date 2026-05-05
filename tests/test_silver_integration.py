import pytest
import uuid
import datetime
import pyspark.sql.functions as F
from nyc_taxi_pipeline.nyc_taxi_silver import NYC_Taxi_Silver_Loader
from nyc_taxi_pipeline.config.settings import PipelineConfig

class TestSilverIntegration:

    def test_full_pipeline_and_partition_overwrite(self, spark):

        PipelineConfig.CATALOG = ""  # 本地测试强制设空，规避多层级报错
        PipelineConfig.DATABASE["silver"] = "process_silver"

        spark.sql(f"CREATE DATABASE IF NOT EXISTS {PipelineConfig.DATABASE['silver']}")
        # ==========================================================
        # 1. 彻底抛弃本地路径，使用 UC 托管表名 (完美避开 /tmp 报错)
        # ==========================================================
        test_suffix = uuid.uuid4().hex[:6]
        
        # 这里就是传给 Loader 的 target_table！绝对没有任何 /tmp/ 路径！
        target_table = PipelineConfig.get_table_path(f"target_silver_test_{test_suffix}", "silver")
        audit_table = PipelineConfig.get_table_path(f"audit_metrics_test_{test_suffix}", "silver")

        #spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        # ==========================================================
        # 2. 初始化 Loader
        # ==========================================================
        loader = NYC_Taxi_Silver_Loader(
            spark,
            run_id="run_int_01",
            target_table=target_table,
            audit_table=audit_table
        )

        # ==========================================================
        # 3. 构造完美的测试数据 (解决 valid=0 的问题)
        # ==========================================================
        dt1 = datetime.datetime(2024, 1, 1, 10, 0, 0)
        dt2 = datetime.datetime(2024, 1, 1, 10, 15, 0) # 行程 15 分钟

        cols = ["vendor_id", "pickup_datetime", "dropoff_datetime", "fare_amount", "trip_distance", "YYYYMM", "total_amount"]
        
        bronze_data = [
            ("1", dt1, dt2, 10.0, 2.0, 202401, 10.0),  # 完美数据，将被放行
            ("2", dt1, dt2, 10.0, 2.0, 202401, -5.0),  # 金额为负，将被拒绝进隔离区
            ("3", dt1, dt2, 10.0, 2.0, 202402, 20.0)   # 完美数据，将被放行
        ]

        initial_df = spark.createDataFrame(bronze_data, cols)

        # ==========================================================
        # 4. 执行测试流水线
        # ==========================================================
        try:
            # 首次运行
            loader.process(initial_df)

            # 验证第一次写入 (此时应该有2条合规数据，1条进隔离区)
            silver_df = spark.read.table(target_table)
            assert silver_df.count() == 2

            initial_vendor_ids = {row["vendor_id"] for row in silver_df.select("vendor_id").collect()}
            assert "1" in initial_vendor_ids
            assert "3" in initial_vendor_ids
            assert "2" not in initial_vendor_ids

            # --- 重试覆盖逻辑测试 ---
            new_bronze_data = [
                # 4号：用来动态覆盖 202401 分区的新数据
                ("4", dt1, dt2, 20.0, 5.0, 202401, 100.0)
            ]
            retry_df = spark.createDataFrame(new_bronze_data, cols)

            loader_retry = NYC_Taxi_Silver_Loader(
                spark,
                run_id="run_int_02",
                target_table=target_table,
                audit_table=audit_table
            )
            loader_retry.process(retry_df)

            # 最终验证
            final_silver_df = spark.read.table(target_table)
            assert final_silver_df.count() == 2

            final_rows = final_silver_df.select("vendor_id", "YYYYMM", "total_amount").collect()
            final_data = {(row["vendor_id"], row["YYYYMM"], row["total_amount"]) for row in final_rows}

            assert ("1", 202401, 10.0) not in final_data
            assert ("4", 202401, 100.0) in final_data
            assert ("3", 202402, 20.0) in final_data

        finally:
            # ==========================================================
            # 5. 清理测试表
            # ==========================================================
            spark.sql(f"DROP TABLE IF EXISTS {target_table}")
            spark.sql(f"DROP TABLE IF EXISTS {target_table}_quarantine")
            spark.sql(f"DROP TABLE IF EXISTS {audit_table}")