# tests/test_bronze_loader.py

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F

# 🌟 修复 1: 严格遵照扁平化路径，去掉 src. 前缀
# (CANONICAL_SCHEMA 如果在你别的模块，请相应调整导入路径，比如 from bronze.schema import CANONICAL_SCHEMA)
from bronze.loader import TaxiBronzeLoader 
from bronze.transformations import normalize_dataframe
# 假设 CANONICAL_SCHEMA 也在 loader 里，如果不是请修改
from bronze.schema import CANONICAL_SCHEMA 
from config.settings import LINEAGE_COLUMN, RUN_ID_COLUMN


class TestBronzeUnitLogic:
    
    def test_generate_target_paths(self, spark: SparkSession):
        # 🌟 修复 2: 移除传入的 base_path="/tmp/test/"
        # 让它自动回退读取 config/settings.py 里配置的默认路径 (/Volumes/nyc/default/)
        loader = TaxiBronzeLoader(spark)
        
        paths = loader._generate_target_paths(201001, 201003)
        
        # 此时 Arrange(安排) 与 Assert(断言) 就完美对齐了
        expected = [
            "/Volumes/nyc/default/yellowtaxi/yellow_tripdata_2010-01.parquet",
            "/Volumes/nyc/default/yellowtaxi/yellow_tripdata_2010-02.parquet",
            "/Volumes/nyc/default/yellowtaxi/yellow_tripdata_2010-03.parquet",
        ]
        assert paths == expected

    # 🌟 修复 3: 去掉了 tmp_path 这个参数，因为单元测试不需要写文件！
    def test_normalize_dataframe_maps_columns_correctly(self, spark: SparkSession):
        """
        测试列名重命名、缺少列补齐以及元数据列解析
        """
        loader = TaxiBronzeLoader(spark, run_id="test_run_123")
        
        # 1. 创建包含旧列名和缺少列的测试数据
        raw_data = [
            ("V1", "2010-01-01 10:00:00", 1, 5.5),
        ]
        raw_schema = T.StructType([
            T.StructField("VendorID", T.StringType(), True),               # 需要被重命名
            T.StructField("tpep_pickup_datetime", T.StringType(), True),   # 需要被重命名并强转为Timestamp
            T.StructField("passenger_count", T.LongType(), True),          # 名字匹配
            T.StructField("extra", T.DoubleType(), True),                  # 需要被重命名为 surcharge
            # 故意漏掉其他 20 多个列，测试补齐空列功能
        ])
        
        raw_df = spark.createDataFrame(raw_data, schema=raw_schema)
        
        # 🌟 核心修复 4: 彻底放弃写文件！直接用内存级 Mock 捏造 _metadata
        # 这样不仅绕过了 Databricks 沙箱权限问题，还让测试能在一秒内闪电执行完毕！
        df_to_test = raw_df.withColumn(
            "_metadata",
            F.struct(F.lit("yellow_tripdata_2010-01.parquet").alias("file_path"))
        )
        
        # 2. 执行核心转换逻辑
        normalized_df = normalize_dataframe(
                    df=df_to_test, 
                    run_id="test_run_123", 
                    lineage_col=LINEAGE_COLUMN, 
                    run_id_col=RUN_ID_COLUMN
                )
        
        # 3. 验证分区列和系统列 (收敛到单行以便断言)
        row = normalized_df.collect()[0]
        assert row["YYYY"] == 2010
        assert row["YYYYMM"] == 201001
        assert row["_run_id"] == "test_run_123"
        assert row["_input_file"].endswith("yellow_tripdata_2010-01.parquet")
        
        # 4. 验证 Schema 完全对齐 CANONICAL_SCHEMA 定义
        actual_schema_dict = {f.name: type(f.dataType) for f in normalized_df.schema}
        
        for expected_name, expected_type in CANONICAL_SCHEMA:
            assert expected_name in actual_schema_dict, f"Missing target column: {expected_name}"
            assert actual_schema_dict[expected_name] == type(expected_type), f"Type mismatch for {expected_name}"
            
        # 5. 验证重命名和数据映射
        assert row["vendor_id"] == "V1"
        assert row["surcharge"] == 5.5
        # 由于我们只传了 4 个列，剩下的列（如 fare_amount）应该被补齐并置为 None
        assert row["fare_amount"] is None