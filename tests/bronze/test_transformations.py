import pytest
from nyc_taxi_pipeline.bronze.transformations import normalize_dataframe
from tests.helpers import create_input_df
from pyspark.sql import functions as F

class TestNormalizeDataFrame:
    """
    测试 normalize_dataframe 的数据标准化与衍生逻辑
    """

    @pytest.fixture(scope="function")
    def processed_data(self, spark):
        """
        核心夹具 (Fixture)：统一准备数据并执行转换。
        """
        # 1. 确保输入的 DataFrame 包含 _temp_file_path，否则无法提取日期
        df = create_input_df(spark)
        if "_temp_file_path" not in df.columns:
            df = df.withColumn("_temp_file_path", F.lit("yellow_tripdata_2010-01.parquet"))
        
        # 2. 传入匹配的参数：df 和 run_id
        result_df = normalize_dataframe(
            df,
            run_id="test-run-id"
        )
        
        return result_df, result_df.collect()[0]

    def test_business_columns_retained(self, processed_data):
        """测试原有的业务字段是否被无损保留"""
        _, row = processed_data
        
        assert row.vendor_id == "1"
        assert row.passenger_count == 2
        assert row.total_amount == 30.5

    def test_time_partitions_extracted(self, processed_data):
        """测试时间分区字段 (YYYY, YYYYMM) 是否被正确提取"""
        _, row = processed_data
        
        assert row.YYYY == 2010
        assert row.YYYYMM == 201001

    def test_system_audit_columns_injected(self, processed_data):
        """测试底层血缘与审计字段是否成功注入"""
        _, row = processed_data
        
        # 🌟 修复：使用从 Row 中实际看到的字段名 _bronze_run_id
        assert row._bronze_run_id == "test-run-id"
        assert row._input_file == "yellow_tripdata_2010-01.parquet"

    def test_expected_schema_integrity(self, processed_data):
        """测试最终输出的 DataFrame 契约"""
        result_df, _ = processed_data
        
        # 🌟 修复：明确我们需要验证的核心字段（其余字段通过 Schema Evolution 自动保留）
        expected_mandatory_columns = {
            "vendor_id",
            "passenger_count",
            "total_amount",
            "YYYY",
            "YYYYMM",
            "_bronze_run_id",  # 对应实际注入的列名
            "_input_file"
        }
        
        actual_columns = set(result_df.columns)
        
        # 使用 issubset 校验核心字段依然存在
        assert expected_mandatory_columns.issubset(actual_columns), \
            f"Missing columns! Expected {expected_mandatory_columns} to be in {actual_columns}"
        