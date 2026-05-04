import pytest
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType, TimestampType
from chispa.dataframe_comparer import assert_df_equality
from src.nyc_taxi_silver import NYC_Taxi_Silver_Loader
import datetime

class TestSilverUnitLogic:
    
    # =========================================================================
    # Unit Test 1: Schema Validation (Defense Mechanism Test)
    # =========================================================================
    def test_schema_validation_rejects_missing_columns(self, spark):
        print(f"DEBUG: Executing test on Spark Version {spark.version}")
        
        loader = NYC_Taxi_Silver_Loader(spark, run_id="run_test_001")
        
        # Intentionally providing only 'vendor_id', missing the required 'total_amount' and 'YYYYMM'
        bad_bronze_df = spark.createDataFrame([("V1",)], ["vendor_id"])
        
        # 修复点 1 & 2：修改方法名为 _ensure_schema，匹配正确的异常文案
        with pytest.raises(ValueError, match="Missing required column"):
            loader._ensure_schema(bad_bronze_df)

    # =========================================================================
    # Unit Test 2: DQ Rules Engine (Poison Pill Interception Test)
    # =========================================================================
    def test_dq_rules_correctly_flags_bad_records(self, spark):
        loader = NYC_Taxi_Silver_Loader(spark, "run_test_002", "silver_taxi_trips")
        
        # 修复点 3：为了让 apply_transformations 能算出 duration_min 和 temp_eff，必须给合法的时间戳
        valid_pickup = datetime.datetime(2024, 1, 1, 10, 0, 0)
        valid_dropoff = datetime.datetime(2024, 1, 1, 10, 15, 0)
        
        input_schema = StructType([
            StructField("fare_amount", DoubleType(), True),
            StructField("passenger_count", IntegerType(), True),
            StructField("pickup_datetime", TimestampType(), True),
            StructField("dropoff_datetime", TimestampType(), True),
            StructField("total_amount", DoubleType(), True), # required
            StructField("YYYYMM", IntegerType(), True)       # required
        ])
        
        input_data = [
            (15.0, 2, valid_pickup, valid_dropoff, 15.0, 202401),   # Valid
            (-5.0, 1, valid_pickup, valid_dropoff, -5.0, 202401),   # Negative fare/total
            (20.0, 99, valid_pickup, valid_dropoff, 20.0, 202401)   # Passenger overflow
        ]
        test_df = spark.createDataFrame(input_data, input_schema)
        
        
        # 修复点 4：必须先过 apply_transformations 提取出 temp_eff 等特征列
        enriched_df = loader.apply_transformations(test_df)
        result_df = loader.apply_dq_rules(enriched_df)
        
        expected_schema = StructType([
            StructField("fare_amount", DoubleType(), True),
            StructField("is_valid", BooleanType(), False)
        ])
        expected_data = [
            (15.0, True),  
            (-5.0, False), 
            (20.0, False)  
        ]
        expected_df = spark.createDataFrame(expected_data, expected_schema)
        
        actual_df = result_df.select("fare_amount", "is_valid")
        
        assert_df_equality(actual_df, expected_df, ignore_row_order=True)