import pytest
from pyspark.sql.types import *
from datetime import datetime
import pyspark.sql.functions as F
from unittest.mock import patch

from nyc_taxi_pipeline.silver.nyc_taxi_silver import (
    NYC_Taxi_Silver_Loader
)


@patch("databricks.sdk.WorkspaceClient")
def test_all_dq_rules(mock_client, spark):
    """
    Industrial-grade DQ rule coverage test.

    This test validates:
    - Valid records pass all rules
    - Every DQ rule correctly flags bad records
    - Multiple edge cases are handled consistently
    """

    # ---------------------------------------------------
    # Test Schema
    # ---------------------------------------------------

    schema = StructType([
        StructField("vendor_id", StringType(), True),
        StructField("pickup_datetime", TimestampType(), True),
        StructField("dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("YYYYMM", IntegerType(), True),
        StructField("_run_id", StringType(), True)
    ])

    # ---------------------------------------------------
    # Base Valid Record
    # ---------------------------------------------------

    valid_pickup = datetime(2023, 1, 1, 10, 0, 0)
    valid_dropoff = datetime(2023, 1, 1, 10, 30, 0)

    # ---------------------------------------------------
    # Poison Test Matrix
    # ---------------------------------------------------

    mock_data = [

        # [0] Valid record
        ("V1", valid_pickup, valid_dropoff,
         2, 15.0, 18.0, 5.0, 202301, "test_001"),

        # [1] missing_pickup
        ("V1", None, valid_dropoff,
         2, 15.0, 18.0, 5.0, 202301, "test_001"),

        # [2] missing_dropoff
        ("V1", valid_pickup, None,
         2, 15.0, 18.0, 5.0, 202301, "test_001"),

        # [3] dropoff_before_pickup
        ("V1",
         datetime(2023, 1, 1, 11, 0, 0),
         datetime(2023, 1, 1, 10, 0, 0),
         2, 15.0, 18.0, 5.0, 202301, "test_001"),

        # [4] passenger_count_invalid
        ("V1", valid_pickup, valid_dropoff,
         10, 15.0, 18.0, 5.0, 202301, "test_001"),

        # [5] total_amount_negative
        ("V1", valid_pickup, valid_dropoff,
         2, 15.0, -1.0, 5.0, 202301, "test_001"),

        # [6] invalid_YYYYMM
        ("V1", valid_pickup, valid_dropoff,
         2, 15.0, 18.0, 5.0, 189912, "test_001"),

        # [7] duration_out_of_range (too short)
        ("V1",
         valid_pickup,
         datetime(2023, 1, 1, 10, 1, 0),
         2, 15.0, 18.0, 5.0, 202301, "test_001"),

        # [8] duration_out_of_range (too long)
        ("V1",
         valid_pickup,
         datetime(2023, 1, 1, 14, 0, 0),
         2, 15.0, 18.0, 5.0, 202301, "test_001"),

        # [9] distance_too_small
        ("V1", valid_pickup, valid_dropoff,
         2, 15.0, 18.0, 0.05, 202301, "test_001"),

        # [10] efficiency_too_high
        ("V1",
         valid_pickup,
         datetime(2023, 1, 1, 10, 10, 0),
         2, 200.0, 210.0, 5.0, 202301, "test_001"),

        # [11] fare_too_low
        ("V1", valid_pickup, valid_dropoff,
         2, 2.0, 2.0, 5.0, 202301, "test_001")
    ]

    # ---------------------------------------------------
    # Build Test DataFrame
    # ---------------------------------------------------

    poisoned_df = spark.createDataFrame(mock_data, schema)

    # ---------------------------------------------------
    # Create Loader
    # ---------------------------------------------------

    loader = NYC_Taxi_Silver_Loader(
        spark=spark,
        run_id="test_dq_all",
        target_table="dummy"
    )

    # ---------------------------------------------------
    # Add Missing Bronze Columns
    # ---------------------------------------------------

    for col_name in loader.EXPECTED_BRONZE_COLS:

        if col_name not in poisoned_df.columns:

            poisoned_df = poisoned_df.withColumn(
                col_name,
                F.lit(None)
            )

    # ---------------------------------------------------
    # Execute Pipeline
    # ---------------------------------------------------

    enriched_df = loader.apply_transformations(poisoned_df)

    result_df = loader.apply_dq_rules(enriched_df)

    results = result_df.collect()

    # ---------------------------------------------------
    # Assertions
    # ---------------------------------------------------

    # [0] Valid record
    assert results[0]["is_valid"] is True
    assert len(results[0]["violated_rules"]) == 0

    # [1] missing_pickup
    assert "missing_pickup" in results[1]["violated_rules"]

    # [2] missing_dropoff
    assert "missing_dropoff" in results[2]["violated_rules"]

    # [3] dropoff_before_pickup
    assert "dropoff_before_pickup" in results[3]["violated_rules"]

    # [4] passenger_count_invalid
    assert "passenger_count_invalid" in results[4]["violated_rules"]

    # [5] total_amount_negative
    assert "total_amount_negative" in results[5]["violated_rules"]

    # [6] invalid_YYYYMM
    assert "invalid_YYYYMM" in results[6]["violated_rules"]

    # [7] duration_out_of_range (short)
    assert "duration_out_of_range" in results[7]["violated_rules"]

    # [8] duration_out_of_range (long)
    assert "duration_out_of_range" in results[8]["violated_rules"]

    # [9] distance_too_small
    assert "distance_too_small" in results[9]["violated_rules"]

    # [10] efficiency_too_high
    assert "efficiency_too_high" in results[10]["violated_rules"]

    # [11] fare_too_low
    assert "fare_too_low" in results[11]["violated_rules"]

    print("All DQ rule tests passed!")