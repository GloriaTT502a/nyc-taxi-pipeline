# tests/helpers.py

from pyspark.sql import Row
from pyspark.sql import functions as F 


def create_input_df(spark):

    data = [

        Row(
            VendorID="1",
            passenger_count=2,
            total_amount=30.5
        )
    ]

    df = spark.createDataFrame(data)

    return df.withColumn(
        "_metadata",
        F.struct(
            F.lit("yellow_tripdata_2010-01.parquet").alias("file_path")
        )
    )