from pyspark.sql import types as T 

# Target table schema 
CANONICAL_SCHEMA = (
    ("vendor_id", T.StringType()),
    ("pickup_datetime", T.TimestampType()),
    ("dropoff_datetime", T.TimestampType()),
    ("passenger_count", T.LongType()),
    ("trip_distance", T.DoubleType()),
    ("rate_code", T.LongType()),
    ("store_and_fwd_flag", T.StringType()),
    ("pickup_longitude", T.DoubleType()),
    ("pickup_latitude", T.DoubleType()),
    ("dropoff_longitude", T.DoubleType()),
    ("dropoff_latitude", T.DoubleType()),
    ("PULocationID", T.LongType()),
    ("DOLocationID", T.LongType()),
    ("payment_type", T.StringType()),
    ("fare_amount", T.DoubleType()),
    ("surcharge", T.DoubleType()),
    ("mta_tax", T.DoubleType()),
    ("tip_amount", T.DoubleType()),
    ("tolls_amount", T.DoubleType()),
    ("improvement_surcharge", T.DoubleType()),
    ("congestion_surcharge", T.DoubleType()),
    ("airport_fee", T.DoubleType()),
    ("cbd_congestion_fee", T.DoubleType()),
    ("total_amount", T.DoubleType()),
)

# Deal with schema evolution 
RENAME_MAP = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "RatecodeID": "rate_code",
    "extra": "surcharge",
    "Airport_fee": "airport_fee",
}


