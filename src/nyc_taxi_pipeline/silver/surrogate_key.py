import pyspark.sql.functions as F 
from pyspark.sql import DataFrame 

TRIP_IDENTITY_COLUMNS = [
    "vendor_id",               # Who provide service 
    "pickup_datetime",         # When start 
    "dropoff_datetime",        # When end
    "PULocationID",            # Where start 
    "DOLocationID",            # Where end 
    "passenger_count",         # Passenger count 
    "trip_distance",           # Distance 
    "total_amount"             # Amount 
]



def generate_trip_key(df: DataFrame) -> DataFrame: 
    
    # Generate total_amoun column in silver table 
    # df = _add_total_amount_if_missing(df)
    
    # Generate surrogate key based on latitude and longitude for 2010 and Location ID for others 
    pickup_loc = F.coalesce(
        F.col("PULocationID").cast("string"), 
        F.concat_ws("_", F.col("pickup_latitude").cast("string"), F.col("pickup_longitude").cast("string"))
    )

    dropoff_loc = F.coalesce(
        F.col("DOLocationID").cast("string"), 
        F.concat_ws("_", F.col("dropoff_latitude").cast("string"), F.col("dropoff_longitude").cast("string"))
    )

    base_cols = [F.col(c).cast("string") for c in TRIP_IDENTITY_COLUMNS]

    all_business_cols = [*base_cols, pickup_loc, dropoff_loc]

    normalized_cols = [F.coalesce(c, F.lit("NULL")) for c in all_business_cols] 

    # hash SHA-256 
    return df.withColumn(
        "trip_key", 
        F.sha2(F.concat_ws("||", *normalized_cols), 256)
    )