# src/nyc_taxi_pipeline/metadata/business_columns.py 

"""
Taxi Business Metadata Constants
"""

COL_TRIP_KEY = "trip_key"
COL_VENDOR_ID = "vendor_id"
COL_PU_DATETIME = "pickup_datetime"
COL_DO_DATETIME = "dropoff_datetime"
COL_PU_DATETIME_UTC = "pickup_datetime_utc" 
COL_DO_DATETIME_UTC = "dropoff_datetime_utc"


COL_PASSENGER_COUNT = "passenger_count"
COL_TRIP_DISTANCE = "trip_distance"
COL_RATE_CODE = "rate_code"
COL_STORE_FWD_FLAG = "store_and_fwd_flag"
COL_PAYMENT_TYPE = "payment_type"


COL_PU_LOCATION_ID = "PULocationID"
COL_DO_LOCATION_ID = "DOLocationID"
COL_PU_LAT = "pickup_latitude"
COL_PU_LNG = "pickup_longitude"
COL_DO_LAT = "dropoff_latitude"
COL_DO_LNG = "dropoff_longitude"
COL_H3_PU = "h3_pickup"
COL_H3_DO = "h3_dropoff"


COL_FARE_AMOUNT = "fare_amount"
COL_SURCHARGE = "surcharge"
COL_MTA_TAX = "mta_tax"
COL_TIP_AMOUNT = "tip_amount"
COL_TOLLS_AMOUNT = "tolls_amount"
COL_IMPROVEMENT_SURCHARGE = "improvement_surcharge"
COL_CONGESTION_SURCHARGE = "congestion_surcharge"
COL_AIRPORT_FEE = "airport_fee"
COL_CBD_CONGESTION_FEE = "cbd_congestion_fee"
COL_TOTAL_AMOUNT = "total_amount"


# Business Derived Columns
COL_DURATION_MIN = "duration_min"
COL_FARE_PER_MINUTE = "fare_per_minute"

# Telemetry / RCA Columns
COL_IS_PU_FALLBACK = "is_pickup_fallback"
COL_IS_DO_FALLBACK = "is_dropoff_fallback"

