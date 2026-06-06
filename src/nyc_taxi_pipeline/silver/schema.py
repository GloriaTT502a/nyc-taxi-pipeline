# src/nyc_taxi_pipeline/silver/schema.py

# 预期的上游 Bronze 层 Schema 契约
BRONZE_SCHEMA = {
    "vendor_id": {"type": "string", "required": False},
    "pickup_datetime": {"type": "timestamp", "required": False},
    "dropoff_datetime": {"type": "timestamp", "required": False},
    "passenger_count": {"type": "integer", "required": False},
    "trip_distance": {"type": "double", "required": False},
    "rate_code": {"type": "integer", "required": False},
    "store_and_fwd_flag": {"type": "string", "required": False},
    "pickup_longitude": {"type": "double", "required": False},
    "pickup_latitude": {"type": "double", "required": False},
    "dropoff_longitude": {"type": "double", "required": False},
    "dropoff_latitude": {"type": "double", "required": False},
    "PULocationID": {"type": "integer", "required": False},
    "DOLocationID": {"type": "integer", "required": False},
    "payment_type": {"type": "string", "required": False},
    "fare_amount": {"type": "double", "required": False},
    "surcharge": {"type": "double", "required": False},
    "mta_tax": {"type": "double", "required": False},
    "tip_amount": {"type": "double", "required": False},
    "tolls_amount": {"type": "double", "required": False},
    "improvement_surcharge": {"type": "double", "required": False},
    "congestion_surcharge": {"type": "double", "required": False},
    "airport_fee": {"type": "double", "required": False},
    "cbd_congestion_fee": {"type": "double", "required": False},
    "total_amount": {"type": "double", "required": True},
    "YYYY": {"type": "integer", "required": False},
    "YYYYMM": {"type": "integer", "required": True},
    "_run_id": {"type": "string", "required": False},
    "_load_timestamp": {"type": "timestamp", "required": False},
    "_input_file": {"type": "string", "required": False}
}

# 动态生成列名列表供后续 Select 使用
EXPECTED_BRONZE_COLS = list(BRONZE_SCHEMA.keys())

NY_TZ = "America/New_York"

