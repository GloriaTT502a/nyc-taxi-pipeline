# src/nyc_taxi_pipeline/observability/metric_keys.py 

"""
Pipeline Telemetry & Metrics Keys
""" 

# H3 空间引擎降级指标
METRIC_PU_FALLBACK = "pickup_fallback_count"
METRIC_DO_FALLBACK = "dropoff_fallback_count"

# 数据质量拦截指标
METRIC_DQ_IS_VALID = "is_valid"
METRIC_DQ_VIOLATED_RULES = "violated_rules"


# Column name after Aggregation 
METRIC_TOTAL_VALID = "total_valid"
METRIC_PU_FALLBACK_COUNT = "pu_fallback_count"
METRIC_DO_FALLBACK_COUNT = "do_fallback_count"
