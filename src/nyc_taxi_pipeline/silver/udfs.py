import h3 
import pandas as pd 
from pyspark.sql.functions import pandas_udf 
from pyspark.sql import SparkSession 

@pandas_udf("string") 
def h3_enrichment_udf(lat: pd.Series, lng: pd.Series) -> pd.Series: 
    return pd.Series([
        (h3.latlng_to_cell(y, x, 8) if hasattr(h3, 'latlng_to_cell') else h3.geo_to_h3(y, x, 8))
        if pd.notnull(y) and pd.notnull(x) else None 
        for y, x in zip(lat, lng)
    ]) 

def register_udfs(spark: SparkSession): 
    spark.udf.register("h3_enrichment_udf", h3_enrichment_udf) 
    