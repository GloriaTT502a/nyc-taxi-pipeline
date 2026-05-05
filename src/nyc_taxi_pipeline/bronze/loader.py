import uuid
from datetime import datetime
from functools import reduce
from dateutil.relativedelta import relativedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from nyc_taxi_pipeline.config.settings import BASE_PATH, BRONZE_TABLE, LINEAGE_COLUMN, RUN_ID_COLUMN
from nyc_taxi_pipeline.common.logger import get_logger
from nyc_taxi_pipeline.models.dq_result import DQResult
from nyc_taxi_pipeline.bronze.transformations import normalize_dataframe

# Get current Logger
logger = get_logger(__name__)

class TaxiBronzeLoader:
    def __init__(self, spark: SparkSession, run_id: str = None):
        self.spark = spark
        self.target_table = BRONZE_TABLE
        self.yellow_path = f"{BASE_PATH}yellowtaxi/"
        self.run_id = run_id or str(uuid.uuid4())

    def _generate_target_paths(self, start_time: int, end_time: int) -> list[str]:
        """Get the file list that need to be loaded"""
        start_date = datetime.strptime(str(start_time), "%Y%m")
        end_date = datetime.strptime(str(end_time), "%Y%m")
        
        target_paths = []
        current_date = start_date
        while current_date <= end_date:
            yyyy_mm_str = current_date.strftime("%Y-%m")
            target_paths.append(f"{self.yellow_path}yellow_tripdata_{yyyy_mm_str}.parquet")
            current_date += relativedelta(months=1)
            
        return target_paths

    def write_idempotent(self, start_time: int, end_time: int) -> DQResult:
        """core logic: write with idempotent"""
        paths = self._generate_target_paths(start_time, end_time)
        normalized_dfs = []

        # 1. Get all files in the path
        for path in paths:
            try:
                logger.info(f"Reading source file: {path}")
                raw_df = self.spark.read.parquet(path)
                
                # Apply transformations.py 
                norm_df = normalize_dataframe(
                    df=raw_df, 
                    run_id=self.run_id, 
                    lineage_col=LINEAGE_COLUMN, 
                    run_id_col=RUN_ID_COLUMN
                )
                normalized_dfs.append(norm_df)
            except Exception as e:
                logger.warning(f"File skipped (may not exist or corrupted): {path}. Reason: {e}")

        if not normalized_dfs:
            logger.warning(f"No valid data found for period {start_time} to {end_time}.")
            return DQResult(total_rows=0, bad_rows=0, bad_by_rule={})

        # 2. merge different files with different YYYYMM (allowMissingColumns assure Schema fault tolerance)
        final_df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), normalized_dfs)
        
        # Make sure the time format is current
        filtered_df = final_df.filter((F.col("YYYYMM") >= start_time) & (F.col("YYYYMM") <= end_time))

        # 3. Get YYYYMM dynamically
        partitions = [str(r["YYYYMM"]) for r in filtered_df.select("YYYYMM").distinct().collect()]
        if not partitions:
            logger.info("No data left after time filtering.")
            return DQResult(total_rows=0, bad_rows=0, bad_by_rule={})

        replace_condition = f"YYYYMM IN ({','.join(partitions)})"
        logger.info(f"Executing idempotent write with replaceWhere: {replace_condition}")

        # 4. write idempotent to Delta Lake
        (filtered_df.write
         .format("delta")
         .mode("overwrite")
         .option("replaceWhere", replace_condition)
         .saveAsTable(self.target_table))

        # Bronze no DQ，so bad_rows is 0
        total_ingested = filtered_df.count()
        logger.info(f"Bronze layer load completed. Ingested {total_ingested} rows.")
        
        return DQResult(total_rows=total_ingested, bad_rows=0, bad_by_rule={})