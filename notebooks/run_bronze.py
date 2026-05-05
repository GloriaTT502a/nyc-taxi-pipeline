import argparse
from pyspark.sql import SparkSession

from bronze.loader import TaxiBronzeLoader
from common.logger import get_logger

logger = get_logger("run_bronze")

def main():
    parser = argparse.ArgumentParser(description="Bronze Pipeline Runner")
    parser.add_argument("--start_month", type=int, required=True, help="Start month (YYYYMM)")
    parser.add_argument("--end_month", type=int, required=True, help="End month (YYYYMM)")
    args = parser.parse_args()

    logger.info(f"Initializing Bronze Pipeline for period: {args.start_month} to {args.end_month}")

    spark = (
        SparkSession.builder
        .appName("NYC_Taxi_Bronze_Ingestion")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

    # Please build target table by scripts/migrations/run_migrations.py firstly
    loader = TaxiBronzeLoader(spark)
    
    logger.info("Starting Bronze ingestion...")
    result = loader.write_idempotent(start_time=args.start_month, end_time=args.end_month)
    
    logger.info(f"Pipeline executed successfully. Total rows ingested: {result.total_rows}")

if __name__ == "__main__":
    main()