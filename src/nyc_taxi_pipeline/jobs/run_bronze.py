# src/nyc_taxi_pipeline/jobs/run_bronze.py

import sys
import uuid
import argparse
import logging
from datetime import datetime

from nyc_taxi_pipeline.utils.spark_utils import get_spark_session
from nyc_taxi_pipeline.config.settings import PipelineSettings
from nyc_taxi_pipeline.bronze.loader import TaxiBronzeLoader

# Import the generalized Auditor
from nyc_taxi_pipeline.observability.metrics import PipelineAuditor 
from nyc_taxi_pipeline.config.tables import Table, get_table_name

# ==========================================
# Logger Setup
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

def main():
    # ==========================================
    # 1. Parse Command Line Arguments
    # ==========================================
    parser = argparse.ArgumentParser(description="Run NYC Taxi Bronze Pipeline")
    parser.add_argument(
        "--target-month", 
        type=int, 
        required=True, 
        help="Target month in YYYYMM format (e.g., 202601)"
    )
    parser.add_argument(
        "--env", 
        type=str, 
        default="dev", 
        choices=["local", "dev", "qa", "prod"],
        help="Environment to run the pipeline in"
    )
    
    # Accept the Unity Catalog parameter injected by Databricks YAML
    parser.add_argument(
        "--catalog", 
        type=str, 
        default=None, 
        help="Unity Catalog name to override default settings"
    )
    
    args = parser.parse_args()
    
    target_month = args.target_month
    
    # Generate a unique Run ID for this batch
    current_run_id = str(uuid.uuid4())
    logger.info(f"Starting Bronze Pipeline Run | RunID: {current_run_id} | Env: {args.env}")
    logger.info(f"Processing target month: {target_month}")

    # ==========================================
    # 2. Initialize Core Components
    # ==========================================
    try:
        # Load environment-specific settings AND inject the catalog immediately
        # This prevents the strict validation inside from_env() from throwing a CRITICAL error
        settings = PipelineSettings.from_env(
            env_override=args.env,
            catalog_override=args.catalog
        )
        
        if args.catalog:
            logger.info(f"Target Unity Catalog explicitly set to: {args.catalog}")
        
        # Initialize Spark Session (Stateless and parameter-driven)
        spark = get_spark_session(runtime_env=args.env)
        
        # Initialize the generalized Auditor
        auditor = PipelineAuditor(settings=settings, spark=spark)
        
        # Target table name for logging
        bronze_table = get_table_name(settings, Table.BRONZE_TRIP)

    except Exception as e:
        logger.critical(f"Failed to initialize pipeline components: {e}")
        sys.exit(1)

    # ==========================================
    # 3. Execute Pipeline Logic
    # ==========================================
    start_timestamp = datetime.now()
    
    try:
        loader = TaxiBronzeLoader(settings=settings, spark=spark, run_id=current_run_id)
        
        # Execute the high-performance idempotent write logic
        dq_result = loader.write_idempotent(target_month=target_month)
        
        # ==========================================
        # 4. Log Telemetry and Audit Metrics
        # ==========================================
        duration_seconds = (datetime.now() - start_timestamp).total_seconds()
        
        # Use our extensible auditor to log the results
        auditor.log_run_metrics(
            run_id=current_run_id,
            layer="Bronze",
            target_table=bronze_table,
            valid_count=dq_result.total_rows,
            rejected_count=0, # Bronze typically accepts all data
            custom_metrics={
                "processing_time_seconds": duration_seconds,
                "target_month": target_month 
            }
        )
        
        logger.info(f"Bronze Pipeline completed successfully in {duration_seconds:.2f} seconds.")
        
    except Exception as e:
        # Catch any pipeline failures to prevent silent crashes
        duration_seconds = (datetime.now() - start_timestamp).total_seconds()
        logger.error(f"Bronze Pipeline failed after {duration_seconds:.2f} seconds. Error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()