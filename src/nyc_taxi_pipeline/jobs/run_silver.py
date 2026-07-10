import sys
import uuid
import argparse
import logging
from datetime import datetime


from nyc_taxi_pipeline.utils.spark_utils import get_spark_session
from nyc_taxi_pipeline.config.settings import PipelineSettings
from nyc_taxi_pipeline.config.tables import Table, get_table_name
from nyc_taxi_pipeline.silver.pipeline import NYCTaxiSilverPipeline 

# 配置本地终端的日志打印格式，方便你在 VS Code 实时观察 Pipeline 的运行轨迹
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S" 
)
logger = logging.getLogger("NYC_Taxi_Silver_Job")

def main():
    # ==========================================
    # 1. Parse Command Line Arguments
    # ==========================================
    parser = argparse.ArgumentParser(description="Run NYC Taxi Silver Pipeline")
    parser.add_argument("--env", type=str, default="dev", choices=["local", "dev", "qa", "prod"], help="Environment")
    parser.add_argument("--catalog", type=str, default=None, help="Unity Catalog name to override")
    
    parser.add_argument("--reset-env", action="store_true", help="Drop target tables before running (Dev only)")
    parser.add_argument("--test-limit", type=int, default=None, help="Limit number of rows for testing/probing")
    
    args = parser.parse_args()

    run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    logger.info(f"Starting Silver Pipeline Run | RunID: {run_id} | Env: {args.env}") 

    # ==========================================
    # 2. Initialize Core Components & Settings
    # ==========================================
    try: 
        settings = PipelineSettings.from_env(
            env_override=args.env,
            catalog_override=args.catalog
        ) 

        if args.catalog:
            logger.info(f"Overriding target Unity Catalog to: {args.catalog}") 
        
        spark = get_spark_session(runtime_env=args.env) 

        bronze_table = get_table_name(settings, Table.BRONZE_TRIP) 
        target_silver_table = get_table_name(settings, Table.SILVER_TRIP) 
        audit_table = get_table_name(settings, Table.AUDIT_LOG) 

        zone_dim_table = get_table_name(settings, Table.SILVER_ZONE_H3) 

        checkpoint_schema = f"{settings.catalog}.{settings.silver_db}" 
    
    except Exception as e: 
        logger.critical(f"Failed to initialize pipeline components: {e}") 
        sys.exit(1) 

    
    # ==========================================
    # 3. Environment Reset Logic 
    # ========================================== 
    if args.reset_env: 
        if args.env == "prod": 
            logger.warning("WARNING: --reset-env flag is IGNORED in 'prod' environment for safety!" )
        else: 
            logger.warning(f"[Dev Mode] Cleaning up existing tables...") 
            spark.sql(f"DROP TABLE IF EXISTS {target_silver_table}")
            spark.sql(f"DROP TABLE IF EXISTS {audit_table}")
            spark.sql(f"DROP TABLE IF EXISTS {target_silver_table}_quarantine")
            logger.info("Cleanup complete. Pipeline will start from scratch.")


    # ==========================================
    # 4. Data Ingestion
    # ========================================== 
    logger.info(f"Reading source Bronze table: {bronze_table}") 
    bronze_df = spark.read.table(bronze_table)
    
    logger.info(f"Reading Spatial Dimension table: {zone_dim_table}")
    zone_dim_df = spark.read.table(zone_dim_table) 

    # ==========================================
    # 5. Test Mode (Probing)
    # ========================================== 

    if args.test_limit: 
        logger.info(f"Probing Mode Active: Limiting input data to {args.test_limit} rows.")
        bronze_df = bronze_df.limit(args.test_limit)
    
    
    # ==========================================
    # 6. Execute Pipeline
    # ========================================== 
    try: 
        pipeline = NYCTaxiSilverPipeline(
            settings=settings,
            spark=spark, 
            run_id=run_id, 
            zone_dim_df=zone_dim_df, 
            target_table=target_silver_table, 
            audit_table=audit_table, 
            checkpoint_schema=checkpoint_schema 
        ) 

        logger.info("Engine ignited! Pipeline processing started...")
        pipeline.process(bronze_df) 

        logger.info(f"Run completed successfully! Data available in: {target_silver_table}")
        
    except Exception as e:
        logger.error(f"Silver Pipeline failed. Error: {e}", exc_info=True)
        sys.exit(1) 


if __name__ == "__main__":
    main()