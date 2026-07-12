# src/nyc_taxi_pipeline/jobs/run_spatial.py

import sys
import uuid
import argparse
import logging
from datetime import datetime

from nyc_taxi_pipeline.utils.spark_utils import get_spark_session
from nyc_taxi_pipeline.config.settings import PipelineSettings
from nyc_taxi_pipeline.observability.metrics import PipelineAuditor
from nyc_taxi_pipeline.spatial.zone_lookup import build_spatial_tables 

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__) 

def parse_args():
    parser = argparse.ArgumentParser(
        description="Run NYC Taxi Spatial Pipeline"
    )

    parser.add_argument(
        "--env",
        default="dev",
        choices=["local", "dev", "qa", "prod"],
    )

    parser.add_argument(
        "--catalog",
        default=None,
    )

    return parser.parse_args()


def main():

    
    args = parse_args()

    run_id = (
        f"spatial_run_"
        f"{datetime.now():%Y%m%d_%H%M%S}_"
        f"{uuid.uuid4().hex[:6]}"
    )

    logger.info("=" * 80)
    logger.info("Starting Spatial Pipeline")
    logger.info("Run ID      : %s", run_id)
    logger.info("Environment : %s", args.env)
    logger.info("=" * 80)

    #
    # Initialization
    #
    logger.info("Initializing Pipeline...")

    settings = PipelineSettings.from_env(
        env_override=args.env,
        catalog_override=args.catalog,
    )

    spark = get_spark_session(env=args.env)

    auditor = PipelineAuditor(
        settings=settings,
        spark=spark,
    )

    logger.info("Initialization Complete")

    #
    # Execute Pipeline
    #
    logger.info("Building Spatial Tables...")

    build_spatial_tables(
        spark=spark,
        settings=settings,
        auditor=auditor,
        run_id=run_id,
    )

    logger.info("=" * 80)
    logger.info("Spatial Pipeline Finished Successfully")
    logger.info("=" * 80)


if __name__ == "__main__":
    try:
        main()

    except Exception:

        logger.exception(
            "Spatial Pipeline Failed with Unhandled Exception"
        )

        raise 



