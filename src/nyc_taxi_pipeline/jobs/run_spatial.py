# src/nyc_taxi_pipeline/jobs/run_spatial.py

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
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run NYC Taxi Spatial Index Pipeline"
    )

    parser.add_argument(
        "--env",
        type=str,
        default="dev",
        choices=["local", "dev", "qa", "prod"],
        help="Environment"
    )

    parser.add_argument(
        "--catalog",
        type=str,
        default=None,
        help="Unity Catalog override"
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
    logger.info("Starting Spatial Infrastructure Pipeline")
    logger.info("Run ID     : %s", run_id)
    logger.info("Environment: %s", args.env)
    logger.info("Catalog    : %s", args.catalog)
    logger.info("=" * 80)

    #
    # Initialization
    #
    logger.info("Initializing pipeline...")

    settings = PipelineSettings.from_env(
        env_override=args.env,
        catalog_override=args.catalog,
    )

    spark = get_spark_session(runtime_env=args.env)

    auditor = PipelineAuditor(
        settings=settings,
        spark=spark,
    )

    logger.info("Pipeline initialization completed.")

    #
    # Execute pipeline
    #
    logger.info("Building spatial lookup tables...")

    build_spatial_tables(
        spark=spark,
        settings=settings,
        auditor=auditor,
        run_id=run_id,
    )

    logger.info("=" * 80)
    logger.info("Spatial Pipeline Finished Successfully")
    logger.info("=" * 80)


def run():
    """
    Enterprise entry point.

    All exceptions are logged with full traceback
    and then re-raised so Databricks can report
    the real failure.
    """

    try:
        main()

    except Exception:

        logger.exception(
            "Spatial Pipeline Failed"
        )

        raise


if __name__ == "__main__":
    run()