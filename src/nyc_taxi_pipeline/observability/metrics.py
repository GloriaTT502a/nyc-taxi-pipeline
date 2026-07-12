import logging
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, Row 
from typing import Dict, Any, Optional

from nyc_taxi_pipeline.utils.spark_utils import get_spark_session 
from nyc_taxi_pipeline.config.settings import PipelineSettings
from nyc_taxi_pipeline.config.tables import Table, get_table_name


logger = logging.getLogger(__name__)

class PipelineAuditor:
    """
    Enterprise-grade Pipeline Auditor
    Responsible for centralized processing of data observability tracking points, supporting dual-write mechanism.
    """
    
    def __init__(self, settings: PipelineSettings, spark: SparkSession=None):
        self.settings = settings 
        self.spark = spark if spark is not None else get_spark_session() 
        self.audit_table = get_table_name(self.settings, Table.AUDIT_LOG)

    def log_run_metrics(
        self, 
        run_id: str, 
        layer: str,           # Target layer: e.g., 'Bronze', 'Silver', 'Gold'
        target_table: str, 
        valid_count: int, 
        rejected_count: int, 
        custom_metrics: Optional[Dict[str, Any]] = None   # Flexible metric injection
    ) -> None:
        """
        Record Pipeline batch run quality metrics and telemetry.
        """
        # 1. Calculate base metrics 
        total_processed = valid_count + rejected_count
        rejected_ratio = (rejected_count / total_processed) if total_processed > 0 else 0.0

        # 2. Build the unified payload 
        payload = {
            "run_id": run_id,
            "layer": layer,
            "target_table": target_table,
            "valid_count": valid_count,
            "rejected_count": rejected_count,
            "rejected_ratio": float(rejected_ratio)
        }

        # Inject custom metrics (e.g., fallback counts, skew indicators)
        if custom_metrics:
            payload.update(custom_metrics) 
        
        
        # 3. Write to standard console log (For developers/CloudWatch/Databricks logs)
        custom_log_str = f" | Custom: {custom_metrics}" if custom_metrics else ""
        logger.info(
            f"Audit Metrics [{layer}] | RunID: {run_id} | "
            f"Table: {target_table} | "
            f"Valid: {valid_count} | Rejected: {rejected_count} | "
            f"Reject Rate: {rejected_ratio:.2%}{custom_log_str}"
        )

        # 4. Persist to Delta Audit Table (For Dashboards/Grafana)
        try:
            # Dynamically create DataFrame from the payload dictionary
            metrics_df = self.spark.createDataFrame([payload])
            metrics_df = metrics_df.withColumn("created_at", F.current_timestamp())

            (
                metrics_df.write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true") 
                .saveAsTable(self.audit_table)   
            )
            logger.info(f"Audit metrics successfully written to Delta table: {self.audit_table}")
            
        except Exception as e:
            # Fail-safe mechanism: Logging failures should never crash the main data pipeline
            logger.warning(f"Failed to write to audit table. Skipping to ensure main pipeline continuity: {e}")

            