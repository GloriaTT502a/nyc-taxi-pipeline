# src/config/settings.py
import os

class PipelineConfig:
    # 通过环境变量控制。
    # 生产/测试集群上设置 ENV_CATALOG="nyc"
    # 本地/GitHub Actions 上不设置该变量（默认为空）
    CATALOG = os.getenv("ENV_CATALOG", "") 

    @classmethod
    def get_table_path(cls, table_name: str, default_db: str) -> str:
        """
        核心智能路由：自动适配两层或三层命名空间
        """
        database = os.getenv("ENV_DATABASE", default_db) 

        if cls.CATALOG:
            # Databricks 环境 -> catalog.schema.table
            return f"{cls.CATALOG}.{database}.{table_name}"
        else:
            # 本地 / GitHub Actions / 开源 Spark -> schema.table
            return f"{database}.{table_name}"
        

# ==========================================
# 2. Exporting global constants and table names (for reference by the Loader)
# ==========================================

# The base path can be overridden via environment variables (for easier testing, change it to /tmp/...).
BASE_PATH = os.getenv("ENV_BASE_PATH", "/Volumes/nyc/default/")

# Dynamically generated Bronze table name (default database is process_bronze)
BRONZE_TABLE = PipelineConfig.get_table_path("brz_yellow_nyc_taxi", default_db="process_bronze")

# Dynamically generated Silver table name (default database is process_silver)
SILVER_TABLE = PipelineConfig.get_table_path("slv_yellow_nyc_taxi", default_db="process_silver")

# System-level metadata column names
RUN_ID_COLUMN = "_run_id"
LINEAGE_COLUMN = "_input_file"