# config.py
import os

class PipelineConfig:
    # 通过环境变量控制。
    # 生产/测试集群上设置 ENV_CATALOG="nyc"
    # 本地/GitHub Actions 上不设置该变量（默认为空）
    CATALOG = os.getenv("ENV_CATALOG", "") 
    
    # 根据环境动态切换 Database 名字
    # 比如在 CI 里可以叫 "ci_process_silver" 防止污染
    DATABASE = os.getenv("ENV_DATABASE", "process_silver")

    @classmethod
    def get_table_path(cls, table_name: str) -> str:
        """
        核心智能路由：自动适配两层或三层命名空间
        """
        if cls.CATALOG:
            # Databricks 环境 -> catalog.schema.table
            return f"{cls.CATALOG}.{cls.DATABASE}.{table_name}"
        else:
            # 本地 / GitHub Actions / 开源 Spark -> schema.table
            return f"{cls.DATABASE}.{table_name}"