import yaml
import os
import pyspark.sql.functions as F
import logging
# 🌟 新增：引入现代 Python 标准资源管理库
from importlib import resources

logger = logging.getLogger(__name__)

def get_silver_dq_rules(yaml_path: str = None) -> dict:
    """
    Data quality rules are dynamically loaded from a YAML configuration file.
    The returned format remains { "rule_name": pyspark.sql.Column }, completely transparent to downstream callers.
    """
    config = None

    # 1. 如果用户手动传了绝对路径（比如挂载在 DBFS 或 UC Volume 上的外部路径），优先使用
    if yaml_path is not None:
        try:
            with open(yaml_path, 'r', encoding='utf-8') as file:
                config = yaml.safe_load(file)
        except FileNotFoundError:
            logger.error(f"Cannot find the external yaml configuration file: {yaml_path}")
            raise

    # 2. 如果没有传 yaml_path，则安全、规范地直接从已安装的 Python 包内部资产中读取
    else:
        try:
            # 🌟 核心修改：锚定 'nyc_taxi_pipeline.config' 包路径，安全打开 'rules.yaml'
            with resources.files("nyc_taxi_pipeline.config").joinpath("rules.yaml").open("r", encoding="utf-8") as file:
                config = yaml.safe_load(file)
            logger.info("Successfully loaded rules.yaml from package internal resources.")
        except FileNotFoundError:
            logger.error("Cannot find 'rules.yaml' inside the package config directory.")
            raise

    # 3. Dynamically Parsing into PySpark Column Expressions
    if not config:
        raise ValueError("Configuration data is empty or could not be loaded.")

    rules_dict = {}
    for rule in config.get('rules', []):
        rule_name = rule['name']
        sql_expr = rule['expr']
        
        # Convert SQL string to PySpark Column object
        rules_dict[rule_name] = F.expr(sql_expr)

    logger.info(f"{len(rules_dict)} data quality rules were successfully loaded.")
    return rules_dict