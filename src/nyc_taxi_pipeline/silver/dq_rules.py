import yaml
import pyspark.sql.functions as F
import logging

from importlib import resources
from pyspark.sql.column import Column
from typing import Dict

from nyc_taxi_pipeline.config.settings import PipelineSettings

from nyc_taxi_pipeline.metadata import business_columns as BC
from nyc_taxi_pipeline.metadata import system_columns as SC


logger = logging.getLogger(__name__)

def _get_all_metadata_constants() -> Dict[str, str]: 
    """
    Internal helper functions: Aggregate all metadata constants (Business & System) in the system,
    and construct a dictionary for YAML template injection.
    """ 

    constants_dict = {} 

    # Get all captical words in business_columns 
    for key, value in vars(BC).items(): 
        if key.isupper() and not key.startswith("__"): 
            constants_dict[key] = value 

    # Get all captical words in system_columns 
    for key, value in vars(SC).items(): 
        if key.isupper() and not key.startswith("__"): 
            constants_dict[key] = value 
    
    return constants_dict 


def get_silver_dq_rules(settings: PipelineSettings) -> Dict[str, Column]:
    """
    Data quality rules are dynamically loaded from a YAML configuration file.
    The returned format remains { "rule_name": pyspark.sql.Column }, completely transparent to downstream callers.
    """
    config = None

    # 1. 如果用户手动传了绝对路径（比如挂载在 DBFS 或 UC Volume 上的外部路径），优先使用
    if settings.dq_rules_path:
        try:
            with open(settings.dq_rules_path, 'r', encoding='utf-8') as file:
                config = yaml.safe_load(file)
            logger.info(f"Loaded DQ rules from external override path: {settings.dq_rules_path}")
        except Exception as e:
            logger.error(f"Cannot find the external yaml configuration file at {settings.dq_rules_path}: {e}")
            raise

    # 2. 如果没有传 yaml_path，则安全、规范地直接从已安装的 Python 包内部资产中读取
    else:
        try:
            
            with resources.files("nyc_taxi_pipeline.config").joinpath("rules.yaml").open("r", encoding="utf-8") as file:
                config = yaml.safe_load(file)
            logger.info("Successfully loaded rules.yaml from package internal resources.")
        except Exception as e: 
            logger.error(f"Cannot find 'rules.yaml' inside the package config directory: {e}")
            raise

    # 3. Dynamically Parsing into PySpark Column Expressions
    if not config or 'rules' not in config:
        raise ValueError("Configuration data is empty or missing the 'rules' key.")

    constants_dict = _get_all_metadata_constants() 
    
    rules_dict: Dict[str, Column] = {}
    for rule in config.get('rules', []):
        rule_name = rule['name']
        sql_expr = rule['expr']
        
        if not rule_name or not sql_expr:
            logger.warning(f"Skipping invalid rule definition (missing name or expr): {rule}")
            continue 
        
        try: 
            # Template Injection 
            # 例如 "{COL_PU_DATETIME_UTC} IS NULL" 替换为 "pickup_datetime_utc IS NULL"
            injected_expr = sql_expr.format(**constants_dict) 
        
            # Convert SQL string to PySpark Column object
            rules_dict[rule_name] = F.expr(injected_expr)

        except KeyError as e: 
            error_msg = f"YAML [{rule_name}] uses undefined constants: {e}" 
            logger.error(f"{error_msg}") 
            raise ValueError(error_msg)

    logger.info(f"{len(rules_dict)} data quality rules were successfully loaded.")
    return rules_dict