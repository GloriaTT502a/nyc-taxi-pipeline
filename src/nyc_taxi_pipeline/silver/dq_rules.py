# src/nyc_taxi_pipeline/silver/dq_rules.py

import yaml
import os
import pyspark.sql.functions as F
import logging

logger = logging.getLogger(__name__)

def get_silver_dq_rules(yaml_path: str = None) -> dict:
    """
    Data quality rules are dynamically loaded from a YAML configuration file.
    The returned format remains { "rule_name": pyspark.sql.Column }, completely transparent to downstream callers.
    """
    # 1. Resolve the configuration file path (by default, it points to the config folder in the parent directory).
    if yaml_path is None:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        yaml_path = os.path.join(base_dir, "..", "config", "rules.yaml")

    # 2. Read YAML file
    try:
        with open(yaml_path, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)
    except FileNotFoundError:
        logger.error(f"Cannot find the yaml configration file: {yaml_path}")
        raise

    # 3. Dynamically Parsing into PySpark Column Expressions
    # F.expr() is a very powerful feature of Spark; it can parse a SQL string into a DataFrame that can be executed.
    rules_dict = {}
    for rule in config.get('rules', []):
        rule_name = rule['name']
        sql_expr = rule['expr']
        
        # Convert SQL string to PySpark Column object
        rules_dict[rule_name] = F.expr(sql_expr)

    logger.info(f"{len(rules_dict)} data quality rules were successfully loaded from YAML.")
    return rules_dict
