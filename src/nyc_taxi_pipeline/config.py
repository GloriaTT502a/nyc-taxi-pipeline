# src/nyc_taxi_pipeline/config.py
import os 
import sys
import logging

logger = logging.getLogger(__name__) 

def _get_runtime_env() -> str: 
    """
    Safe Parser
    Prioritizes reading command-line arguments (Databricks Serverless), then environment variables (CI/Local)
    """

    # Solution A: Implicitly Safely Parses Command-Line Arguments

    # Does not use argparse, directly scans sys.argv, absolutely immune to other parameters (such as pytest arguments)!

    if "--env" in sys.argv: 
        try: 
            idx = sys.argv.index("--env") 
            return sys.argv[idx + 1].lower() 
        except IndexError: 
            pass 

    return os.getenv("RUNTIME_ENV", "local").lower()

RUNTIME_ENV = os.getenv("RUNTIME_ENV", "local").lower() 

IS_DATABRICKS = RUNTIME_ENV == "databricks" 
IS_LOCAL = RUNTIME_ENV == "local" 
IS_CI = RUNTIME_ENV == "ci" 

logger.info(f"Initialized Pipeline with RUNTIME_ENV: {RUNTIME_ENV}")
