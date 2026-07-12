# src/nyc_taxi_pipeline/__init__.py
import sys
import traceback
import logging

# 初始化一个基本的 logger 以便捕捉启动阶段的错误
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pipeline_launcher")
# 将 jobs 子模块里的 main 函数暴露到包的根目录，供 Databricks Wheel Task 调用
from .jobs.run_bronze import main as run_bronze
from .jobs.run_silver import main as run_silver

# Spatial 模块的入口
from .jobs.run_spatial import main as run_build_zone_lookup


def run_job_with_monitoring(job_func, job_name):
    try:
        logger.info(f"Starting {job_name}...")
        job_func()
        logger.info(f"{job_name} finished successfully.")
    except Exception as e:
        # 🌟 这一步非常关键：强制将详细的错误堆栈输出到标准错误流 (stderr)
        logger.error(f"CRITICAL ERROR in {job_name}: {str(e)}")
        traceback.print_exc() 
        sys.exit(1) 
        