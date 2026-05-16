# tests/conftest.py
import pytest
import os
import sys

# 1. 动态注入 src 路径，确保 pytest 在任何目录下执行都能正确 import 项目模块
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

# 从你刚刚强化好的通用工具类中导入获取 session 的方法
from nyc_taxi_pipeline.utils.spark_utils import get_spark_session


@pytest.fixture(scope="session") 
def spark(): 
    """
    测试专用的全局共享 SparkSession。
    职责解耦：完全托管给业务通用的 spark_utils 处理环境路由。
    """
    # 2. 统一调用工具类获取最适合当前环境的 SparkSession
    spark_session = get_spark_session()

    yield spark_session 

    # 3. 无论哪种模式，测试结束后安全关闭 session 资源
    import warnings 
    with warnings.catch_warnings(): 
        warnings.simplefilter("ignore")
        try: 
            spark_session.stop() 
        except Exception: 
            pass 


@pytest.fixture 
def mock_silver_path(tmp_path): 
    """
    临时路径，用于本地 Silver 层测试（如隔离区写入或本地 Delta 落盘测试）。
    """ 
    return str(tmp_path / "silver_table") 
