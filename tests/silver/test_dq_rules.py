import pytest
import yaml
from dataclasses import replace  # 🌟 关键导入
from nyc_taxi_pipeline.silver.dq_rules import get_silver_dq_rules
from nyc_taxi_pipeline.config.settings import PipelineSettings

@pytest.fixture
def mock_settings(tmp_path):
    """使用 dataclasses.replace 创建带有自定义 dq_rules_path 的新实例"""
    # 构造 YAML
    config_data = {
        "rules": [
            {"name": "passenger_gt_zero", "expr": "passenger_count > 0"},
            {"name": "trip_distance_valid", "expr": "trip_distance >= 0.0"}
        ]
    }
    yaml_file = tmp_path / "mock_rules.yaml"
    with open(yaml_file, "w", encoding="utf-8") as f:
        yaml.dump(config_data, f)
    
    # 1. 创建基础实例
    base_settings = PipelineSettings(
        runtime_env="local",
        catalog="nyc",
        base_path="/tmp/test",
        shp_path="/tmp/test/zone.shp",
        bronze_db="bronze",
        silver_db="silver",
        gold_db="gold"
    )
    
    # 2. 🌟 关键：使用 replace 创建新实例，同时改变 dq_rules_path
    return replace(base_settings, dq_rules_path=str(yaml_file))

def test_get_silver_dq_rules_loading(mock_settings, spark):
    rules = get_silver_dq_rules(settings=mock_settings)
    assert "passenger_gt_zero" in rules
    assert "trip_distance_valid" in rules
    assert hasattr(rules["passenger_gt_zero"], "desc") 

def test_get_silver_dq_rules_file_not_found():
    """测试当路径非法时，代码会抛出预期的异常"""
    base_settings = PipelineSettings(
        runtime_env="local",
        catalog="nyc",
        base_path="/tmp/test",
        shp_path="/tmp/test/zone.shp",
        bronze_db="bronze",
        silver_db="silver",
        gold_db="gold"
    )
    
    # 🌟 关键：使用 replace 创建无效路径实例
    invalid_settings = replace(base_settings, dq_rules_path="non_existent.yaml")
    
    with pytest.raises(FileNotFoundError):
        get_silver_dq_rules(settings=invalid_settings)