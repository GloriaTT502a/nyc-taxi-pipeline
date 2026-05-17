# tests/silver/test_dq_rules.py
import pytest
import os
import yaml
from nyc_taxi_pipeline.silver.dq_rules import get_silver_dq_rules

@pytest.fixture
def mock_yaml_config(tmp_path):
    """创建临时的 YAML 配置文件防止污染环境"""
    config_data = {
        "rules": [
            {"name": "passenger_gt_zero", "expr": "passenger_count > 0"},
            {"name": "trip_distance_valid", "expr": "trip_distance >= 0.0"}
        ]
    }
    yaml_file = tmp_path / "mock_rules.yaml"
    with open(yaml_file, "w", encoding="utf-8") as f:
        yaml.dump(config_data, f)
    return str(yaml_file)

def test_get_silver_dq_rules_loading(mock_yaml_config, spark):
    rules = get_silver_dq_rules(yaml_path=mock_yaml_config)
    
    assert "passenger_gt_zero" in rules
    assert "trip_distance_valid" in rules
    # 验证返回值是否已经转换为了 PySpark 的 Column 对象
    assert hasattr(rules["passenger_gt_zero"], "desc") 

def test_get_silver_dq_rules_file_not_found():
    with pytest.raises(FileNotFoundError):
        get_silver_dq_rules(yaml_path="invalid_path_to_file.yaml")