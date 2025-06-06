import pytest
from pyspark import SparkConf
import yaml
from infrastructure.spark.load_spark_config import load_spark_config
from unittest.mock import mock_open, patch

@pytest.mark.unit
def test_load_spark_config_missing_spark_key():
    mock_spark_config = {
        "not_spark": {
            "app_name": "test-app",
            "master": "local[*]"
        }
    }

    with patch("builtins.open", mock_open(read_data="fake_yaml_content")) as mock_file:
        with patch("yaml.safe_load", return_value=mock_spark_config):
            with pytest.raises(Exception):
                load_spark_config("dummy_path.yaml")

@pytest.mark.unit
def test_load_spark_config_missing_app_name_key():
    mock_spark_config = {
        "spark": {
            "not_app_name": "test-app",
            "master": "local[*]"
        }
    }

    with patch("builtins.open", mock_open(read_data="fake_yaml_content")) as mock_file:
        with patch("yaml.safe_load", return_value=mock_spark_config):
            with pytest.raises(Exception):
                load_spark_config("dummy_path.yaml")

@pytest.mark.unit
def test_load_spark_config_missing_master_key():
    mock_spark_config = {
        "spark": {
            "app_name": "test-app",
            "not_master": "local[*]"
        }
    }

    with patch("builtins.open", mock_open(read_data="fake_yaml_content")):
        with patch("yaml.safe_load", return_value=mock_spark_config):
            with pytest.raises(Exception):
                load_spark_config("dummy_path.yaml")

@pytest.mark.unit
def test_load_spark_config_raises_yaml_error():
    with patch("builtins.open", mock_open(read_data="fake_yaml_content")):
        with patch("yaml.safe_load", side_effect=yaml.YAMLError("Invalid YAML")):
            with pytest.raises(Exception):
                load_spark_config("dummy_path.yaml")

@pytest.mark.unit
def test_load_spark_config_file_not_found():
    with patch("builtins.open", side_effect=FileNotFoundError("File not found")):
        with pytest.raises(Exception):
            load_spark_config("dummy_path.yaml")
