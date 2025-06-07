from pyspark import SparkConf
import pytest
import yaml
from utils.load_spark_config import load_spark_config


@pytest.mark.integration
def test_load_spark_config_missing_spark_key(tmp_path):
    """Tests loading the Spark configuration with a missing Spark key
    
    Given:
        - A config path using tmp_path
        - YAML content without a Spark key
        
    When:
        - load_spark_config() is called
        
    Then:
        - An Exception is raised"""
    yaml_content = {
        "not_spark": {"app_name": "integration-test-app", "master": "local[*]"}
    }

    config_file = tmp_path / "spark_config.yaml"
    with config_file.open("w") as file:
        yaml.dump(yaml_content, file)

    with pytest.raises(Exception):
        load_spark_config(path=str(config_file))


@pytest.mark.integration
def test_load_spark_config_missing_app_name_key(tmp_path):
    """Tests loading the Spark configuration with a missing app_name key
    
    Given:
        - A config path using tmp_path
        - YAML content without an app_name key
        
    When:
        - load_spark_config() is called
        
    Then:
        - An Exception is raised"""
    yaml_content = {
        "spark": {"not_app_name": "integration-test-app", "master": "local[*]"}
    }

    config_file = tmp_path / "spark_config.yaml"
    with config_file.open("w") as file:
        yaml.dump(yaml_content, file)

    with pytest.raises(Exception):
        load_spark_config(path=str(config_file))


@pytest.mark.integration
def test_load_spark_config_missing_master_key(tmp_path):
    """Tests loading the Spark configuration with a missing master key
    
    Given:
        - A config path using tmp_path
        - YAML content without a master key
        
    When:
        - load_spark_config() is called
        
    Then:
        - An Exception is raised"""
    yaml_content = {
        "spark": {"app_name": "integration-test-app", "not_master": "local[*]"}
    }

    config_file = tmp_path / "spark_config.yaml"
    with config_file.open("w") as file:
        yaml.dump(yaml_content, file)

    with pytest.raises(Exception):
        load_spark_config(path=str(config_file))


@pytest.mark.integration
def test_load_spark_config_file_not_found(tmp_path):
    """Tests loading the Spark configuration with a missing app_name key
    
    Given:
        - An incorrect filepath
        
    When:
        - load_spark_config() is called
        
    Then:
        - An Exception is raised"""
    with pytest.raises(Exception):
        load_spark_config(path="file")


@pytest.mark.integration
def test_load_spark_config_success(tmp_path):
    """Tests loading the Spark configuration
    Given:
        - A config path using tmp_path
        - YAML content written to the config file
        
    When:
        - load_spark_config() is called
        
    Then:
        - The expected content is returned in a SparkConf object"""
    yaml_content = {"spark": {"app_name": "integration-test-app", "master": "local[*]"}}

    config_file = tmp_path / "spark_config.yaml"
    with config_file.open("w") as file:
        yaml.dump(yaml_content, file)

    conf = load_spark_config(path=str(config_file))

    assert isinstance(conf, SparkConf)
    assert conf.get("spark.app.name") == "integration-test-app"
    assert conf.get("spark.master") == "local[*]"
