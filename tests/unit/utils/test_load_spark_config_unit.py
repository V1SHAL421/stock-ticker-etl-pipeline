import pytest
from pyspark import SparkConf
import yaml
from utils.load_spark_config import load_spark_config
from unittest.mock import mock_open, patch


@pytest.mark.unit
def test_load_spark_config_missing_spark_key():
    """Tests loading the Spark configuration with a missing Spark key

    Given:
        - A mocked Spark config with no Spark key

    When:
        - load_spark_config() is called

    Then:
        - An Exception is raised"""
    mock_spark_config = {"not_spark": {"app_name": "test-app", "master": "local[*]"}}

    with patch("builtins.open", mock_open(read_data="fake_yaml_content")):
        with patch("yaml.safe_load", return_value=mock_spark_config):
            with pytest.raises(Exception):
                load_spark_config("dummy_path.yaml")


@pytest.mark.unit
def test_load_spark_config_missing_app_name_key():
    """Tests loading the Spark configuration with a missing app_name

    Given:
        - A mocked Spark config with no app_name

    When:
        - load_spark_config() is called

    Then:
        - An Exception is raised"""
    mock_spark_config = {"spark": {"not_app_name": "test-app", "master": "local[*]"}}

    with patch("builtins.open", mock_open(read_data="fake_yaml_content")):
        with patch("yaml.safe_load", return_value=mock_spark_config):
            with pytest.raises(Exception):
                load_spark_config("dummy_path.yaml")


@pytest.mark.unit
def test_load_spark_config_missing_master_key():
    """Tests loading the Spark configuration with a missing master key

    Given:
        - A mocked Spark config with no master key

    When:
        - load_spark_config() is called

    Then:
        - An Exception is raised"""
    mock_spark_config = {"spark": {"app_name": "test-app", "not_master": "local[*]"}}

    with patch("builtins.open", mock_open(read_data="fake_yaml_content")):
        with patch("yaml.safe_load", return_value=mock_spark_config):
            with pytest.raises(Exception):
                load_spark_config("dummy_path.yaml")


@pytest.mark.unit
def test_load_spark_config_raises_yaml_error():
    """Tests loading the Spark configuration with a YAML error

    Given:
        - A mocked Spark config with a YAML error

    When:
        - load_spark_config() is called

    Then:
        - An Exception is raised"""
    with patch("builtins.open", mock_open(read_data="fake_yaml_content")):
        with patch("yaml.safe_load", side_effect=yaml.YAMLError("Invalid YAML")):
            with pytest.raises(Exception):
                load_spark_config("dummy_path.yaml")


@pytest.mark.unit
def test_load_spark_config_file_not_found():
    """Tests loading the Spark configuration with the file not found

    Given:
        - A mocked Spark config with the file not found

    When:
        - load_spark_config() is called

    Then:
        - An Exception is raised"""
    with patch("builtins.open", side_effect=FileNotFoundError("File not found")):
        with pytest.raises(Exception):
            load_spark_config("dummy_path.yaml")


@pytest.mark.unit
def test_load_spark_config_success():
    """Tests loading the Spark configuration

    Given:
        - A mocked Spark config

    When:
        - load_spark_config() is called

    Then:
        - The expected Spark configuration fields are returned"""
    mock_spark_config = {
        "spark": {
            "app_name": "unit-test-app",
            "master": "local[*]",
            "jars_packages": "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.397",
            "hadoop_aws_configs": {
                "fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
                # "fs.s3a.aws.credentials.provider": "com.amazonaws.auth.profile.ProfileCredentialsProvider",
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "fs.s3a.vectored.read.min.seek.size": "131072",
                "spark.hadoop.fs.s3a.vectored.read.max.merged.size": "2097152",
                "fs.s3a.threads.keepalivetime": "60000",
                "yarn.router.subcluster.cleaner.interval.time": "60000",
                "yarn.resourcemanager.delegation-token-renewer.thread-retry-interval": "60000",
                "yarn.resourcemanager.delegation-token-renewer.thread-timeout": "60000",
                "fs.s3a.connection.establish.timeout": "30000",
                "yarn.federation.state-store.heartbeat.initial-delay": "30000",
                "yarn.federation.gpg.webapp.connect-timeout": "30000",
                "yarn.federation.gpg.webapp.read-timeout": "30000",
                "yarn.apps.cache.expire": "30000",
                "hadoop.service.shutdown.timeout": "30000",
                "fs.s3a.connection.timeout": "200000",
                "fs.s3a.multipart.purge.age": "86400000",
            },
        }
    }

    with patch("builtins.open", mock_open(read_data="fake_yaml_content")):
        with patch("yaml.safe_load", return_value=mock_spark_config):
            with patch(
                "utils.load_spark_config.inject_boto3_sso_credentials",
                return_value=mock_spark_config,
            ):
                conf = load_spark_config("dummy_path.yaml")

                assert isinstance(conf, SparkConf)
                assert conf.get("spark.app.name") == "unit-test-app"
                assert conf.get("spark.master") == "local[*]"
