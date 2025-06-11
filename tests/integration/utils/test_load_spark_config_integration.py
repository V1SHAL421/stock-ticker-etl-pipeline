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
    yaml_content = {
        "spark": {
            "app_name": "integration-test-app",
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

    config_file = tmp_path / "spark_config.yaml"
    with config_file.open("w") as file:
        yaml.dump(yaml_content, file)

    conf = load_spark_config(path=str(config_file))

    assert isinstance(conf, SparkConf)
    assert conf.get("spark.app.name") == "integration-test-app"
    assert conf.get("spark.master") == "local[*]"
