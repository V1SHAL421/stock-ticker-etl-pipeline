from datetime import date
import pytest
import yaml

from infrastructure.aws.s3_io import write_raw_data_to_s3_bucket
from infrastructure.spark.spark_session_manager import SparkSessionManager
from utils.main_logger import MainLogger
from pyspark.sql import Row


@pytest.fixture
def s3_test_path():
    """Loads the file path to the test S3 bucket with today's date"""
    with open("src/config/infra/s3.yaml", "r") as file:
        config = yaml.safe_load(file)

    test_path = config["s3"]["test_raw_bucket"]
    test_path_with_date = f"{test_path}/{date.today().isoformat()}"
    return test_path_with_date


@pytest.fixture
def setup():
    """Loads the test logger and Spark session manager"""
    main_logger = MainLogger()
    test_logger = main_logger.get_logger()
    test_spark_session_manager = SparkSessionManager(test_logger)
    yield test_logger, test_spark_session_manager


@pytest.mark.integration
def test_write_raw_data_to_s3_bucket_invalid_s3_test_path(setup):
    """Tests the Spark Session Manager retrieves the Spark session

    Given:
        - A test logger
        - A test Spark session
        - An invalid test S3 file path
        - A Spark DataFrame

    When:
        - write_raw_data_to_s3_bucket() is called

    Then:
        - A ValueError is raised"""
    test_logger, test_spark_session_manager = setup
    spark = test_spark_session_manager.get_spark_session()
    written_df = spark.createDataFrame(
        [Row(open=100.0, close=110.0), Row(open=105.0, close=110.0)]
    )

    test_logger.info(f"The output filepath is {s3_test_path}")
    with pytest.raises(ValueError, match="The S3 file path is of an incorrect format"):
        write_raw_data_to_s3_bucket(
            written_df, test_spark_session_manager, test_logger, "invalid-test-path"
        )


@pytest.mark.integration
def test_write_raw_data_to_s3_bucket_empty_df(s3_test_path, setup):
    """Tests the Spark Session Manager retrieves the Spark session

    Given:
        - A test logger
        - A test Spark session
        - A test S3 file path
        - No Spark DataFrame

    When:
        - write_raw_data_to_s3_bucket() is called

    Then:
        - A ValueError is raised"""
    test_logger, test_spark_session_manager = setup

    test_logger.info(f"The output filepath is {s3_test_path}")
    with pytest.raises(ValueError, match="The Spark DataFrame does not have any data"):
        write_raw_data_to_s3_bucket(
            None, test_spark_session_manager, test_logger, s3_test_path
        )


@pytest.mark.integration
def test_write_raw_data_to_s3_bucket_success(s3_test_path, setup):
    """Tests the Spark Session Manager retrieves the Spark session

    Given:
        - A test logger
        - A test Spark session
        - A test S3 file path
        - A Spark DataFrame

    When:
        - write_raw_data_to_s3_bucket() is called

    Then:
        - The data is written to the test S3 bucket"""
    test_logger, test_spark_session_manager = setup
    spark = test_spark_session_manager.get_spark_session()
    written_df = spark.createDataFrame(
        [Row(open=100.0, close=110.0), Row(open=105.0, close=110.0)]
    )

    test_logger.info(f"The output filepath is {s3_test_path}")
    write_raw_data_to_s3_bucket(
        written_df, test_spark_session_manager, test_logger, s3_test_path
    )

    spark = test_spark_session_manager.get_spark_session()

    stored_df = spark.read.parquet(f"{s3_test_path}")

    assert stored_df.count() == 2

    test_spark_session_manager.stop_spark_session()
