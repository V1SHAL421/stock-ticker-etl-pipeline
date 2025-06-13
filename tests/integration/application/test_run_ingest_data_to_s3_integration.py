from datetime import date
import pytest
import yaml

from application.run_ingest_data_to_s3 import run_ingest_data_to_s3
from infrastructure.apis.yahoo_finance_api_client import YFinanceClient
from infrastructure.aws.s3_io import load_s3_file_path
from infrastructure.spark.spark_session_manager import SparkSessionManager
from utils.main_logger import MainLogger


@pytest.fixture
def setup():
    main_logger = MainLogger()
    logger = main_logger.get_logger()
    yfinance_client = YFinanceClient(logger)
    raw_s3_filepath = load_s3_file_path()
    spark_session_manager = SparkSessionManager(logger)

    yield logger, yfinance_client, raw_s3_filepath, spark_session_manager


@pytest.fixture
def s3_test_path():
    """Loads the file path to the test S3 bucket with today's date"""
    with open("src/config/infra/s3.yaml", "r") as file:
        config = yaml.safe_load(file)

    test_path = config["s3"]["test_raw_bucket"]
    test_path_with_date = f"{test_path}/{date.today().isoformat()}"
    return test_path_with_date


@pytest.mark.integration
def test_run_ingest_data_to_s3(setup, s3_test_path):
    print("Test is running")
    logger, yfinance_client, s3_raw_filepath, spark_session_manager = setup
    run_ingest_data_to_s3(logger, yfinance_client, s3_raw_filepath)
    assert spark_session_manager.spark_session is None

    spark = spark_session_manager.get_spark_session()
    stored_df = spark.read.parquet(f"{s3_raw_filepath}")

    print(stored_df.count())

    assert stored_df.count() > 5

    spark_session_manager.stop_spark_session()
