import pytest
from pyspark.sql import SparkSession
from infrastructure.spark.spark_session_manager import SparkSessionManager
from utils.main_logger import MainLogger


@pytest.fixture
def teardown():
    yield  # For teardown functionality
    logger = MainLogger().get_logger()
    spark_session_manager = SparkSessionManager(logger)
    if SparkSessionManager.spark_session:
        spark_session_manager.stop_spark_session()
        assert SparkSessionManager.spark_session is None


@pytest.mark.integration
def test_get_spark_session(teardown):
    logger = MainLogger().get_logger()
    test_manager = SparkSessionManager(logger=logger)
    test_session = test_manager.get_spark_session()
    assert isinstance(test_session, SparkSession)
    assert test_session.sparkContext.appName == "main-spark-app"
