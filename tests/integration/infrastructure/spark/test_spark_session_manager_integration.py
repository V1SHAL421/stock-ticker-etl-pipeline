import pytest
from pyspark.sql import SparkSession
from infrastructure.spark.spark_session_manager import SparkSessionManager
from utils.main_logger import MainLogger


@pytest.fixture
def teardown():
    """Stops the SparkSession after each test is ran

    Given:
        - A SparkSessionManager instance
        - A running Spark session

    When:
        - stop_spark_session() is called

    Then:
        - The spark_session is set to None
        - An Exception is raised if the method is called again
    """
    yield  # For teardown functionality
    logger = MainLogger().get_logger()
    spark_session_manager = SparkSessionManager(logger)
    if SparkSessionManager.spark_session:
        spark_session_manager.stop_spark_session()
        assert SparkSessionManager.spark_session is None
        with pytest.raises(Exception, match="Spark Session has not been initialised"):
            spark_session_manager.stop_spark_session()


@pytest.mark.integration
def test_get_spark_session(teardown):
    """Stops the SparkSession after each test is ran

    Given:
        - A SparkSessionManager instance
        - A running Spark session

    When:
        - get_spark_session() is called

    Then:
        - The spark_session is returned with the expected app name
    """
    logger = MainLogger().get_logger()
    test_manager = SparkSessionManager(logger=logger)
    test_session = test_manager.get_spark_session()
    expected_app_name = "main-spark-app"
    assert isinstance(test_session, SparkSession)
    assert test_session.sparkContext.appName == expected_app_name
