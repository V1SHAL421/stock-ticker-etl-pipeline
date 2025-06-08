from unittest.mock import patch
import pytest

from infrastructure.spark.spark_session_manager import SparkSessionManager
from utils.main_logger import MainLogger


@pytest.fixture
def setup():
    main_logger = MainLogger()
    logger = main_logger.get_logger()
    spark_session_manager = SparkSessionManager(logger)
    if spark_session_manager.spark_session:
        spark_session_manager.stop_spark_session()


@pytest.mark.unit
def test_get_spark_session(setup, mocker):
    mock_logger = mocker.Mock()
    mock_session = mocker.Mock()
    mock_builder = mocker.Mock()
    mock_config = mocker.Mock()

    mock_builder.config.return_value = mock_config
    mock_config.getOrCreate.return_value = mock_session

    test_manager = SparkSessionManager(logger=mock_logger)

    print(f"The current Spark session is {test_manager.spark_session}")

    with patch(
        "infrastructure.spark.spark_session_manager.SparkSession.builder",
        new=mock_builder,
    ):
        result_session = test_manager.get_spark_session()
        assert result_session == mock_session
        mock_builder.config.assert_called_once()
        mock_config.getOrCreate.assert_called_once()
