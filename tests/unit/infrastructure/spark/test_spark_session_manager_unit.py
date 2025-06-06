from unittest.mock import patch
import pytest

from infrastructure.spark.spark_session_manager import SparkSessionManager


@pytest.mark.unit
def test_get_spark_session(mocker):
    mock_logger = mocker.Mock()
    mock_session = mocker.Mock()
    mock_builder = mocker.Mock()
    mock_config = mocker.Mock()

    mock_builder.config.return_value = mock_config
    mock_config.getOrCreate.return_value = mock_session

    test_manager = SparkSessionManager(logger=mock_logger)

    with patch("pyspark.sql.SparkSession.builder", new=mock_builder):
        result_session = test_manager.get_spark_session()
        assert result_session == mock_session
        mock_builder.config.assert_called_once()
        mock_config.getOrCreate.assert_called_once()
