from unittest.mock import patch
import pytest

from infrastructure.aws.s3_io import write_raw_data_to_s3_bucket


@pytest.mark.unit
def test_write_raw_data_to_s3_bucket_empty_df(mocker):
    mock_spark_session_manager = mocker.Mock()
    mock_logger = mocker.Mock()
    mock_spark_session = mocker.Mock()
    mock_spark_session_manager.get_spark_session.return_value = mock_spark_session

    with pytest.raises(ValueError, match="The Spark DataFrame does not have any data"):
        write_raw_data_to_s3_bucket(
            None,
            mock_spark_session_manager,
            mock_logger,
            "s3a://test-raw-tick-data-bucket",
        )


@pytest.mark.unit
def test_write_raw_data_to_s3_bucket_success(mocker):
    mock_spark_session_manager = mocker.Mock()
    mock_logger = mocker.Mock()
    mock_spark_session = mocker.Mock()
    mock_spark_session_manager.get_spark_session.return_value = mock_spark_session
    mock_spark_session_manager.stop_spark_session.return_value = mock_spark_session
    mock_df = mocker.Mock()
    mock_df.isEmpty.return_value = False
    mock_df.withColumn.return_value = mock_df

    mock_df_written = mocker.Mock()
    mock_df.write.return_value = mock_df_written

    mock_df_written.option.return_value = mock_df_written
    mock_df_written.partitionBy.return_value = mock_df_written
    mock_df_written.mode.return_value = mock_df_written
    mock_df_written.parquet.return_value = mock_df_written

    with patch("pyspark.sql.functions.current_date", return_value="date"):
        write_raw_data_to_s3_bucket(
            mock_df,
            mock_spark_session_manager,
            mock_logger,
            "s3a://test-raw-tick-data-bucket",
        )

    mock_df.withColumn.assert_called_once()
