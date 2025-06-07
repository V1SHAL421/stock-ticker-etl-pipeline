import pandas as pd
import pytest
from pyspark.sql import Row
from pyspark.sql import SparkSession

from utils.pandas_into_spark_df import pandas_into_spark_df


@pytest.fixture
def pandas_df():
    d = {"Open": [100.0], "Close": [101.0]}
    df = pd.DataFrame(data=d)
    yield df


@pytest.fixture
def spark_df():
    spark = SparkSession.builder.getOrCreate()
    mock_spark_df = spark.createDataFrame([Row(Open=100.0, Close=101.0)])
    yield mock_spark_df


@pytest.mark.unit
def test_pandas_into_spark_df_with_invalid_spark_session(mocker, pandas_df, spark_df):
    mock_spark_session = None
    mock_logger = mocker.Mock()

    with pytest.raises(Exception):
        pandas_into_spark_df(mock_spark_session, pandas_df, mock_logger)


@pytest.mark.unit
def test_pandas_into_spark_df_with_invalid_logger(mocker, pandas_df, spark_df):
    mock_spark_session = mocker.Mock()
    mock_logger = None
    mock_spark_session.createDataFrame.return_value = spark_df

    with pytest.raises(Exception):
        pandas_into_spark_df(mock_spark_session, pandas_df, mock_logger)


@pytest.mark.unit
def test_pandas_into_spark_df_success(mocker, pandas_df, spark_df):
    mock_spark_session = mocker.Mock()
    mock_logger = mocker.Mock()
    mock_spark_session.createDataFrame.return_value = spark_df

    result = pandas_into_spark_df(mock_spark_session, pandas_df, mock_logger)

    assert result == spark_df
