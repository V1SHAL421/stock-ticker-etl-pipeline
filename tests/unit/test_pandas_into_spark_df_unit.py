import pandas as pd
import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.testing.utils import assertDataFrameEqual

from utils.pandas_into_spark_df import pandas_into_spark_df


@pytest.fixture
def pandas_df():
    """Yields a test Pandas DataFrame of tick data"""
    d = {"Open": [100.0], "Close": [101.0]}
    df = pd.DataFrame(data=d)
    yield df


@pytest.fixture
def spark_df():
    """Yields a test Spark DataFrame of tick data"""
    spark = SparkSession.builder.getOrCreate()
    mock_spark_df = spark.createDataFrame([Row(Open=100.0, Close=101.0)])
    yield mock_spark_df


@pytest.mark.unit
def test_pandas_into_spark_df_with_invalid_spark_session(mocker, pandas_df):
    """Tests that pandas_into_spark_df with an invalid Spark session raises an Exception

    Given:
        - A test Pandas DataFrame
        - A mock Spark Session set to None
        - A mock logger

    When:
        - pandas_into_spark_df() is called with these parameters

    Then:
        - An Exception is raised"""
    mock_spark_session = None
    mock_logger = mocker.Mock()

    with pytest.raises(Exception):
        pandas_into_spark_df(mock_spark_session, pandas_df, mock_logger)

@pytest.mark.unit
def test_pandas_into_spark_df_with_invalid_pandas_df(mocker, pandas_df):
    """Tests that pandas_into_spark_df with an invalid Spark session raises an Exception

    Given:
        - A test Pandas DataFrame
        - A mock Spark Session set to None
        - A mock logger

    When:
        - pandas_into_spark_df() is called with these parameters

    Then:
        - An Exception is raised"""
    mock_spark_session = mocker.Mock()
    mock_pandas_df = None
    mock_logger = mocker.Mock()

    with pytest.raises(TypeError):
        pandas_into_spark_df(mock_spark_session, mock_pandas_df, mock_logger)


@pytest.mark.unit
def test_pandas_into_spark_df_with_invalid_logger(mocker, pandas_df, spark_df):
    """Tests that pandas_into_spark_df with an invalid logger raises an Exception

    Given:
        - A test Pandas DataFrame
        - A mock Spark Session
        - A mock logger set to None

    When:
        - pandas_into_spark_df() is called with these parameters

    Then:
        - An Exception is raised"""
    mock_spark_session = mocker.Mock()
    mock_logger = None
    mock_spark_session.createDataFrame.return_value = spark_df

    with pytest.raises(Exception):
        pandas_into_spark_df(mock_spark_session, pandas_df, mock_logger)


@pytest.mark.unit
def test_pandas_into_spark_df_success(mocker, pandas_df, spark_df):
    """Tests that pandas_into_spark_df is successful

    Given:
        - A test Pandas DataFrame
        - A test Spark DataFrame
        - A mock Spark Session
        - A mock logger set to None

    When:
        - pandas_into_spark_df() is called with these parameters

    Then:
        - The resultant DataFrame returned will equal the test Spark DataFrame"""
    mock_spark_session = mocker.Mock()
    mock_logger = mocker.Mock()
    mock_spark_session.createDataFrame.return_value = spark_df

    result = pandas_into_spark_df(mock_spark_session, pandas_df, mock_logger)

    assert isinstance(result, DataFrame)
    assertDataFrameEqual(spark_df, result)
