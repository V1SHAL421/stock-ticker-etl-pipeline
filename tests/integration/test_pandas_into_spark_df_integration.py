import pytest
from infrastructure.spark.spark_session_manager import SparkSessionManager
from utils.main_logger import MainLogger
from utils.pandas_into_spark_df import pandas_into_spark_df
import pandas as pd
from pyspark.sql import dataframe as sp_df
from pyspark.sql import Row
from pyspark.testing.utils import assertDataFrameEqual


@pytest.fixture
def setup():
    """Setup the Spark session, Pandas DataFrame and test logger"""
    main_logger = MainLogger()
    test_logger = main_logger.get_logger()
    spark_session_manager = SparkSessionManager(test_logger)
    spark_session = spark_session_manager.get_spark_session()
    d = {"Open": [100.0], "Close": [101.0]}
    df = pd.DataFrame(data=d)

    yield spark_session, df, test_logger


@pytest.mark.integration
def test_pandas_into_spark_df_with_invalid_spark_session(setup):
    """Tests that pandas_into_spark_df with an invalid Spark session raises an Exception

    Given:
        - A test Pandas DataFrame
        - A test Spark Session set to None
        - A test logger
    When:
        - pandas_into_spark_df() is called with these parameters

    Then:
        - An Exception is raised"""
    _, pandas_df, test_logger = setup
    test_spark_session = None

    with pytest.raises(Exception):
        pandas_into_spark_df(test_spark_session, pandas_df, test_logger)


@pytest.mark.integration
def test_pandas_into_spark_df_with_invalid_pandas_df(setup):
    """Tests that pandas_into_spark_df with an invalid Pandas DataFrame raises an Exception

    Given:
        - A test Pandas DataFrame set to None
        - A test Spark Session
        - A test logger
    When:
        - pandas_into_spark_df() is called with these parameters

    Then:
        - An Exception is raised"""
    test_spark_session, _, test_logger = setup
    pandas_df = None

    with pytest.raises(Exception):
        pandas_into_spark_df(test_spark_session, pandas_df, test_logger)


@pytest.mark.integration
def test_pandas_into_spark_df_with_invalid_test_logger(setup):
    """Tests that pandas_into_spark_df with an invalid logger raises an Exception

    Given:
        - A test Pandas DataFrame
        - A test Spark Session
        - A test logger set to None
    When:
        - pandas_into_spark_df() is called with these parameters

    Then:
        - An Exception is raised"""
    test_spark_session, pandas_df, _ = setup
    test_logger = None

    with pytest.raises(Exception):
        pandas_into_spark_df(test_spark_session, pandas_df, test_logger)


@pytest.mark.integration
def test_pandas_into_spark_df_success(setup):
    """Tests that pandas_into_spark_df is successful

    Given:
        - A test Pandas DataFrame
        - A test Spark Session
        - A test logger
    When:
        - pandas_into_spark_df() is called with these parameters

    Then:
        - The expected Spark DataFrame is returned"""
    test_spark_session, pandas_df, test_logger = setup

    spark_df = pandas_into_spark_df(test_spark_session, pandas_df, test_logger)

    expected_df = test_spark_session.createDataFrame([Row(Open=100.0, Close=101.0)])

    print(f"The spark_df is {spark_df} and the expected df is {expected_df}")
    assertDataFrameEqual(spark_df, expected_df)

    assert isinstance(spark_df, sp_df.DataFrame)
    assert "Open" in spark_df.columns
    assert "Close" in spark_df.columns

