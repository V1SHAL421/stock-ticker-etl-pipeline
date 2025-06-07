import pytest
from infrastructure.spark.spark_session_manager import SparkSessionManager
from utils.main_logger import MainLogger
from utils.pandas_into_spark_df import pandas_into_spark_df
import pandas as pd
from pyspark.sql.functions import array_contains
from pyspark.sql import dataframe as sp_df


@pytest.fixture
def setup():
    main_logger = MainLogger()
    test_logger = main_logger.get_logger()
    spark_session_manager = SparkSessionManager(test_logger)
    spark_session = spark_session_manager.get_spark_session()
    d = {'Open': [100.0], 'Close': [101.0]}
    df = pd.DataFrame(data=d)

    yield spark_session, df, test_logger
    


@pytest.mark.integration
def test_pandas_into_spark_df_with_invalid_spark_session(setup):
    test_spark_session = None
    pandas_df = setup[1]
    test_logger = setup[2]

    with pytest.raises(Exception):
        pandas_into_spark_df(test_spark_session, pandas_df, test_logger)

@pytest.mark.integration
def test_pandas_into_spark_df_with_invalid_pandas_df(setup):
    test_spark_session = setup[0]
    pandas_df = None
    test_logger = setup[2]

    with pytest.raises(Exception):
        pandas_into_spark_df(test_spark_session, pandas_df, test_logger)

@pytest.mark.integration
def test_pandas_into_spark_df_with_invalid_test_logger(setup):
    test_spark_session = setup[0]
    pandas_df = setup[1]
    test_logger = None

    with pytest.raises(Exception):
        pandas_into_spark_df(test_spark_session, pandas_df, test_logger)

@pytest.mark.spark_df_success
def test_pandas_into_spark_df_success(setup):
    test_spark_session = setup[0]
    pandas_df = setup[1]
    test_logger = setup[2]

    spark_df = pandas_into_spark_df(test_spark_session, pandas_df, test_logger)

    expected_open_value = [100.0]
    expected_close_value = [101.0]

    print(f"The Spark DataFrame returned is {spark_df}")

    assert isinstance(spark_df, sp_df.DataFrame)

    rows = spark_df.select("Open", "Close").collect()
    open_vals = [row["Open"] for row in rows]
    close_vals = [row["Close"] for row in rows]

    assert open_vals == expected_open_value
    assert close_vals == expected_close_value