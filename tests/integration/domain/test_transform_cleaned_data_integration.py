from datetime import date
import pytest
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, StringType, DateType
import numpy as np

from domain.cleaned_to_redshift.transform_cleaned_data import momentum, rolling_returns, rsi, volatility, vwap
from infrastructure.spark.spark_session_manager import SparkSessionManager
from utils.main_logger import MainLogger

@pytest.fixture
def setup():
    main_logger = MainLogger()
    test_logger = main_logger.get_logger()
    spark_session_manager = SparkSessionManager(test_logger)
    spark = spark_session_manager.get_spark_session()
    data = [
    (100.0, 105.0, 98.0, 102.0, 10000, 0.0, 0.0, "AAPL", date(2025, 6, 12)),
    (102.0, 106.0, 101.0, 104.0, 11000, 0.0, 0.0, "AAPL", date(2025, 6, 12)),
    (104.0, 107.0, 103.0, 106.0, 9000, 0.0, 0.0, "AAPL", date(2025, 6, 12)),
    (106.0, 108.0, 104.0, 105.0, 9500, 0.0, 0.0, "AAPL", date(2025, 6, 12)),
    (105.0, 109.0, 103.0, 108.0, 9700, 0.0, 0.0, "AAPL", date(2025, 6, 12)),
    (100.0, 105.0, 98.0, 102.0, 10000, 0.0, 0.0, "AAPL", date(2025, 6, 12)),
    (102.0, 106.0, 101.0, 104.0, 11000, 0.0, 0.0, "AAPL", date(2025, 6, 12)),
    (104.0, 107.0, 103.0, 106.0, 9000, 0.0, 0.0, "AAPL", date(2025, 6, 12)),
    (106.0, 108.0, 104.0, 105.0, 9500, 0.0, 0.0, "AAPL", date(2025, 6, 12)),
    (105.0, 109.0, 103.0, 108.0, 9700, 0.0, 0.0, "AAPL", date(2025, 6, 12)),
    (105.0, 109.0, 103.0, 108.0, 9700, 0.0, 0.0, "AAPL", date(2025, 6, 12)),
    ]

    schema = StructType([
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", LongType(), True),
        StructField("dividends", DoubleType(), True),
        StructField("stock_splits", DoubleType(), True),
        StructField("partition_0", StringType(), True),
        StructField("date_col", DateType(), True),
    ])

    df = spark.createDataFrame(data, schema)
    aggregate_rolling_window = Window.orderBy("date_col").rowsBetween(-9, 0)
    lag_rolling_window = Window.orderBy("date_col")
    yield df, lag_rolling_window, aggregate_rolling_window

@pytest.mark.integration
def test_rolling_returns(setup):
    df, lag_rolling_window, _ = setup
    result_df = rolling_returns(df, lag_rolling_window)
    
    assert "rolling_returns" in result_df.columns

    result_returns = result_df.select("rolling_returns").rdd.flatMap(lambda x: x).collect()
    expected_returns = [None, 2.0, 2.0, -1.0, 3.0, -6.0, 2.0, 2.0, -1.0, 3.0, 0.0]

    assert result_returns == expected_returns

@pytest.mark.integration
def test_momentum(setup):
    df, lag_rolling_window, _ = setup
    result_df = momentum(df, lag_rolling_window)
    
    assert "momentum" in result_df.columns

    result_momentum = result_df.select("momentum").rdd.flatMap(lambda x: x).collect()
    expected_momentum = [None, None, None, None, None, None, None, None, None, None, 6.0]

    assert result_momentum == expected_momentum

@pytest.mark.integration
def test_vwap(setup):
    df, _, aggregate_rolling_window = setup
    result_df = vwap(df, aggregate_rolling_window)
    
    assert "vwap" in result_df.columns

    result_vwap = result_df.select("vwap").rdd.flatMap(lambda x: x).collect()
    expected_vwap = np.array([101.6666667, 102.7142857, 103.5])

    result_vwap = np.array(result_vwap[:3])

    assert np.allclose(result_vwap, expected_vwap, rtol=1e-4)

@pytest.mark.integration
def test_volatility(setup):
    df, _, aggregate_rolling_window = setup
    result_df = volatility(df, aggregate_rolling_window)

    assert "parkinson_volatility" in result_df.columns

    result_volatility = result_df.select("parkinson_volatility").rdd.flatMap(lambda x: x).collect()
    expected_volatility = np.array([0.04143444, 0.03576921, 0.03205426, 0.02998393, 0.03082972, 0.03283588,
                              0.03231813, 0.03129453, 0.03045671, 0.03082972, 0.02990671])

    result_volatility = np.array(result_volatility)

    assert np.allclose(result_volatility, expected_volatility, rtol=1e-4)

@pytest.mark.current
def test_rsi(setup):
    df, lag_rolling_window, aggregate_rolling_window = setup

    returns_df = rolling_returns(df, lag_rolling_window)
    
    assert "rolling_returns" in returns_df.columns

    result_df = rsi(returns_df, aggregate_rolling_window)

    assert "rsi" in result_df.columns

    result_rsi = result_df.select("rsi").rdd.flatMap(lambda x: x).collect()
    expected_rsi = np.array([0.0, 0.0, 0.0, 80.0, 87.5, 50.0, 56.25, 61.11111111, 57.89473684, 63.63636364, 63.63636364])

    result_rsi = np.array(result_rsi)

    assert np.allclose(result_rsi, expected_rsi, rtol=1e-4)