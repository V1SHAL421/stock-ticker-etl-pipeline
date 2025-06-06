import pandas as pd
import pytest
from infrastructure.apis.yahoo_finance_api_client import YFinanceClient
from utils.main_logger import MainLogger

@pytest.fixture
def test_yfinance_client():
    test_logger = MainLogger().get_logger()
    test_yf_client = YFinanceClient(test_logger)
    yield test_yf_client

@pytest.mark.integration
def test_fetch_market_data_with_invalid_ticker(test_yfinance_client):
    """Tests that fetch_market_data raises a ValueError with an unavailable ticker inputted.

    Given:
        - A YFinanceClient instance

    When:
        - Calling fetch_market_data() on the instance

    Then:
        - The ValueError should be raised of an incorrect ticker
    """

    with pytest.raises(ValueError, match="This ticker is not available"):
        test_yfinance_client.fetch_market_data("HDFS")

@pytest.mark.integration
def test_fetch_market_data_with_invalid_period(test_yfinance_client):
    """Tests that fetch_market_data raises a ValueError with an unavailable period length inputted.

    Given:
        - A YFinanceClient instance

    When:
        - Calling fetch_market_data() on the instance

    Then:
        - The ValueError should be raised of an incorrect period length
    """

    with pytest.raises(ValueError, match="This period length is not available"):
        test_yfinance_client.fetch_market_data("AMZN", "1hour")

@pytest.mark.integration
def test_fetch_market_data_with_invalid_interval(test_yfinance_client):
    """Tests that fetch_market_data raises a ValueError with an unavailable interval inputted.

    Given:
        - A YFinanceClient instance

    When:
        - Calling fetch_market_data() on the instance

    Then:
        - The ValueError should be raised of an incorrect interval
    """

    with pytest.raises(ValueError, match="This interval is not available"):
        test_yfinance_client.fetch_market_data("AMZN", "1d", "1hour")

@pytest.mark.integration
def test_fetch_market_data_success(test_yfinance_client):
    """Tests that fetch_market_data raises a ValueError with an unavailable period length inputted.

    Given:
        - A YFinanceClient instance

    When:
        - Calling fetch_market_data() on the instance

    Then:
        - The mocked DataFrame should be returned
    """

    result = test_yfinance_client.fetch_market_data("AMZN", "1d", "1m")
    print(f"The resultant stock data is {result}")
    print(f"The data type of open is {type(result['Open'])}")
    assert isinstance(result['Open'], float)
    assert isinstance(result['Close'], float)

