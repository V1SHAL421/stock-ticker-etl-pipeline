import pandas as pd
import pytest
from infrastructure.apis.yahoo_finance_api_client import YFinanceClient

@pytest.mark.unit
def test_fetch_market_data_with_invalid_ticker(mocker):
    """Tests that fetch_market_data raises a ValueError with an unavailable ticker inputted.

    Given:
        - A mocked logger
        - A patched Ticker()
        - A YFinanceClient instance

    When:
        - Calling fetch_market_data() on the instance

    Then:
        - The ValueError should be raised of an incorrect ticker
    """
    mock_df = pd.DataFrame({"Open": [100.0], "Close": [150.0]})
    mock_logger = mocker.Mock()
    mock_ticker = mocker.Mock()
    mock_ticker.history.return_value = mock_df

    mocker.patch(
        "infrastructure.apis.yahoo_finance_api_client.yf.Ticker",
        return_value=mock_ticker,
    )

    test_client = YFinanceClient(logger=mock_logger)

    with pytest.raises(ValueError, match="This ticker is not available"):
        test_client.fetch_market_data("HDFS")

@pytest.mark.unit
def test_fetch_market_data_with_invalid_period(mocker):
    """Tests that fetch_market_data raises a ValueError with an unavailable period length inputted.

    Given:
        - A mocked logger
        - A patched Ticker()
        - A YFinanceClient instance

    When:
        - Calling fetch_market_data() on the instance

    Then:
        - The ValueError should be raised of an incorrect period length
    """
    mock_df = pd.DataFrame({"Open": [100.0], "Close": [150.0]})
    mock_logger = mocker.Mock()
    mock_ticker = mocker.Mock()
    mock_ticker.history.return_value = mock_df

    mocker.patch(
        "infrastructure.apis.yahoo_finance_api_client.yf.Ticker",
        return_value=mock_ticker,
    )

    test_client = YFinanceClient(logger=mock_logger)

    with pytest.raises(ValueError, match="This period length is not available"):
        test_client.fetch_market_data("AMZN", "1hour")

@pytest.mark.unit
def test_fetch_market_data_with_invalid_interval(mocker):
    """Tests that fetch_market_data raises a ValueError with an unavailable interval inputted.

    Given:
        - A mocked logger
        - A patched Ticker()
        - A YFinanceClient instance

    When:
        - Calling fetch_market_data() on the instance

    Then:
        - The ValueError should be raised of an incorrect interval
    """
    mock_df = pd.DataFrame({"Open": [100.0], "Close": [150.0]})
    mock_logger = mocker.Mock()
    mock_ticker = mocker.Mock()
    mock_ticker.history.return_value = mock_df

    mocker.patch(
        "infrastructure.apis.yahoo_finance_api_client.yf.Ticker",
        return_value=mock_ticker,
    )

    test_client = YFinanceClient(logger=mock_logger)

    with pytest.raises(ValueError, match="This interval is not available"):
        test_client.fetch_market_data("AMZN", "1d", "1hour")

@pytest.mark.unit
def test_fetch_market_data_success(mocker):
    """Tests that fetch_market_data raises a ValueError with an unavailable period length inputted.

    Given:
        - A mocked logger
        - A patched Ticker()
        - A YFinanceClient instance
        - A mocked DataFrame when Ticker.history() is called

    When:
        - Calling fetch_market_data() on the instance

    Then:
        - The mocked DataFrame should be returned
    """
    mock_df = pd.DataFrame({"Open": [100.0], "Close": [150.0]})
    mock_logger = mocker.Mock()
    mock_ticker = mocker.Mock()
    mock_ticker.history.return_value = mock_df

    mocker.patch(
        "infrastructure.apis.yahoo_finance_api_client.yf.Ticker",
        return_value=mock_ticker,
    )

    test_client = YFinanceClient(logger=mock_logger)

    result = test_client.fetch_market_data("AMZN", "1d", "1m")

    assert result["Open"].equals(mock_df["Open"])
    assert result["Close"].equals(mock_df["Close"])