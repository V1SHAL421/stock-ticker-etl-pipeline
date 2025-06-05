import pandas as pd
import yfinance as yf
from utils.safe_run import safe_run


class YFinanceClient:
    """Represents the Yahoo Finance API Client"""

    def __init__(self, logger):
        self.logger = logger

    @safe_run()
    def fetch_market_data(
        self, ticker: str = "AMZN", period_len: str = "1d", recent_interval: str = "1m"
    ) -> pd.DataFrame:
        """Fetches tick data from Yahoo Finance API and returns the last period length of the data of a fixed interval

        Args:
            ticker (str): The ticker where the tick data will be fetched from
            period_len (int): The period of time starting from now of which the data was retrieved by the ticker
            recent_interval (str): The interval of time between each piece of tick data

        Returns:
            minute_level_date (pd.DataFrame): A DataFrame consisting of the stock data including open and close prices

        Raises:
            ValueError: If `ticker` or `period_len` or `recent_interval` is not in the available selection.
        """
        available_tickers = ["AMZN"]
        available_periods = ["1d"]
        available_intervals = ["1m"]

        if ticker not in available_tickers:
            self.logger.error("This ticker is not available")
            raise ValueError("This ticker is not available")

        if period_len not in available_periods:
            self.logger.error("This period length is not available")
            raise ValueError("This period length is not available")

        if recent_interval not in available_intervals:
            self.logger.error("This interval is not available")
            raise ValueError("This interval is not available")

        stock_data = yf.Ticker(ticker)
        minute_level_data = stock_data.history(
            period=period_len, interval=recent_interval
        )
        self.logger.info(
            f"The stock data retrieved from ticker: {ticker} is {minute_level_data}"
        )
        return minute_level_data
