import pandas as pd
import yfinance as yf

from utils.main_logger import MainLogger
from utils.safe_run import safe_run


class yfinanceClient:
    def __init__(self):
        self.logger = MainLogger().get_logger()

    @safe_run()
    def fetch_market_data(
        self, ticker: str = "AMZN", period_len: str = "1d", recent_interval: str = "1m"
    ) -> pd.DataFrame:
        stock_data = yf.Ticker(ticker)
        minute_level_data = stock_data.history(
            period=period_len, interval=recent_interval
        )
        self.logger.info(
            f"The stock data retrieved from ticker: {ticker} is {minute_level_data}"
        )
        return minute_level_data
