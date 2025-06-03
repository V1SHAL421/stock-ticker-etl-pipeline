import pandas as pd
import yfinance as yf
class yfinanceClient:

    def fetch_market_data(ticker: str = "AMZN") -> pd.DataFrame:
        stock_data = yf.Ticker(ticker)
        # Log stock data ingested
        return stock_data