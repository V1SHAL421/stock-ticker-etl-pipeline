from typing import Protocol
import pandas as pd

class MarketDataClient(Protocol):
    def fetch_market_data(ticker: str) -> pd.DataFrame:
        ...
