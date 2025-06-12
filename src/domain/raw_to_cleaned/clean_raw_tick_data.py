from pyspark.sql import DataFrame, functions as F
from datetime import datetime

from utils.safe_run import safe_run

@safe_run()
def clean_raw_tick_data(df: DataFrame):
    current_time = datetime.now()
    df_cleaned = df.dropna().dropDuplicates().filter("date_col" in df.columns)
    return df_cleaned