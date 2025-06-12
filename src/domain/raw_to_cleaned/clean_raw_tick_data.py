from pyspark.sql import DataFrame

from utils.safe_run import safe_run


@safe_run()
def clean_raw_tick_data(df: DataFrame):
    df_cleaned = df.dropna().dropDuplicates().filter("date_col" in df.columns)
    return df_cleaned
