from logging import Logger
from pyspark.sql import SparkSession
import pandas as pd

from utils.safe_run import safe_run


@safe_run()
def pandas_into_spark_df(
    spark_session: SparkSession, pandas_df: pd.DataFrame, logger: Logger
):
    """Converts Pandas DataFrame into a Spark DataFrame

    Args:
        - spark_session (SparkSession): The current Spark session
        - pandas_df (pd.DataFrame): A Pandas DataFrame of the ingested tick data
        - logger (Logger): The logger returned from the MainLogger class

    Returns:
        spark_df (DataFrame): A Spark DataFrame of the ingested tick data

    Raises:
        TypeError: If the inputted Pandas DataFrame is None"""
    if pandas_df is None or pandas_df.empty:
        raise TypeError("Pandas DataFrame cannot be None or empty")
    spark_df = spark_session.createDataFrame(pandas_df)
    logger.info("The inputted data has been converted into a Spark DataFrame")
    return spark_df
