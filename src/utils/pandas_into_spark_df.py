from logging import Logger
from pyspark.sql import SparkSession
import pandas as pd

from utils.safe_run import safe_run


@safe_run
def pandas_into_spark_df(
    spark_session: SparkSession, pandas_df: pd.DataFrame, logger: Logger
):
    spark_df = spark_session.createDataFrame(pandas_df)
    logger.info("The inputted data has been converted into a Spark DataFrame")
    return spark_df
