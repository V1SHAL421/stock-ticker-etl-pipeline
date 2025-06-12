"""Conducts the ingestion of data from Yahoo! Finance API to the raw zone in S3"""

from logging import Logger
from infrastructure.apis.yahoo_finance_api_client import YFinanceClient
from infrastructure.aws.s3_io import write_raw_data_to_s3_bucket
from infrastructure.spark.spark_session_manager import SparkSessionManager
from utils.pandas_into_spark_df import pandas_into_spark_df
from utils.safe_run import safe_run


@safe_run()
def run_ingest_data_to_s3(
    logger: Logger, yfinance_client: YFinanceClient, raw_s3_filepath: str
):
    spark_session_manager = SparkSessionManager(logger)
    spark_session = spark_session_manager.get_spark_session()
    market_data = yfinance_client.fetch_market_data()
    spark_df = pandas_into_spark_df(spark_session, market_data, logger)
    write_raw_data_to_s3_bucket(
        spark_df, spark_session_manager, logger, raw_s3_filepath
    )
