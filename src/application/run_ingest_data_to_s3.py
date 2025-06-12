"""Conducts the ingestion of data from Yahoo! Finance API to the raw zone in S3"""
from infrastructure.apis.yahoo_finance_api_client import YFinanceClient
from infrastructure.aws.s3_io import load_s3_file_path, write_raw_data_to_s3_bucket
from infrastructure.spark.spark_session_manager import SparkSessionManager
from utils.main_logger import MainLogger
from utils.pandas_into_spark_df import pandas_into_spark_df


def run_ingest_data_to_s3():
    main_logger = MainLogger()
    logger = main_logger.get_logger()
    yfinance_client = YFinanceClient(logger)
    market_data = yfinance_client.fetch_market_data()
    spark_df = pandas_into_spark_df(market_data)
    spark_session_manager = SparkSessionManager(logger)
    raw_s3_filepath = load_s3_file_path()
    write_raw_data_to_s3_bucket(spark_df, spark_session_manager, logger, raw_s3_filepath)