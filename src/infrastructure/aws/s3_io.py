"""Interactions with Amazon S3"""
from logging import Logger
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession, functions as F
import yaml

from infrastructure.spark.spark_session_manager import SparkSessionManager


def write_raw_data_to_s3_bucket(df: DataFrame, spark_session_manager: SparkSessionManager, logger: Logger):
    with open("src/config/infra/s3.yaml", 'r') as file:
        config = yaml.safe_load(file)

    spark_session_manager.get_spark_session()

    raw_s3_bucket_path = config['s3']['raw_bucket']

    df = df.withColumn("zip_mod", F.col("zip_code").cast("int") % 5)

    df.write.option("useS3ListInplementation", "true").option("compression", "snappy").partitionBy("zip_mod").mode("overwrite").parquet(raw_s3_bucket_path)
    
    logger.info("The Spark DataFrame has been written to the raw S3 bucket")

    spark_session_manager.stop_spark_session()


