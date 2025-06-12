"""Interactions with Amazon S3"""

from datetime import date
from logging import Logger
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F
import yaml

from infrastructure.spark.spark_session_manager import SparkSessionManager
from utils.safe_run import safe_run


@safe_run()
def load_s3_file_path(path="src/config/infra/s3.yaml", bucket: str = "main_raw_bucket"):
    """Loads the file path for the raw S3 bucket with date partitioning

    Args:
        - path (str): The file path to the configuration file for S3

    Returns:
        - raw_s3_bucket_path_with_date (str): The file path to the raw S3 bucket with date partition"""
    with open(path, "r") as file:
        config = yaml.safe_load(file)
    raw_s3_bucket_path = config["s3"][bucket]
    raw_s3_bucket_path_with_date = f"{raw_s3_bucket_path}/{date.today().isoformat()}"
    return raw_s3_bucket_path_with_date


@safe_run()
def write_raw_data_to_s3_bucket(
    df: DataFrame,
    spark_session_manager: SparkSessionManager,
    logger: Logger,
    s3_filepath: str,
):
    (
        """Loads the file path for the raw S3 bucket with date partitioning
    
    Args:
        - df (DataFrame): The Spark DataFrame
        - spark_session_manager (SparkSessionManager): The class that sets up the Spark session
        - logger (Logger): The logger
        - s3_filepath (str): The filepath to the S3 bucket
        
    Raises:
        - ValueError: "The Spark DataFrame does not have any data"""
        ""
    )

    if df is None or df.rdd.isEmpty():
        raise ValueError("The Spark DataFrame does not have any data")

    if not s3_filepath.startswith("s3a://"):
        raise ValueError("The S3 file path is of an incorrect format")

    spark_session_manager.get_spark_session()

    """
    The zip code column is partitioned using the modulo operation % 5 to ensure the data is evenly
    distributed across 5 partitions. This avoids data skew and reduces shuffle overhead.
    """
    df = df.withColumn("date_col", F.current_date())

    print("Add DF Column Passed")

    """
    useS3ListImplementation: optimizes S3 reads for better handling of file listing
    compression -> snappy: Snappy compression is one of the fastest compression algorithms and it is lightweight
    partionBy("zip_mod"): partitions ebable Spark to process the data in parallel, improving scalability
    """
    df.write.option("useS3ListImplementation", "true").option(
        "compression", "snappy"
    ).partitionBy("date_col").mode("overwrite").parquet(s3_filepath)

    print("Write has passed")

    logger.info("The Spark DataFrame has been written to the raw S3 bucket")

    spark_session_manager.stop_spark_session()

