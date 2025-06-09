"""Interactions with Amazon S3"""
from logging import Logger
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession, functions as F
import yaml

from infrastructure.spark.spark_session_manager import SparkSessionManager
from utils.safe_run import safe_run


@safe_run()
def write_raw_data_to_s3_bucket(df: DataFrame, spark_session_manager: SparkSessionManager, logger: Logger):
    with open("src/config/infra/s3.yaml", 'r') as file:
        config = yaml.safe_load(file)

    spark_session_manager.get_spark_session()

    raw_s3_bucket_path = config['s3']['raw_bucket']
    raw_s3_bucket_path_with_date = raw_s3_bucket_path + str(F.current_date())

    """
    The zip code column is partitioned using the modulo operation % 5 to ensure the data is evenly
    distributed across 5 partitions. This avoids data skew and reduces shuffle overhead.
    """
    df = df.withColumn("date_col", F.current_date())

    """
    useS3ListImplementation: optimizes S3 reads for better handling of file listing
    compression -> snappy: Snappy compression is one of the fastest compression algorithms and it is lightweight
    partionBy("zip_mod"): partitions ebable Spark to process the data in parallel, improving scalability
    """
    df.write.option("useS3ListImplementation", "true").option("compression", "snappy").partitionBy("date_col").mode("overwrite").parquet(raw_s3_bucket_path_with_date)

    logger.info("The Spark DataFrame has been written to the raw S3 bucket")

    spark_session_manager.stop_spark_session()


