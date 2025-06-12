from pyspark.sql import SparkSession

from utils.safe_run import safe_run


@safe_run()
def read_raw_tick_data(spark: SparkSession, filepath: str):
    parquet_df = spark.read.parquet(filepath)
    return parquet_df
