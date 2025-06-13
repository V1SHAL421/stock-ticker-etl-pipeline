"""The entry point for the Glue job to clean raw S3 data and load into cleaned zone of S3"""

import sys
from pyspark import SparkContext
from awsglue.context import GlueContext
from datetime import date
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession


def read_raw_tick_data(spark: SparkSession, s3_filepath: str):
    parquet_df = spark.read.parquet(s3_filepath)
    return parquet_df


def clean_raw_tick_data(df: DataFrame):
    df_cleaned = df.dropna().dropDuplicates().filter("date_col IS NOT NULL")
    return df_cleaned


def write_cleaned_data_to_s3(df: DataFrame, s3_filepath: str):
    if df is None or df.rdd.isEmpty():
        raise ValueError("The Spark DataFrame does not have any data")

    if not s3_filepath.startswith("s3a://"):
        raise ValueError("The S3 file path is of an incorrect format")

    df.write.option("useS3ListImplementation", "true").option(
        "compression", "snappy"
    ).partitionBy("date_col").mode("overwrite").parquet(s3_filepath)


def run_raw_to_cleaned_pipeline():
    glue_context = GlueContext(SparkContext.getOrCreate())
    spark = glue_context.spark_session
    raw_s3_bucket_filepath = (
        f"s3a://main-raw-tick-data-bucket/{date.today().isoformat()}"
    )
    cleaned_s3_bucket_filepath = "s3a://main-cleaned-tick-data-bucket/cleaned_data/"

    df_raw = read_raw_tick_data(spark, raw_s3_bucket_filepath)
    df_cleaned = clean_raw_tick_data(df_raw)
    write_cleaned_data_to_s3(df_cleaned, cleaned_s3_bucket_filepath)


def main(argv=None):
    if argv is None:
        argv = sys.argv
    run_raw_to_cleaned_pipeline()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
