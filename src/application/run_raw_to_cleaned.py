"""Conducts the Glue job from the raw S3 zone to the cleaned S3 zone"""
from pyspark import SparkContext

from domain.raw_to_cleaned.clean_raw_tick_data import clean_raw_tick_data
from domain.raw_to_cleaned.read_raw_tick_data import read_raw_tick_data
from awsglue.context import GlueContext
from infrastructure.aws.s3_io import load_s3_file_path, write_cleaned_data_to_s3


def run_raw_to_cleaned_pipeline():
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    s3_file_path = load_s3_file_path(bucket="main_clean_bucket")
    df_raw = read_raw_tick_data(spark, s3_file_path)
    df_cleaned = clean_raw_tick_data(df_raw)
    write_cleaned_data_to_s3(df_cleaned, s3_file_path)