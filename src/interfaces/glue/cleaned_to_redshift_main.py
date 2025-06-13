"""The entry point for the Glue job to transform and load S3 data into Redshift"""
import sys
from pyspark import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window, WindowSpec
from pyspark.sql.functions import avg as spark_avg, col, lag, lit, log as spark_log, max as spark_max, min as spark_min, pow as spark_pow, sqrt, sum as spark_sum, when

"""Read cleaned tick data"""
def read_cleaned_tick_data(spark: SparkSession, s3_filepath: str):
    parquet_df = spark.read.parquet(s3_filepath)
    return parquet_df

"""Transform data"""

def rolling_returns(df: DataFrame, rolling_window: WindowSpec):
    df_rolling_returns = df.withColumn("rolling_returns", col("close") - lag("close", 1).over(rolling_window))
    return df_rolling_returns

def momentum(df: DataFrame, rolling_window: WindowSpec):
    df_momentum = df.withColumn("momentum", col("close") - lag("close", 10).over(rolling_window))
    return df_momentum

def vwap(df: DataFrame, rolling_window: WindowSpec):
    df_vwap = df.withColumn("vwap", 
        spark_sum((col("close") + col("high") + col("low")) / 3 * col("volume")).over(rolling_window) /
        spark_sum(col("volume")).over(rolling_window))
    return df_vwap

def volatility(df: DataFrame, rolling_window: WindowSpec):
    df_high_low = df.withColumn("high_low", spark_pow(spark_log(col("high") / col("low")), 2))
    df_rolling_avg_high_low = df_high_low.withColumn("rolling_avg_high_low", spark_avg("high_low").over(rolling_window))
    df_volatility = df_rolling_avg_high_low.withColumn(
        "parkinson_volatility",
        sqrt(1 / (4 * spark_log(lit(2))) * col("rolling_avg_high_low"))
        )
    df_volatility_filtered = df_volatility.drop("high_low", "rolling_avg_high_low")
    return df_volatility_filtered

def rsi(df: DataFrame, rolling_window: WindowSpec):
    df_gains = df.withColumn("gains", when(col("rolling_returns") >= 0, col("rolling_returns")).otherwise(0))
    df_losses = df_gains.withColumn("losses", when(col("rolling_returns") < 0, -col("rolling_returns")).otherwise(0))
    df_avg_gains = df_losses.withColumn("avg_gains", spark_avg("gains").over(rolling_window))
    df_avg_losses = df_avg_gains.withColumn("avg_losses", spark_avg("losses").over(rolling_window))
    df_rs = df_avg_losses.withColumn("rs", when(col("avg_losses") != 0, col("avg_gains") / col("avg_losses")).otherwise(0))
    df_rsi = df_rs.withColumn("rsi", 100 - (100 / (1 + col("rs"))))
    df_rsi_filtered = df_rsi.drop("gains", "losses", "avg_gains", "avg_losses", "rs")
    return df_rsi_filtered

def transform_cleaned_data(df: DataFrame):
    lag_rolling_window = Window.orderBy("date_col")
    ten_day_rolling_window = Window.orderBy("date_col").rowsBetween(-9, 0)
    fourteen_day_rolling_window = Window.orderBy("date_col").rowsBetween(-13, 0)

    df_rolling_returns = rolling_returns(df, lag_rolling_window)
    df_momentum = momentum(df_rolling_returns, lag_rolling_window)
    df_vwap = vwap(df_momentum, ten_day_rolling_window)
    df_volatility = volatility(df_vwap, ten_day_rolling_window)
    df_rsi = rsi(df_volatility, fourteen_day_rolling_window)
    
    return df_rsi

"""Load transformed data to Redshift"""
def load_transformed_data_to_redshift(df):
    pass

def run_cleaned_to_redshift_pipeline():
    glue_context = GlueContext(SparkContext.getOrCreate())
    spark = glue_context.spark_session
    current_date = F.current_date()
    cleaned_s3_bucket_filepath = (
        f"s3a://main-cleaned-tick-data-bucket/cleaned_data/date_col={current_date}"
    )
    df = read_cleaned_tick_data(spark, cleaned_s3_bucket_filepath)
    df_transformed = transform_cleaned_data(df)
    load_transformed_data_to_redshift(df_transformed)

def main(argv=None):
    if argv is None:
        argv = sys.argv
    run_cleaned_to_redshift_pipeline()


if __name__ == "__main__":
    sys.exit(main(sys.argv))