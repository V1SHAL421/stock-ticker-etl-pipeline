"""Transform data"""

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (
    avg as spark_avg,
    col,
    lag,
    lit,
    log as spark_log,
    pow as spark_pow,
    sqrt,
    sum as spark_sum,
    when,
)
from pyspark.sql.window import WindowSpec


def rolling_returns(df: DataFrame, rolling_window: WindowSpec):
    df_rolling_returns = df.withColumn(
        "rolling_returns", col("close") - lag("close", 1).over(rolling_window)
    )
    return df_rolling_returns


def momentum(df: DataFrame, rolling_window: WindowSpec):
    df_momentum = df.withColumn(
        "momentum", col("close") - lag("close", 10).over(rolling_window)
    )
    return df_momentum


def vwap(df: DataFrame, rolling_window: WindowSpec):
    df_vwap = df.withColumn(
        "vwap",
        spark_sum((col("close") + col("high") + col("low")) / 3 * col("volume")).over(
            rolling_window
        )
        / spark_sum(col("volume")).over(rolling_window),
    )
    return df_vwap


def volatility(df: DataFrame, rolling_window: WindowSpec):
    df_high_low = df.withColumn(
        "high_low", spark_pow(spark_log(col("high") / col("low")), 2)
    )
    df_rolling_avg_high_low = df_high_low.withColumn(
        "rolling_avg_high_low", spark_avg("high_low").over(rolling_window)
    )
    df_volatility = df_rolling_avg_high_low.withColumn(
        "parkinson_volatility",
        sqrt(1 / (4 * spark_log(lit(2))) * col("rolling_avg_high_low")),
    )
    df_volatility_filtered = df_volatility.drop("high_low", "rolling_avg_high_low")
    return df_volatility_filtered


def rsi(df: DataFrame, rolling_window: WindowSpec):
    df_gains = df.withColumn(
        "gains", when(col("rolling_returns") >= 0, col("rolling_returns")).otherwise(0)
    )
    df_losses = df_gains.withColumn(
        "losses", when(col("rolling_returns") < 0, -col("rolling_returns")).otherwise(0)
    )
    df_avg_gains = df_losses.withColumn(
        "avg_gains", spark_avg("gains").over(rolling_window)
    )
    df_avg_losses = df_avg_gains.withColumn(
        "avg_losses", spark_avg("losses").over(rolling_window)
    )
    df_rs = df_avg_losses.withColumn(
        "rs",
        when(col("avg_losses") != 0, col("avg_gains") / col("avg_losses")).otherwise(0),
    )
    df_rsi = df_rs.withColumn("rsi", 100 - (100 / (1 + col("rs"))))
    df_rsi_filtered = df_rsi.drop("gains", "losses", "avg_gains", "avg_losses", "rs")
    return df_rsi_filtered
