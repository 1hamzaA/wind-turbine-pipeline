from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def calculate_summary_statistics(df: DataFrame, period_hours: int = 24) -> DataFrame:
    """
    Calculate summary statistics for each turbine within fixed time windows.

    The data is divided into time windows of the specified duration, and the minimum, maximum, and average power output are calculated for each turbine within each window.

    Args:
        df (DataFrame): Cleaned turbine data.
        period_hours (int): Duration of each time window in hours. Defaults to 24.

    Returns:
        DataFrame: Summary statistics for each turbine and time window.
    """
    return (
        df.groupBy(
            F.window("timestamp", f"{period_hours} hours"),
            "turbine_id",
        )
        .agg(
            F.min("power_output").alias("min_power_output"),
            F.max("power_output").alias("max_power_output"),
            F.avg("power_output").alias("avg_power_output"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.lit(period_hours).alias("period_hours"),
            "turbine_id",
            "min_power_output",
            "max_power_output",
            "avg_power_output",
        )
    )


def identify_anomalies(df: DataFrame, period_hours: int = 24) -> DataFrame:
    """
    Identify anomalous power output measurements within each time window.

    A measurement is considered anomalous when its power output is more than two standard deviations from the mean power output within its time window.

    Args:
        df (DataFrame): Cleaned turbine data.
        period_hours (int): Duration of each time window in hours. Defaults to 24.

    Returns:
        DataFrame: Input data with an `is_anomaly` column.
    """
    windowed_stats = df.groupBy(
    F.window("timestamp", f"{period_hours} hours"),
    "turbine_id",
    ).agg(
        F.mean("power_output").alias("mean_power_output"),
        F.stddev("power_output").alias("stddev_power_output"),
    )

    return (
        df.withColumn(
            "window",
            F.window("timestamp", f"{period_hours} hours"),
        )
        .join(windowed_stats, on=["window", "turbine_id"], how="left")
        .withColumn(
            "is_anomaly",
            (F.col("power_output") < ( F.col("mean_power_output") - 2 * F.col("stddev_power_output")))
            | (F.col("power_output") > ( F.col("mean_power_output") + 2 * F.col("stddev_power_output"))),
        )
        .drop(
            "window",
            "mean_power_output",
            "stddev_power_output",
        )
    )


def analyse_turbine_data(df: DataFrame, period_hours: int = 24) -> tuple[DataFrame, DataFrame]:
    """
    Run the turbine data analysis.

    Summary statistics and anomalies are calculated within the specified time window.

    Args:
        df (DataFrame): Cleaned turbine data.
        period_hours (int): Duration of each time window in hours. Defaults to 24.

    Returns:
        tuple[DataFrame, DataFrame]:
            Summary statistics and anomaly results.
    """
    summary = calculate_summary_statistics(df, period_hours)
    anomalies = identify_anomalies(df, period_hours)

    return summary, anomalies
