from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def filter_invalid_keys(df: DataFrame) -> DataFrame:
    """
    Remove records without a valid timestamp or turbine ID.

    Args:
        df (DataFrame): Input DataFrame containing turbine data.
    Returns:
            DataFrame: DataFrame with invalid keys removed.
    """
    return df.filter(
        F.col("timestamp").isNotNull()
        & F.col("turbine_id").isNotNull()
    )

def filter_invalid_measurements(df: DataFrame) -> DataFrame:
    """
    Remove rows containing invalid turbine measurements.
    Valid measurements must satisfy:
    - wind_speed >= 0
    - wind_direction between 0 and 360
    - power_output >= 0

    Args:
        df (DataFrame): Input DataFrame containing turbine data.
    Returns:
        DataFrame: DataFrame with invalid measurements removed.
    """
    return df.filter(
        F.col("wind_speed").isNotNull()
        & (F.col("wind_speed") >= 0)
        & F.col("wind_direction").isNotNull()
        & F.col("wind_direction").between(0, 360)
        & F.col("power_output").isNotNull()
        & (F.col("power_output") >= 0)
    )


def remove_duplicate_measurements(df: DataFrame) -> DataFrame:
    """
    Remove duplicate measurements for the same turbine and timestamp.

    Args:
        df (DataFrame): Input DataFrame containing turbine data.
    Returns:
        DataFrame: DataFrame with duplicate measurements removed.
    """
    return df.dropDuplicates(["timestamp", "turbine_id"])


def clean_turbine_data(df: DataFrame) -> DataFrame:
    """
    Clean raw turbine data for downstream analysis.
    Cleaning includes:
        - Removing records with missing timestamp or turbine ID.
        - Removing records with invalid measurements.
        - Removing duplicate turbine/timestamp records.

    Args:
        df (DataFrame): Input DataFrame containing raw turbine data.
    Returns:
        DataFrame: Cleaned turbine data.    
    """
    df = filter_invalid_keys(df)
    df = filter_invalid_measurements(df)
    df = remove_duplicate_measurements(df)

    return df