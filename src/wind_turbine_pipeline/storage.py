from pathlib import Path

from pyspark.sql import DataFrame


def write_dataframe_to_csv(df: DataFrame, output_path: str) -> None:
    """
    Write a Spark DataFrame to a CSV file.

    Args:
        df (DataFrame): DataFrame to store.
        output_path (str): Destination CSV path.
    """
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    df.toPandas().to_csv(output_file, index=False)
