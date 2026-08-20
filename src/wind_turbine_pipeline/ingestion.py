from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StructField,
    StructType,
    TimestampType,
)


turbine_data_schema = StructType(
    [
        StructField("timestamp", TimestampType(), nullable=False),
        StructField("turbine_id", IntegerType(), nullable=False),
        StructField("wind_speed", DoubleType(), nullable=True),
        StructField("wind_direction", IntegerType(), nullable=True),
        StructField("power_output", DoubleType(), nullable=True),
    ]
)

def read_turbine_data(spark: SparkSession, input_path: str) -> DataFrame:
    """
    Read raw turbine CSV data into a Spark DataFrame.

    Args:
        spark (SparkSession): Spark session object.
        input_path (str): Path to the input CSV file.
    """

    return spark.read.option("header", True).schema(turbine_data_schema).csv(input_path) 