import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    print("\nCreating Spark session...", flush=True)

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("wind-turbine-pipeline-tests")
        .getOrCreate()
    )

    print("Spark session created.", flush=True)

    yield spark

    print("Stopping Spark session...", flush=True)
    spark.stop()
    print("Spark session stopped.", flush=True)