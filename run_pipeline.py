import logging
from pyspark.sql import SparkSession
from wind_turbine_pipeline.orchestrator import run_pipeline

logging.basicConfig(level=logging.INFO)

def main():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("wind-turbine-pipeline")
        .getOrCreate()
    )

    input_paths = [
        "data/raw/data_group_1.csv",
        "data/raw/data_group_2.csv",
        "data/raw/data_group_3.csv",
    ]

    output_path = "data/processed"

    try:
        run_pipeline(
            spark=spark,
            input_paths=input_paths,
            output_path=output_path,
            period_hours=24,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
