import logging
from pyspark.sql import DataFrame, SparkSession
from wind_turbine_pipeline.analysis import analyse_turbine_data
from wind_turbine_pipeline.cleaning import clean_turbine_data
from wind_turbine_pipeline.ingestion import read_turbine_data
from wind_turbine_pipeline.storage import write_dataframe_to_csv

logger = logging.getLogger(__name__)

def run_pipeline(spark: SparkSession, input_paths: list[str], output_path: str, period_hours: int = 24) -> None:
    """
    Run the complete wind turbine data pipeline.

    Args:
        spark (SparkSession): Spark session.
        input_paths (list[str]): Paths to raw turbine CSV files.
        output_path (str): Directory for processed output.
        period_hours (int): Duration of analysis windows in hours.
        ...
    """
    logger.info("Starting wind turbine pipeline")
    logger.info("Input files: %s", input_paths)
    logger.info("Analysis period: %s hours", period_hours)

    # Ingestion
    logger.info("Starting ingestion")
    df = read_turbine_data(spark, input_paths)
    logger.info("Ingestion complete")

    # Cleaning
    logger.info("Starting data cleaning")
    cleaned = clean_turbine_data(df)
    logger.info("Data cleaning complete")

    # Analysis
    logger.info("Starting analysis")
    summary, anomalies = analyse_turbine_data(
        cleaned,
        period_hours=period_hours,
    )
    logger.info("Analysis complete")

    # Storage
    logger.info("Writing processed data to %s", output_path)
    write_dataframe_to_csv(
        cleaned.orderBy("timestamp", "turbine_id"),
        f"{output_path}/cleaned_turbine_data.csv",
    )

    write_dataframe_to_csv(
        summary.orderBy("window_start","turbine_id"),
        f"{output_path}/summary_statistics.csv",
    )

    write_dataframe_to_csv(
        anomalies.orderBy("timestamp", "turbine_id"),
        f"{output_path}/anomalies.csv",
    )

    logger.info("Processed data written successfully")
    logger.info("Pipeline completed successfully")