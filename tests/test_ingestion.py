from pathlib import Path

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    TimestampType,
)

from wind_turbine_pipeline.ingestion import (
    turbine_data_schema,
    read_turbine_data,
)


fixture_path = Path("tests/fixtures/turbine_data_fixture.csv")


def test_read_turbine_data(spark):
    df = read_turbine_data(spark, str(fixture_path))

    assert df.count() == 3

    assert df.columns == [
        "timestamp",
        "turbine_id",
        "wind_speed",
        "wind_direction",
        "power_output",
    ]

def test_turbine_schema():
    assert isinstance(turbine_data_schema["timestamp"].dataType, TimestampType)
    assert isinstance(turbine_data_schema["turbine_id"].dataType, IntegerType)
    assert isinstance(turbine_data_schema["wind_speed"].dataType, DoubleType)
    assert isinstance(turbine_data_schema["wind_direction"].dataType, IntegerType)
    assert isinstance(turbine_data_schema["power_output"].dataType, DoubleType)