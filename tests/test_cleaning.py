from pyspark.sql import Row

from wind_turbine_pipeline.cleaning import (
    clean_turbine_data,
    filter_invalid_keys,
    filter_invalid_measurements,
    remove_duplicate_measurements,
)


def test_filter_invalid_keys(spark):
    rows = [
        Row(timestamp="2022-03-01 00:00:00", turbine_id=1),
        Row(timestamp=None, turbine_id=2),
        Row(timestamp="2022-03-01 02:00:00", turbine_id=None),
    ]

    df = spark.createDataFrame(rows)

    result = filter_invalid_keys(df)

    assert result.count() == 1
    assert result.first().turbine_id == 1


def test_filter_invalid_measurements(spark):
    rows = [
        Row(wind_speed=10.0, wind_direction=180, power_output=2.5),
        Row(wind_speed=-1.0, wind_direction=180, power_output=2.5),
        Row(wind_speed=10.0, wind_direction=400, power_output=2.5),
        Row(wind_speed=10.0, wind_direction=180, power_output=-1.0),
        Row(wind_speed=None, wind_direction=180, power_output=2.5),
    ]

    df = spark.createDataFrame(rows)

    result = filter_invalid_measurements(df)

    assert result.count() == 1


def test_remove_duplicate_measurements(spark):
    rows = [
        Row(
            timestamp="2022-03-01 00:00:00",
            turbine_id=1,
            wind_speed=10.0,
        ),
        Row(
            timestamp="2022-03-01 00:00:00",
            turbine_id=1,
            wind_speed=10.0,
        ),
        Row(
            timestamp="2022-03-01 01:00:00",
            turbine_id=1,
            wind_speed=11.0,
        ),
    ]

    df = spark.createDataFrame(rows)

    result = remove_duplicate_measurements(df)

    assert result.count() == 2


def test_clean_turbine_data(spark):
    rows = [
        Row(
            timestamp="2022-03-01 00:00:00",
            turbine_id=1,
            wind_speed=10.0,
            wind_direction=180,
            power_output=2.5,
        ),
        Row(
            timestamp="2022-03-01 00:00:00",
            turbine_id=1,
            wind_speed=10.0,
            wind_direction=180,
            power_output=2.5,
        ),
        Row(
            timestamp=None,
            turbine_id=2,
            wind_speed=10.0,
            wind_direction=180,
            power_output=2.5,
        ),
        Row(
            timestamp="2022-03-01 01:00:00",
            turbine_id=3,
            wind_speed=-5.0,
            wind_direction=180,
            power_output=2.5,
        ),
    ]

    df = spark.createDataFrame(rows)

    result = clean_turbine_data(df)

    assert result.count() == 1
