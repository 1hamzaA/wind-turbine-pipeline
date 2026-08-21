from datetime import datetime
from pyspark.sql import Row
from pyspark.sql import functions as F
from wind_turbine_pipeline.analysis import (
    calculate_summary_statistics,
    identify_anomalies,
)


def test_calculate_summary_statistics(spark):
    df = spark.createDataFrame(
        [
            Row(
                timestamp="2022-03-01 00:00:00",
                turbine_id=1,
                power_output=2.0,
            ),
            Row(
                timestamp="2022-03-01 12:00:00",
                turbine_id=1,
                power_output=4.0,
            ),
            Row(
                timestamp="2022-03-01 23:00:00",
                turbine_id=1,
                power_output=6.0,
            ),
            Row(
                timestamp="2022-03-02 00:00:00",
                turbine_id=1,
                power_output=10.0,
            ),
            Row(
                timestamp="2022-03-01 00:00:00",
                turbine_id=2,
                power_output=1.0,
            ),
            Row(
                timestamp="2022-03-01 12:00:00",
                turbine_id=2,
                power_output=3.0,
            ),
            Row(
                timestamp="2022-03-01 23:00:00",
                turbine_id=2,
                power_output=5.0,
            ),
            Row(
                timestamp="2022-03-02 00:00:00",
                turbine_id=2,
                power_output=9.0,
            ),
        ]
    )

    df = df.withColumn(
        "timestamp",
        F.to_timestamp("timestamp"),
    )

    result = calculate_summary_statistics(
        df,
        period_hours=24,
    )

    rows = result.collect()

    assert len(rows) == 4

    first_window = {
        row["turbine_id"]: row
        for row in rows
        if row["window"]["start"]
        == datetime(2022, 3, 1)
    }

    assert first_window[1]["min_power_output"] == 2.0
    assert first_window[1]["max_power_output"] == 6.0
    assert first_window[1]["avg_power_output"] == 4.0

    assert first_window[2]["min_power_output"] == 1.0
    assert first_window[2]["max_power_output"] == 5.0
    assert first_window[2]["avg_power_output"] == 3.0


def test_identify_anomalies(spark):
    df = spark.createDataFrame(
        [
            Row(
                timestamp="2022-03-01 00:00:00",
                turbine_id=1,
                power_output=10.0,
            ),
            Row(
                timestamp="2022-03-01 01:00:00",
                turbine_id=1,
                power_output=10.0,
            ),
            Row(
                timestamp="2022-03-01 02:00:00",
                turbine_id=1,
                power_output=10.0,
            ),
            Row(
                timestamp="2022-03-01 03:00:00",
                turbine_id=1,
                power_output=10.0,
            ),
            Row(
                timestamp="2022-03-01 04:00:00",
                turbine_id=2,
                power_output=10.0,
            ),
            Row(
                timestamp="2022-03-01 05:00:00",
                turbine_id=2,
                power_output=10.0,
            ),
            Row(
                timestamp="2022-03-01 06:00:00",
                turbine_id=2,
                power_output=10.0,
            ),
            Row(
                timestamp="2022-03-01 07:00:00",
                turbine_id=2,
                power_output=10.0,
            ),
            Row(
                timestamp="2022-03-01 08:00:00",
                turbine_id=2,
                power_output=10.0,
            ),
            Row(
                timestamp="2022-03-01 09:00:00",
                turbine_id=2,
                power_output=300.0,
            ),
        ]
    )

    df = df.withColumn(
        "timestamp",
        F.to_timestamp("timestamp"),
    )

    result = identify_anomalies(df,period_hours=24)

    anomalies = result.filter("is_anomaly").collect()

    assert len(anomalies) == 1
    assert anomalies[0]["turbine_id"] == 2
    assert anomalies[0]["power_output"] == 300.0