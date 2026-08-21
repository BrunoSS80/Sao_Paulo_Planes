import sys
from datetime import datetime
from pathlib import Path

GOLD_DIR = Path(__file__).resolve().parents[1] / "src" / "gold"
sys.path.insert(0, str(GOLD_DIR))

import pytest
from pyspark.sql import SparkSession, types as T

from aircraft_activity import build_aircraft_activity
from airline_counts import build_airline_counts
from altitude_speed import build_altitude_speed
from monitored_area_duration import build_monitored_area_duration



@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("GoldUnitTests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def snapshot_schema():
    return T.StructType([
        T.StructField("time", T.TimestampType(), False),
        T.StructField("icao24", T.StringType(), True),
        T.StructField("baro_altitude", T.DoubleType(), True),
        T.StructField("velocity", T.DoubleType(), True),
        T.StructField("on_ground", T.BooleanType(), True),
        T.StructField("Nome", T.StringType(), True),
        T.StructField("ICAO", T.StringType(), True),
    ])


@pytest.fixture
def snapshots(spark, snapshot_schema):
    rows = [
        (datetime(2026, 8, 19, 10, 0), "A", 1000.0, 100.0, False, "Alpha", "AAA"),
        (datetime(2026, 8, 19, 10, 20), "A", 1200.0, 120.0, False, "Alpha", "AAA"),
        (datetime(2026, 8, 19, 10, 55), "A", None, 140.0, False, "Alpha", "AAA"),
        (datetime(2026, 8, 19, 11, 10), "A", 1400.0, None, False, "Alpha", "AAA"),
        (datetime(2026, 8, 19, 10, 5), "B", 800.0, 80.0, True, "Beta", "BBB"),
        (datetime(2026, 8, 19, 10, 25), "B", 900.0, 90.0, True, "Beta", "BBB"),
    ]
    return spark.createDataFrame(rows, snapshot_schema)


def test_aircraft_activity_counts_distinct_aircraft(snapshots):
    result = build_aircraft_activity(snapshots).collect()

    assert result[0].active_aircraft_count == 1


def test_airline_counts_uses_airline_dimension(snapshots):
    result = build_airline_counts(snapshots).collect()

    names = {row.airline_name for row in result}
    assert names == {"Alpha", "Beta"}
    assert all(row.aircraft_count == 1 for row in result)


def test_altitude_speed_ignores_null_measures(snapshots):
    result = build_altitude_speed(snapshots).filter("hour = 10").collect()

    assert len(result) == 1
    assert result[0].avg_altitude == pytest.approx(975.0)
    assert result[0].avg_velocity == pytest.approx(106.0)


def test_duration_closes_stale_last_session_and_keeps_current_session_open(snapshots):
    report, sessions = build_monitored_area_duration(snapshots)

    closed = sessions.filter("has_exit = 1").collect()
    durations = sorted(row.duration_seconds for row in closed)
    result = report.collect()

    assert durations == [1200, 1200]
    assert result[0].avg_area_duration_seconds == 1200.0
    assert result[0].sessions_used == 2
    assert sessions.filter("has_exit = 0").count() == 1
