from __future__ import annotations

import argparse

import pyspark.sql.functions as sf
from pyspark.sql import DataFrame

from common import (
    DEFAULT_GOLD_PATH,
    add_common_arguments,
    add_time_columns,
    create_spark_session,
    finalize_spark,
    read_silver,
    report_result,
    write_mart,
)


DEFAULT_OUTPUT_PATH = str(DEFAULT_GOLD_PATH / "altitude_speed")
REQUIRED_COLUMNS = {"baro_altitude", "velocity", "on_ground"}


def build_altitude_speed(df: DataFrame) -> DataFrame:
    prepared = add_time_columns(df)
    return (
        prepared
        .groupBy("date", "hour", "year", "month", "day")
        .agg(
            sf.round(sf.avg("baro_altitude"), 2).alias("avg_altitude"),
            sf.round(sf.avg("velocity"), 2).alias("avg_velocity"),
            sf.count("baro_altitude").alias("altitude_observations"),
            sf.count("velocity").alias("velocity_observations"),
        )
        .orderBy("date", "hour")
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Medias de altitude e velocidade por data e hora."
    )
    add_common_arguments(parser, DEFAULT_OUTPUT_PATH)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = None
    try:
        spark = create_spark_session("GoldAltitudeSpeed", args.shuffle_partitions)
        silver = read_silver(
            spark,
            args.source_path,
            args.timezone,
            REQUIRED_COLUMNS,
            args.run_date,
        )
        result = build_altitude_speed(silver)
        write_mart(result, args.output_path, ["year", "month", "day"])
        report_result("Altitude e velocidade medias", result)
    finally:
        finalize_spark(spark)


if __name__ == "__main__":
    main()
