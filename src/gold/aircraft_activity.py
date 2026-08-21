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


DEFAULT_OUTPUT_PATH = str(DEFAULT_GOLD_PATH / "aircraft_activity")
REQUIRED_COLUMNS = {"on_ground"}


def build_aircraft_activity(df: DataFrame) -> DataFrame:
    prepared = add_time_columns(df)
    return (
        prepared
        .filter(
            sf.col("icao24").isNotNull()
            & (sf.col("on_ground") == sf.lit(False))
        )
        .groupBy("date", "hour", "year", "month", "day")
        .agg(sf.countDistinct("icao24").alias("active_aircraft_count"))
        .orderBy("date", "hour")
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Quantidade de aeronaves observadas por data e hora."
    )
    add_common_arguments(parser, DEFAULT_OUTPUT_PATH)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = None
    try:
        spark = create_spark_session("GoldAircraftActivity", args.shuffle_partitions)
        silver = read_silver(
            spark,
            args.source_path,
            args.timezone,
            required_columns=REQUIRED_COLUMNS,
            run_date=args.run_date,
        )
        result = build_aircraft_activity(silver)
        write_mart(result, args.output_path, ["year", "month", "day"])
        report_result("Aeronaves ativas por horario", result)
    finally:
        finalize_spark(spark)


if __name__ == "__main__":
    main()
