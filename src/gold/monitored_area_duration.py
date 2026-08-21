from __future__ import annotations

import argparse

import pyspark.sql.functions as sf
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

from common import (
    BASE_REQUIRED_COLUMNS,
    DEFAULT_GOLD_PATH,
    add_common_arguments,
    create_spark_session,
    finalize_spark,
    read_silver,
    report_result,
    write_mart,
)


DEFAULT_OUTPUT_PATH = str(DEFAULT_GOLD_PATH / "monitored_area_duration")


def build_sessions(df: DataFrame, session_gap_seconds: int) -> DataFrame:
    observation_window = Window.partitionBy("icao24").orderBy("time")
    dataset_window = Window.partitionBy()
    session_window = (
        Window.partitionBy("icao24")
        .orderBy("time")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    observed = (
        df.withColumn("previous_time", sf.lag("time").over(observation_window))
        .withColumn("next_time", sf.lead("time").over(observation_window))
        .withColumn("dataset_end", sf.max("time").over(dataset_window))
        .withColumn(
            "new_session",
            sf.when(
                sf.col("previous_time").isNull()
                | (
                    sf.col("time").cast("long")
                    - sf.col("previous_time").cast("long")
                    > session_gap_seconds
                ),
                1,
            ).otherwise(0),
        )
        .withColumn("session_id", sf.sum("new_session").over(session_window))
        .withColumn(
            "session_closes",
            sf.when(
                (
                    sf.col("next_time").isNotNull()
                    & (
                        sf.col("next_time").cast("long")
                        - sf.col("time").cast("long")
                        > session_gap_seconds
                    )
                )
                | (
                    sf.col("next_time").isNull()
                    & (
                        sf.col("dataset_end").cast("long")
                        - sf.col("time").cast("long")
                        > session_gap_seconds
                    )
                ),
                1,
            ).otherwise(0),
        )
    )

    return (
        observed.groupBy("icao24", "session_id")
        .agg(
            sf.min("time").alias("entry_ts"),
            sf.max("time").alias("exit_ts"),
            sf.max("session_closes").alias("has_exit"),
            sf.first("Nome", ignorenulls=True).alias("airline_name"),
            sf.first("ICAO", ignorenulls=True).alias("airline_icao"),
        )
        .withColumn(
            "duration_seconds",
            sf.col("exit_ts").cast("long") - sf.col("entry_ts").cast("long"),
        )
        .withColumn("date", sf.to_date("entry_ts"))
        .withColumn("year", sf.year("entry_ts"))
        .withColumn("month", sf.month("entry_ts"))
        .withColumn("day", sf.dayofmonth("entry_ts"))
    )


def build_monitored_area_duration(
    df: DataFrame, session_gap_seconds: int = 30 * 60
) -> tuple[DataFrame, DataFrame]:
    sessions = build_sessions(df, session_gap_seconds)
    closed_sessions = sessions.filter(
        (sf.col("has_exit") == 1) & (sf.col("duration_seconds") > 0)
    )
    report = (
        closed_sessions.groupBy("date", "year", "month", "day")
        .agg(
            sf.round(sf.avg("duration_seconds"), 2).alias(
                "avg_area_duration_seconds"
            ),
            sf.count("*").alias("sessions_used"),
        )
        .orderBy("date")
    )
    return report, sessions


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Tempo medio de permanencia por sessoes de aeronaves."
    )
    add_common_arguments(parser, DEFAULT_OUTPUT_PATH)
    parser.add_argument("--session_gap", type=int, default=30 * 60)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = None
    try:
        spark = create_spark_session("GoldMonitoredAreaDuration", args.shuffle_partitions)
        silver = read_silver(
            spark,
            args.source_path,
            args.timezone,
            {"Nome", "ICAO"},
            run_date=None,
        )
        result, sessions = build_monitored_area_duration(silver, args.session_gap)
        if args.run_date:
            result = result.filter(sf.col("date") == sf.to_date(sf.lit(args.run_date)))
        write_mart(result, args.output_path, ["year", "month", "day"])
        report_result("Duracao media na area monitorada", result)
        quality = sessions.agg(
            sf.count("*").alias("sessions_total"),
            sf.sum(sf.when(sf.col("has_exit") == 1, 1).otherwise(0)).alias(
                "sessions_closed"
            ),
            sf.sum(sf.when(sf.col("has_exit") == 0, 1).otherwise(0)).alias(
                "sessions_open"
            ),
            sf.sum(
                sf.when(
                    (sf.col("has_exit") == 1)
                    & (sf.col("duration_seconds") <= 0),
                    1,
                ).otherwise(0)
            ).alias("sessions_discarded_non_positive"),
        )
        quality.show(truncate=False)
    finally:
        finalize_spark(spark)


if __name__ == "__main__":
    main()
