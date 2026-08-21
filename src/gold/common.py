from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import pyspark.sql.functions as sf
from pyspark.sql import DataFrame, SparkSession


BASE_DIR = Path(__file__).resolve().parents[2]
DEFAULT_SOURCE_PATH = str(BASE_DIR / "data" / "silver" / "output")
DEFAULT_GOLD_PATH = BASE_DIR / "data" / "gold"

BASE_REQUIRED_COLUMNS = {
    "time",
    "icao24",
    "year",
    "month",
    "day",
}


def create_spark_session(app_name: str, shuffle_partitions: int) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .getOrCreate()
    )


def add_common_arguments(parser: argparse.ArgumentParser, default_output: str) -> None:
    parser.add_argument("--source_path", default=DEFAULT_SOURCE_PATH)
    parser.add_argument("--output_path", default=default_output)
    parser.add_argument("--run_date", default=None)
    parser.add_argument("--timezone", default="UTC")
    parser.add_argument("--shuffle_partitions", type=int, default=4)


def validate_columns(df: DataFrame, required_columns: Iterable[str]) -> None:
    missing_columns = set(required_columns).difference(df.columns)
    if missing_columns:
        raise ValueError(
            "Contrato Silver incompleto; faltam as colunas: "
            f"{sorted(missing_columns)}"
        )

    time_type = df.schema["time"].dataType.simpleString()
    if time_type != "timestamp":
        raise TypeError(
            f"A coluna time deve ser timestamp na Silver; tipo encontrado: {time_type}"
        )


def read_silver(
    spark: SparkSession,
    source_path: str,
    timezone: str,
    required_columns: Iterable[str],
    run_date: str | None = None,
) -> DataFrame:
    spark.conf.set("spark.sql.session.timeZone", timezone)
    df = spark.read.parquet(source_path)
    validate_columns(df, set(BASE_REQUIRED_COLUMNS).union(required_columns))

    df = df.filter(sf.col("icao24").isNotNull() & sf.col("time").isNotNull())
    if run_date:
        df = df.filter(sf.to_date("time") == sf.to_date(sf.lit(run_date)))
    return df


def add_time_columns(df: DataFrame, timestamp_column: str = "time") -> DataFrame:
    return (
        df.withColumn("date", sf.to_date(timestamp_column))
        .withColumn("hour", sf.hour(timestamp_column))
        .withColumn("year", sf.year(timestamp_column))
        .withColumn("month", sf.month(timestamp_column))
        .withColumn("day", sf.dayofmonth(timestamp_column))
    )


def write_mart(df: DataFrame, output_path: str, partition_columns: list[str]) -> None:
    (
        df.write
        .mode("overwrite")
        .format("parquet")
        .partitionBy(*partition_columns)
        .save(output_path)
    )


def report_result(name: str, df: DataFrame) -> None:
    print(f"{name}: {df.count()} linhas")
    df.printSchema()


def finalize_spark(spark: SparkSession | None) -> None:
    if spark is not None:
        spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Gera todos os relatorios da camada Gold."
    )
    parser.add_argument("--source_path", default=DEFAULT_SOURCE_PATH)
    parser.add_argument("--output_path", default=str(DEFAULT_GOLD_PATH))
    parser.add_argument("--run_date", default=None)
    parser.add_argument("--timezone", default="UTC")
    parser.add_argument("--shuffle_partitions", type=int, default=4)
    parser.add_argument("--session_gap", type=int, default=30 * 60)
    return parser.parse_args()


def generate_all_reports(spark: SparkSession, args: argparse.Namespace) -> None:
    from aircraft_activity import build_aircraft_activity
    from airline_counts import build_airline_counts
    from altitude_speed import REQUIRED_COLUMNS as ALTITUDE_SPEED_COLUMNS
    from altitude_speed import build_altitude_speed
    from monitored_area_duration import build_monitored_area_duration

    required_columns = (
        set(ALTITUDE_SPEED_COLUMNS)
        | {"Nome", "ICAO"}
    )
    silver = read_silver(
        spark,
        args.source_path,
        args.timezone,
        required_columns,
        args.run_date,
    )

    reports = [
        (
            "Aeronaves ativas por horario",
            build_aircraft_activity(silver),
            "aircraft_activity",
        ),
        (
            "Aeronaves por companhia",
            build_airline_counts(silver),
            "airline_counts",
        ),
        (
            "Altitude e velocidade medias",
            build_altitude_speed(silver),
            "altitude_speed",
        ),
    ]

    duration_report, sessions = build_monitored_area_duration(
        silver, args.session_gap
    )
    reports.append(
        (
            "Duracao media na area monitorada",
            duration_report,
            "monitored_area_duration",
        )
    )

    for name, report, directory in reports:
        output_path = str(Path(args.output_path) / directory)
        write_mart(report, output_path, ["year", "month", "day"])
        report_result(name, report)

    quality = sessions.agg(
        sf.count("*").alias("sessions_total"),
        sf.sum(sf.when(sf.col("has_exit") == 1, 1).otherwise(0)).alias(
            "sessions_closed"
        ),
        sf.sum(sf.when(sf.col("has_exit") == 0, 1).otherwise(0)).alias(
            "sessions_open"
        ),
    )
    print("Qualidade das sessoes:")
    quality.show(truncate=False)


def main() -> None:
    args = parse_args()
    spark = None
    try:
        spark = create_spark_session("GoldAllReports", args.shuffle_partitions)
        generate_all_reports(spark, args)
    finally:
        finalize_spark(spark)


if __name__ == "__main__":
    main()
