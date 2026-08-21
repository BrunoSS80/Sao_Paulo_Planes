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


DEFAULT_OUTPUT_PATH = str(DEFAULT_GOLD_PATH / "airline_counts")


def build_airline_counts(
    df: DataFrame,
    airline_name_column: str = "Nome",
    airline_icao_column: str = "ICAO",
) -> DataFrame:
    required = {airline_name_column, airline_icao_column}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(
            "Dimensao de companhia ausente na Silver; faltam: "
            f"{sorted(missing)}"
        )

    prepared = add_time_columns(df)
    return (
        prepared
        .withColumn(
            "airline_name",
            sf.coalesce(
                sf.nullif(sf.trim(sf.col(airline_name_column)), sf.lit("")),
                sf.lit("Desconhecida"),
            ),
        )
        .withColumn(
            "airline_icao",
            sf.coalesce(
                sf.nullif(sf.trim(sf.col(airline_icao_column)), sf.lit("")),
                sf.lit("N/A"),
            ),
        )
        .groupBy("date", "year", "month", "day", "airline_icao", "airline_name")
        .agg(sf.countDistinct("icao24").alias("aircraft_count"))
        .orderBy("date", "airline_name")
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Quantidade de aeronaves observadas por companhia e data."
    )
    add_common_arguments(parser, DEFAULT_OUTPUT_PATH)
    parser.add_argument("--airline_name_column", default="Nome")
    parser.add_argument("--airline_icao_column", default="ICAO")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = None
    try:
        spark = create_spark_session("GoldAirlineCounts", args.shuffle_partitions)
        silver = read_silver(
            spark,
            args.source_path,
            args.timezone,
            {args.airline_name_column, args.airline_icao_column},
            args.run_date,
        )
        result = build_airline_counts(
            silver, args.airline_name_column, args.airline_icao_column
        )
        write_mart(result, args.output_path, ["year", "month", "day"])
        report_result("Aeronaves por companhia", result)
    finally:
        finalize_spark(spark)


if __name__ == "__main__":
    main()
