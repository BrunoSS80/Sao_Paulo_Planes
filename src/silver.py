import argparse
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.window import Window
from pathlib import Path
import time


BASE_DIR = Path(__file__).resolve().parents[1]
BRONZE_OUTPUT_PATH = str(BASE_DIR / "data" / "bronze" / "output")
SILVER_OUTPUT_PATH = str(BASE_DIR / "data" / "silver" / "output")
SILVER_CHECKPOINT_PATH = str(BASE_DIR / "data" / "silver" / "checkpoint")
AIRLINES_PATH = str(BASE_DIR / "data" / "dim_airlines" / "airlines.dat")


def wait_for_bronze_output(spark, source_path, max_retries=30, initial_wait=2):
    attempt = 0
    wait_time = initial_wait

    while attempt < max_retries:
        try:
            print(f"[Silver] Tentativa {attempt + 1}/{max_retries} de ler schema do Bronze...")
            df_schema = spark.read.parquet(source_path)
            schema = df_schema.schema
            print("[Silver] Schema do Bronze lido com sucesso!")
            return schema
        except Exception as e:
            attempt += 1
            if attempt >= max_retries:
                print(f"[Silver] Falha ao ler Bronze após {max_retries} tentativas")
                raise
            print(f"[Silver] Bronze ainda não disponível. Aguardando {wait_time}s...")
            time.sleep(wait_time)
            wait_time = min(wait_time * 1.5, 30)


AIRLINES_SCHEMA = StructType([
    StructField("Airline ID", StringType(), True),
    StructField("Nome", StringType(), True),
    StructField("Alias", StringType(), True),
    StructField("IATA", StringType(), True),
    StructField("ICAO", StringType(), True),
    StructField("Indicativo", StringType(), True),
    StructField("País", StringType(), True),
    StructField("Active", StringType(), True),
])

def load_airlines(spark):
    airlines = (
        spark.read
        .option("header", "false")
        .option("sep", ",")
        .option("quote", '"')
        .option("escape", '"')
        .option("nullValue", r"\N")
        .schema(AIRLINES_SCHEMA)
        .csv(AIRLINES_PATH)
        .withColumn("ICAO", sf.upper(sf.trim(sf.col("ICAO"))))
        .filter(sf.col("ICAO").isNotNull() & (sf.col("ICAO") != ""))
    )

    airlines_window = Window.partitionBy("ICAO").orderBy(
        sf.when(sf.upper(sf.col("Active")) == "Y", 0).otherwise(1),
        sf.col("Airline ID").cast("long"),
    )
    airlines = (
        airlines.withColumn("icao_rank", sf.row_number().over(airlines_window))
        .filter(sf.col("icao_rank") == 1)
        .drop("icao_rank")
    )
    return airlines

def transform_silver(df, airlines):
    return (
        df.dropDuplicates(["icao24", "last_contact"])
        .dropna(subset=["callsign"])
        .withColumn("time", sf.from_unixtime(sf.col("time")).cast("timestamp"))
        .withColumn("year", sf.year(sf.col("time")))
        .withColumn("month", sf.month(sf.col("time")))
        .withColumn("day", sf.day(sf.col("time")))
        .withColumn(
            "callsign_icao",
            sf.upper(sf.substring(sf.trim(sf.col("callsign")), 1, 3)),
        )
        .join(
            sf.broadcast(airlines),
            sf.col("callsign_icao") == sf.col("ICAO"),
            "left",
        )
        .drop("callsign_icao")
    )


def parse_args():
    parser = argparse.ArgumentParser(description="Processa a camada Silver em streaming.")
    parser.add_argument("--source_path", default=BRONZE_OUTPUT_PATH)
    parser.add_argument("--schema_source", default=BRONZE_OUTPUT_PATH)
    parser.add_argument("--output_path", default=SILVER_OUTPUT_PATH)
    parser.add_argument("--checkpoint_path", default=SILVER_CHECKPOINT_PATH)
    parser.add_argument("--shuffle_partitions", type=int, default=4)
    return parser.parse_args()


def main():
    args = parse_args()
    spark = None
    query = None
    try:
        spark = (SparkSession.builder
                 .master("spark://spark-master:7077")
                 .appName("SilverStreaming")
                 .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
                 .getOrCreate()
        )

        schema = wait_for_bronze_output(spark, args.schema_source)
        df = spark.readStream.schema(schema).parquet(args.source_path)
        airlines = load_airlines(spark)
        df_clean = transform_silver(df, airlines)

        df_clean.printSchema()
        query = (
            df_clean.writeStream
            .outputMode("append")
            .format("parquet")
            .partitionBy("year", "month", "day")
            .option("path", args.output_path)
            .option("checkpointLocation", args.checkpoint_path)
            .trigger(availableNow=True)
            .start()
        )
        print(f"[Silver] Processando arquivos pendentes em {args.source_path}")
        query.awaitTermination()
        print(f"[Silver] Processamento availableNow concluido em {args.output_path}")
    except Exception as error:
        print(f"Erro ao executar o pipeline Silver: {error}")
        raise
    finally:
        if query is not None:
            query.stop()
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()