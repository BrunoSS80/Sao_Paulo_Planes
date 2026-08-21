from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.window import Window
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
BRONZE_OUTPUT_PATH = str(BASE_DIR / "data" / "bronze" / "output")
SILVER_OUTPUT_PATH = str(BASE_DIR / "data" / "silver" / "output")
SILVER_CHECKPOINT_PATH = str(BASE_DIR / "data" / "silver" / "checkpoint")
AIRLINES_PATH = str(BASE_DIR / "data" / "dim_airlines" / "airlines.dat")


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

spark = None
query = None

try:
    spark = (SparkSession.builder
             .appName("KafkaConsumer")
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate()
    )

    df_schema = spark.read.parquet(BRONZE_OUTPUT_PATH)
    schema = df_schema.schema

    df = (
        spark.readStream
        .schema(schema)
        .parquet(BRONZE_OUTPUT_PATH)
    )

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

    df_clean = (
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
    df_clean.printSchema()

    query = (
        df_clean.writeStream
        .format("parquet")
        .outputMode("append")
        .trigger(processingTime="35 seconds")
        .partitionBy("year", "month", "day")
        .option("path", SILVER_OUTPUT_PATH)
        .option("checkpointLocation", SILVER_CHECKPOINT_PATH)
        .start()
        .awaitTermination()
    )
except Exception as error:
    print(f"Erro ao executar o pipeline Silver: {error}")
    raise
finally:
    if query is not None:
        query.stop()
    if spark is not None:
        spark.stop()