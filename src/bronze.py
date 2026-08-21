from pathlib import Path
import os

from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

BASE_DIR = Path(__file__).resolve().parents[1]
BRONZE_OUTPUT_PATH = str(BASE_DIR / "data" / "bronze" / "output")
BRONZE_CHECKPOINT_PATH = str(BASE_DIR / "data" / "bronze" / "checkpoint")


def create_spark_session():
    return (
        SparkSession.builder
        .appName("BronzeKafkaConsumer")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0")
        #.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def get_json_schema():
    return StructType([
        StructField("time", LongType(), True),
        StructField(
            "states",
            ArrayType(
                StructType([
                    StructField("icao24", StringType(), True),
                    StructField("callsign", StringType(), True),
                    StructField("origin_country", StringType(), True),
                    StructField("time_position", LongType(), True),
                    StructField("last_contact", LongType(), True),
                    StructField("longitude", DoubleType(), True),
                    StructField("latitude", DoubleType(), True),
                    StructField("baro_altitude", DoubleType(), True),
                    StructField("on_ground", BooleanType(), True),
                    StructField("velocity", DoubleType(), True),
                    StructField("true_track", DoubleType(), True),
                    StructField("vertical_rate", DoubleType(), True),
                    StructField("geo_altitude", DoubleType(), True),
                    StructField("spi", BooleanType(), True),
                    StructField("position_source", IntegerType(), True),
                    StructField("category", IntegerType(), True),
                ])
            ),
            True,
        ),
    ])


def main():
    spark = create_spark_session()
    query = None

    try:
        df = (
            spark.readStream
            .format("kafka")
            .option(
                "kafka.bootstrap.servers",
                os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
            )
            .option("subscribe", "plane_data")
            .option("startingOffsets", "earliest")
            .load()
        )

        kafka_json_df = df.withColumn("value", sf.col("value").cast("string"))

        streaming_df = kafka_json_df.withColumn(
            "values_json",
            sf.from_json(sf.col("value"), get_json_schema()),
        )
        streaming_df = streaming_df.selectExpr("values_json.*")

        streaming_df = (
            streaming_df.withColumn("states", sf.explode("states"))
            .withColumn("icao24", sf.col("states.icao24"))
            .withColumn("callsign", sf.col("states.callsign"))
            .withColumn("origin_country", sf.col("states.origin_country"))
            .withColumn("time_position", sf.col("states.time_position"))
            .withColumn("last_contact", sf.col("states.last_contact"))
            .withColumn("longitude", sf.col("states.longitude"))
            .withColumn("latitude", sf.col("states.latitude"))
            .withColumn("baro_altitude", sf.col("states.baro_altitude"))
            .withColumn("on_ground", sf.col("states.on_ground"))
            .withColumn("velocity", sf.col("states.velocity"))
            .withColumn("true_track", sf.col("states.true_track"))
            .withColumn("vertical_rate", sf.col("states.vertical_rate"))
            .withColumn("geo_altitude", sf.col("states.geo_altitude"))
            .withColumn("spi", sf.col("states.spi"))
            .withColumn("position_source", sf.col("states.position_source"))
            .withColumn("category", sf.col("states.category"))
            .drop("states")
        )

        print("Schema do stream de bronze:")
        streaming_df.printSchema()

        query = (
            streaming_df.writeStream
            .format("parquet")
            .outputMode("append")
            .trigger(processingTime="35 seconds")
            .option("path", BRONZE_OUTPUT_PATH)
            .option("checkpointLocation", BRONZE_CHECKPOINT_PATH)
            .start()
        )
        print("Dados Gravados!")
        query.awaitTermination()

    finally:
        if query is not None:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
