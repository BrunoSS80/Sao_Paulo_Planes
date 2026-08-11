from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
from datetime import datetime, timezone

spark = (SparkSession.builder
         .appName("KafkaConsumer")
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate()
)

df_schema = spark.read.parquet("../data/bronze/output")

schema = df_schema.schema

df = (
    spark.readStream
    .schema(schema)
    .parquet("../data/bronze/output")
)

df_clean = (
    df.dropDuplicates(["icao24", "last_contact"])
    .dropna(subset=["callsign"])
)

df_clean = (
    df_clean.withColumn("time", sf.from_unixtime(sf.col("time")).cast("timestamp"))
    .withColumn("year", sf.year(sf.col("time")))
    .withColumn("month", sf.month(sf.col("time")))
    .withColumn("day", sf.day(sf.col("time")))
)
df_clean.printSchema()

(
    df_clean.writeStream
    .format("parquet")
    .outputMode("append")
    .trigger(processingTime="35 seconds")
    .partitionBy("year", "month", "day")
    .option("path", "../data/silver/output")
    .option("checkpointLocation", "../data/silver/checkpoint")
    .start()
    .awaitTermination()
)