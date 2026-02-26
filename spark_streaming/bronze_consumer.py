import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

# --- Config ---
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'ecommerce_events'
BRONZE_PATH = 'data/bronze/events'
CHECKPOINT_PATH = 'data/bronze/checkpoints'

# --- Schema ---
event_schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_timestamp", StringType()),
    StructField("user_id", StringType()),
    StructField("session_id", StringType()),
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("price", DoubleType()),
    StructField("quantity", IntegerType()),
    StructField("total_amount", DoubleType()),
    StructField("device", StringType()),
    StructField("country", StringType()),
    StructField("city", StringType()),
])

def create_spark_session():
    print("Starting Spark session...")
    spark = SparkSession.builder \
        .appName("EcommerceStreamingBronze") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                "io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("Spark session started!")
    return spark

def main():
    spark = create_spark_session()

    # Read from Kafka
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON events
    parsed_stream = raw_stream.select(
        from_json(col("value").cast("string"), event_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "kafka_timestamp")

    # Write to Delta Lake Bronze layer
    query = parsed_stream.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .option("path", BRONZE_PATH) \
        .trigger(processingTime="10 seconds") \
        .start()

    print("Streaming to Bronze Delta Lake at: " + BRONZE_PATH)
    print("Press Ctrl+C to stop.\n")
    query.awaitTermination()

if __name__ == "__main__":
    main()
