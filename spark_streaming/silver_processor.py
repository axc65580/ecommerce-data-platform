from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, when, upper, trim, round as spark_round
)

BRONZE_PATH = 'data/bronze/events'
SILVER_PATH = 'data/silver/events'

def create_spark_session():
    spark = SparkSession.builder \
        .appName("EcommerceSilverProcessor") \
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def main():
    spark = create_spark_session()

    print("Reading Bronze layer...")
    bronze_df = spark.read.format("delta").load(BRONZE_PATH)
    print("Bronze record count: " + str(bronze_df.count()))

    # --- Transformations ---
    silver_df = bronze_df \
        .filter(col("event_id").isNotNull()) \
        .filter(col("user_id").isNotNull()) \
        .filter(col("total_amount") > 0) \
        .dropDuplicates(["event_id"]) \
        .withColumn("event_timestamp", to_timestamp(col("event_timestamp"))) \
        .withColumn("event_type", upper(trim(col("event_type")))) \
        .withColumn("category", upper(trim(col("category")))) \
        .withColumn("total_amount", spark_round(col("total_amount"), 2)) \
        .withColumn("is_purchase", when(col("event_type") == "PURCHASE", 1).otherwise(0)) \
        .withColumn("is_refund", when(col("event_type") == "REFUND", 1).otherwise(0)) \
        .withColumn("price_bucket",
            when(col("price") < 25, "budget")
            .when(col("price") < 60, "mid_range")
            .otherwise("premium")
        )

    print("Silver record count after cleaning: " + str(silver_df.count()))

    # Write to Silver Delta Lake
    silver_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("path", SILVER_PATH) \
        .save()

    print("Silver layer written to: " + SILVER_PATH)
    print("Sample records:")
    silver_df.select(
        "event_id", "event_type", "user_id",
        "product_name", "total_amount", "price_bucket", "is_purchase"
    ).show(5, truncate=False)

if __name__ == "__main__":
    main()
