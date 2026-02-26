from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, countDistinct,
    date_trunc, max as spark_max
)

SILVER_PATH = 'data/silver/events'
GOLD_PATH_REVENUE = 'data/gold/revenue_by_product'
GOLD_PATH_USERS = 'data/gold/user_summary'
GOLD_PATH_FUNNEL = 'data/gold/conversion_funnel'

def create_spark_session():
    spark = SparkSession.builder \
        .appName("EcommerceGoldProcessor") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def main():
    spark = create_spark_session()

    print("Reading Silver layer...")
    silver_df = spark.read.format("delta").load(SILVER_PATH)
    silver_df.cache()
    print("Silver record count: " + str(silver_df.count()))

    # --- Gold Table 1: Revenue by Product ---
    print("\nBuilding Gold Table 1: Revenue by Product...")
    revenue_df = silver_df \
        .filter(col("event_type") == "PURCHASE") \
        .groupBy("product_id", "product_name", "category", "price_bucket") \
        .agg(
            spark_sum("total_amount").alias("total_revenue"),
            count("event_id").alias("total_orders"),
            avg("total_amount").alias("avg_order_value"),
            countDistinct("user_id").alias("unique_buyers")
        ) \
        .orderBy(col("total_revenue").desc())

    revenue_df.write.format("delta").mode("overwrite").option("path", GOLD_PATH_REVENUE).save()
    print("Revenue by Product:")
    revenue_df.show(8, truncate=False)

    # --- Gold Table 2: User Summary ---
    print("\nBuilding Gold Table 2: User Summary...")
    user_df = silver_df \
        .groupBy("user_id") \
        .agg(
            count("event_id").alias("total_events"),
            spark_sum("is_purchase").alias("total_purchases"),
            spark_sum("is_refund").alias("total_refunds"),
            spark_sum(
                col("total_amount") * col("is_purchase")
            ).alias("total_spent"),
            countDistinct("session_id").alias("total_sessions"),
            countDistinct("device").alias("devices_used")
        ) \
        .orderBy(col("total_spent").desc())

    user_df.write.format("delta").mode("overwrite").option("path", GOLD_PATH_USERS).save()
    print("Top Users by Spend:")
    user_df.show(5, truncate=False)

    # --- Gold Table 3: Conversion Funnel ---
    print("\nBuilding Gold Table 3: Conversion Funnel...")
    funnel_df = silver_df \
        .groupBy("event_type") \
        .agg(
            count("event_id").alias("event_count"),
            countDistinct("user_id").alias("unique_users")
        ) \
        .orderBy(col("event_count").desc())

    funnel_df.write.format("delta").mode("overwrite").option("path", GOLD_PATH_FUNNEL).save()
    print("Conversion Funnel:")
    funnel_df.show(truncate=False)

    print("\nGold layer complete!")

if __name__ == "__main__":
    main()
