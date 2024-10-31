import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, date_format, sum as spark_sum
from config import Config

spark = SparkSession.builder \
    .appName("Transform - Daily Price Analysis") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,io.delta:delta-storage:2.4.0") \
    .getOrCreate()

config = Config()

processed_price_path = os.path.join(config.processed_path, 'price')
processed_power_path = os.path.join(config.processed_path, 'public_power')
final_path = os.path.join(config.final_path, 'daily_price_analysis')

def transform_daily_price_analysis():
    price_df = spark.read.format("delta").load(processed_price_path)
    power_df = spark.read.format("delta").load(processed_power_path)

    wind_power_df = power_df.filter(col("production_type").isin("onshore wind", "offshore wind")) \
        .withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd")) \
        .groupBy("date", "production_type") \
        .agg(spark_sum("value").alias("total_net_power"))

    daily_price_df = price_df \
        .withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd")) \
        .groupBy("date") \
        .agg(avg("price").alias("avg_price"))

    daily_price_analysis_df = wind_power_df.join(daily_price_df, on="date", how="inner")

    daily_price_analysis_df.write.format("delta").mode("overwrite").save(final_path)

if __name__ == "__main__":
    transform_daily_price_analysis()
    print("Daily price analysis data transformed.")