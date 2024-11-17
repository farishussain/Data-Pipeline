import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, sum as spark_sum
from config import Config

spark = SparkSession.builder \
    .appName("Transform - Daily Production Trend") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,io.delta:delta-storage:2.4.0") \
    .getOrCreate()

config = Config()

processed_path = os.path.join(config.processed_path, 'public_power')
final_path = os.path.join(config.final_path, 'daily_production_trend')

def transform_daily_production_trend():
    processed_df = spark.read.format("delta").load(processed_path)

    daily_production_df = processed_df \
        .withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd")) \
        .groupBy("date", "production_type") \
        .agg(spark_sum("value").alias("total_production"))

    daily_production_df.write.format("delta").mode("overwrite").save(final_path)

if __name__ == "__main__":
    transform_daily_production_trend()
    print("Daily Production Trend Data Transformed.")
