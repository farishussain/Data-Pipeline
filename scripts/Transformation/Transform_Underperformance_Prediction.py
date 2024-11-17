import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, to_timestamp
from config import Config

spark = SparkSession.builder \
    .appName("Transform - Underperformance Prediction") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,io.delta:delta-storage:2.4.0") \
    .getOrCreate()

config = Config()

processed_path = os.path.join(config.processed_path, 'public_power')
final_path = os.path.join(config.final_path, 'underperformance_prediction')

def transform_underperformance_prediction():
    processed_df = spark.read.format("delta").load(processed_path)

    prediction_df = processed_df \
        .withColumn("timestamp", to_timestamp(col("timestamp"))) \
        .groupBy("timestamp", "production_type") \
        .agg(avg("value").alias("avg_production"))

    prediction_df.write.format("delta").mode("overwrite").save(final_path)

if __name__ == "__main__":
    transform_underperformance_prediction()
    print("Underperformance prediction data transformed.")
