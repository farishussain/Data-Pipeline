# transform_public_power.py
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import DeltaTable

# Initialize Spark session with Delta Lake package
spark = SparkSession.builder \
    .appName("Public Power Transformation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .getOrCreate()

# Create the database if it doesn't exist
spark.sql("CREATE DATABASE IF NOT EXISTS final")

# Paths for staging and final tables
staging_path = "/workspaces/baywa-data-pipeline/data/public_power"
final_path = "/workspaces/baywa-data-pipeline/final/public_power_trend"

# Read from staging
df = spark.read.format("delta").load(staging_path)

# Daily aggregation for each production type
df_daily = df.withColumn("date", F.to_date("timestamp")).groupBy("date", "production_type") \
    .agg(F.sum("value").alias("net_electricity"))

# Write to final Delta table in the 'final' database
final_table_path = "final.public_power_trend"  # Use the database and table name

if DeltaTable.isDeltaTable(spark, final_table_path):
    delta_table = DeltaTable.forPath(spark, final_table_path)
    delta_table.alias("target").merge(
        df_daily.alias("source"),
        "target.date = source.date AND target.production_type = source.production_type"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    df_daily.write.format("delta").mode("overwrite").saveAsTable(final_table_path)

print("Public power data transformed and saved to final Delta table.")