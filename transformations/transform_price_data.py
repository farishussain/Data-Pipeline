# transform_price_data.py
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # Importing functions as F for aliasing
from delta import DeltaTable

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Price Data Transformation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .getOrCreate()

# Paths for staging and final tables
staging_path = "/workspaces/baywa-data-pipeline/data/price"
final_path = "/workspaces/baywa-data-pipeline/final/price_data"

# Read from staging
df = spark.read.format("delta").load(staging_path)

# Daily aggregation
df_daily = df.withColumn("date", F.to_date("timestamp")).groupBy("date") \
    .agg(F.avg("price").alias("average_price"))

# Write to final Delta table
if DeltaTable.isDeltaTable(spark, final_path):
    delta_table = DeltaTable.forPath(spark, final_path)
    delta_table.alias("target").merge(
        df_daily.alias("source"),
        "target.date = source.date"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    df_daily.write.format("delta").mode("overwrite").save(final_path)

print("Price data transformed and saved to final Delta table.")