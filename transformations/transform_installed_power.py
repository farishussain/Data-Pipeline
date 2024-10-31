# transform_installed_power.py
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # Importing functions as F for aliasing
from delta import DeltaTable

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Installed Power Transformation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .getOrCreate()

# Paths for staging and final tables
staging_path = "/workspaces/baywa-data-pipeline/data/installed_power"
final_path = "/workspaces/baywa-data-pipeline/final/installed_power"

# Read from staging
df = spark.read.format("delta").load(staging_path)

# Save to final Delta table
if DeltaTable.isDeltaTable(spark, final_path):
    delta_table = DeltaTable.forPath(spark, final_path)
    delta_table.alias("target").merge(
        df.alias("source"),
        "target.year = source.year AND target.production_type = source.production_type"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    df.write.format("delta").mode("overwrite").save(final_path)

print("Installed power data saved to final Delta table.")