import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import DeltaTable
from config import Config

# Initialize Spark session with Delta support
spark = SparkSession.builder \
    .appName("CDC - Installed Power") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,io.delta:delta-storage:2.4.0") \
    .getOrCreate()

# Set log level and increase debug max fields
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.debug.maxToStringFields", "100")

config = Config()

staging_path = os.path.join(config.staging_path, 'installed_power')
processed_path = os.path.join(config.processed_path, 'installed_power')

def apply_cdc():
    # Load source data from the staging area
    staging_df = spark.read.format("delta").load(staging_path)

    # Deduplicate source data based on key columns
    deduped_staging_df = staging_df.dropDuplicates(["year", "production_type"])

    if DeltaTable.isDeltaTable(spark, processed_path):
        processed_table = DeltaTable.forPath(spark, processed_path)

        # Define condition to detect if rows are different
        update_condition = (
            (F.col("processed.installed_power") != F.col("staging.installed_power")) |
            (F.col("processed.production_type") != F.col("staging.production_type"))
        )

        processed_table.alias("processed").merge(
            deduped_staging_df.alias("staging"),
            "processed.year = staging.year AND processed.production_type = staging.production_type"
        ).whenMatchedUpdate(
            condition=update_condition,
            set={
                "year": "staging.year",
                "installed_power": "staging.installed_power",
                "production_type": "staging.production_type"
            }
        ).whenNotMatchedInsertAll() \
         .execute()
    else:
        # Initial write if Delta table does not exist
        deduped_staging_df.write.format("delta").mode("overwrite").save(processed_path)

if __name__ == "__main__":
    apply_cdc()
    print("CDC applied for Installed Power Data.")