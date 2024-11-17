import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import DeltaTable
from config import Config

# Initialize Spark session with Delta support
spark = SparkSession.builder \
    .appName("CDC - Public Power") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,io.delta:delta-storage:2.4.0") \
    .getOrCreate()

# Set log level and increase debug max fields
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.debug.maxToStringFields", "100")

config = Config()

# Define paths
staging_path = os.path.join(config.staging_path, 'public_power')
processed_path = os.path.join(config.processed_path, 'public_power')

def apply_cdc():
    # Load source data from the staging area
    staging_df = spark.read.format("delta").load(staging_path)
    
    # Deduplicate source data based on all relevant columns
    deduped_staging_df = staging_df.dropDuplicates(["timestamp", "production_type", "country", "start", "end"])

    # Check if the processed Delta table already exists
    if DeltaTable.isDeltaTable(spark, processed_path):
        processed_table = DeltaTable.forPath(spark, processed_path)

        # Define condition to detect if rows are different for any column
        update_condition = (
            (F.col("processed.value") != F.col("staging.value")) |
            (F.col("processed.production_type") != F.col("staging.production_type")) |
            (F.col("processed.country") != F.col("staging.country")) |
            (F.col("processed.start") != F.col("staging.start")) |
            (F.col("processed.end") != F.col("staging.end"))
            # Add any other columns here if needed
        )

        # Merge data from staging to processed based on the key columns and update any row with differing values
        merge_operation = processed_table.alias("processed").merge(
            deduped_staging_df.alias("staging"),
            "processed.timestamp = staging.timestamp AND processed.production_type = staging.production_type"
        ).whenMatchedUpdate(
            condition=update_condition,
            set={
                "timestamp": "staging.timestamp",
                "value": "staging.value",
                "production_type": "staging.production_type",
                "country": "staging.country",
                "start": "staging.start",
                "end": "staging.end"
                # Include any other columns here if needed
            }
        ).whenNotMatchedInsertAll()

        # Execute the merge and capture the operation metrics
        merge_metrics = merge_operation.execute()

        # Check if merge_metrics is not None and print the number of updated and inserted rows
        if merge_metrics:
            print(f"Number of records updated: {merge_metrics['numTargetRowsUpdated']}")
            print(f"Number of records inserted: {merge_metrics['numTargetRowsInserted']}")
        else:
            print("No records were updated or inserted.")
    else:
        # Initial write if Delta table does not exist
        deduped_staging_df.write.format("delta").mode("overwrite").save(processed_path)
        print("Initial load completed. All records inserted.")

if __name__ == "__main__":
    apply_cdc()
    print("CDC applied for Public Power Data.")