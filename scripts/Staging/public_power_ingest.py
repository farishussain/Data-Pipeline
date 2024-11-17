import os
import datetime
from pyspark.sql import SparkSession
from delta import DeltaTable
from config import Config
from data_quality_checks import fetch_data, validate_data
from utils import save_data_to_staging

# Initialize Spark session with Delta support
spark = SparkSession.builder \
    .appName("Public Power Ingestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,io.delta:delta-storage:2.4.0") \
    .getOrCreate()

def ingest_public_power(config):
    api_url = config.api_urls['public_power']
    params = {
        "country": config.country, 
        "start": config.start_date, 
        "end": config.end_date
    }
    data = fetch_data(api_url, params)

    if data and validate_data(data, 'public_power'):
        timestamps = data['unix_seconds']
        rows = [
            {
                "timestamp": datetime.datetime.fromtimestamp(ts), 
                "value": value, 
                "production_type": production['name'],
                "country": params["country"],   # Add country to each row
                "start": params["start"],       # Add start date to each row
                "end": params["end"]            # Add end date to each row
            }
            for ts in timestamps
            for production in data['production_types']
            for value in production['data']
        ]
        
        # Create DataFrame with new columns included
        df = spark.createDataFrame(rows)

        # Define staging path
        stage_path = os.path.join(config.staging_path, 'public_power')

        # Truncate the staging table if it exists
        if DeltaTable.isDeltaTable(spark, stage_path):
            delta_table = DeltaTable.forPath(spark, stage_path)
            delta_table.delete()  # Deletes all data in the Delta table

        # Save new data to staging
        save_data_to_staging(spark, df, stage_path, ["timestamp", "production_type"])    
    else:
        print("No valid data to save for public power.")

if __name__ == "__main__":
    config = Config()
    ingest_public_power(config)