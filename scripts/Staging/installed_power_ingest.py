import os
from pyspark.sql import SparkSession
from delta import DeltaTable
from config import Config
from data_quality_checks import fetch_data, validate_data
from utils import save_data_to_staging

# Initialize Spark session with Delta support
spark = SparkSession.builder \
    .appName("Installed Power Ingestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,io.delta:delta-storage:2.4.0") \
    .getOrCreate()

def ingest_installed_power(config):
    api_url = config.api_urls['installed_power']
    params = {
        "country": config.country,
        "time_step": config.time_step,
        "installation_decommission": config.installation_decommission
    }
    data = fetch_data(api_url, params)

    if data and validate_data(data, 'installed_power'):
        years = data['time']
        rows = [
            {
                "year": int(year), 
                "installed_power": value, 
                "production_type": production['name'],
                "country": params["country"],                  # Add country
                "time_step": params["time_step"],              # Add time_step
                "installation_decommission": params["installation_decommission"]  # Add installation_decommission
            }
            for year in years
            for production in data['production_types']
            for value in production['data']
        ]
        
        # Create DataFrame with new columns included
        df = spark.createDataFrame(rows)

        # Define staging path
        stage_path = os.path.join(config.staging_path, 'installed_power')

        # Truncate the staging table if it exists
        if DeltaTable.isDeltaTable(spark, stage_path):
            delta_table = DeltaTable.forPath(spark, stage_path)
            delta_table.delete()  # Deletes all data in the Delta table

        # Save new data to staging
        save_data_to_staging(spark, df, stage_path, ["year", "production_type"])
    else:
        print("No valid data to save for installed power.")

if __name__ == "__main__":
    config = Config()
    ingest_installed_power(config)