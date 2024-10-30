import os
from pyspark.sql import SparkSession
from delta import DeltaTable
from config import Config
from data_quality_checks import fetch_data, validate_data
from utils import save_data_to_staging

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
        "time_step": config.frequency,
        "installation_decommission": config.decommission
        }
    data = fetch_data(api_url, params)

    if data and validate_data(data, 'installed_power'):
        years = data['time']
        rows = [
            {"year": int(year), "installed_power": value, "production_type": production['name']}
            for year in years
            for production in data['production_types']
            for value in production['data']
        ]
        df = spark.createDataFrame(rows)
        save_data_to_staging(spark, df, os.path.join(config.staging_path, 'installed_power'), ["year", "production_type"])
    else:
        print("No valid data to save for installed power.")

if __name__ == "__main__":
    config = Config()
    ingest_installed_power(config)