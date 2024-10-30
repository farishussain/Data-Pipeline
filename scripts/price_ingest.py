import os
import datetime
from pyspark.sql import SparkSession
from delta import DeltaTable
from config import Config
from data_quality_checks import fetch_data, validate_data
from utils import save_data_to_staging

spark = SparkSession.builder \
    .appName("Price Ingestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,io.delta:delta-storage:2.4.0") \
    .getOrCreate()

def ingest_price(config):
    api_url = config.api_urls['price']
    params = {"start_date": config.start_date, "end_date": config.end_date}
    data = fetch_data(api_url, params)

    if data and validate_data(data, 'price'):
        timestamps = data['unix_seconds']
        rows = [{"timestamp": datetime.datetime.fromtimestamp(ts), "price": price, "unit": data['unit']}
                for ts, price in zip(timestamps, data['price'])]
        df = spark.createDataFrame(rows)
        save_data_to_staging(spark, df, os.path.join(config.staging_path, 'price'), ["timestamp"])
    else:
        print("No valid data to save for price.")

if __name__ == "__main__":
    config = Config()
    ingest_price(config)