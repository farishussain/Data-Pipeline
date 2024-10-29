# ingest_data.py
import os
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import DeltaTable
from config import Config
from data_quality_checks import fetch_data, validate_data

# Initialize Spark session with Delta support
spark = SparkSession.builder \
    .appName("Energy Data Pipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .getOrCreate()

def save_data_to_staging(df, path, merge_columns):
    """
    Saves the given DataFrame to the specified Delta table path, merging on specified columns.
    """
    if DeltaTable.isDeltaTable(spark, path):
        delta_table = DeltaTable.forPath(spark, path)
        delta_table.alias("target").merge(
            df.alias("source"),
            " AND ".join([f"target.{col} = source.{col}" for col in merge_columns])
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        df.write.format("delta").mode("overwrite").save(path)
    print(f"Data saved to staging at {path}.")

def ingest_data(config, endpoint):
    """
    Ingests data from the specified endpoint and saves it to a Delta table in the staging area.
    """
    api_url = config.api_urls[endpoint]
    params = {"start_date": config.start_date, "end_date": config.end_date}
    data = fetch_data(api_url, params)

    if data and validate_data(data, endpoint):
        if endpoint == "price":
            timestamps = data['unix_seconds']
            rows = [{"timestamp": datetime.datetime.fromtimestamp(ts), "price": price, "unit": data['unit']}
                    for ts, price in zip(timestamps, data['price'])]
            df = spark.createDataFrame(rows)
            save_data_to_staging(df, os.path.join(config.staging_path, endpoint), ["timestamp"])

        elif endpoint == "public_power":
            timestamps = data['unix_seconds']
            rows = [
                {"timestamp": datetime.datetime.fromtimestamp(ts), "value": value, "production_type": production['name']}
                for ts in timestamps
                for production in data['production_types']
                for value in production['data']
            ]
            df = spark.createDataFrame(rows)
            save_data_to_staging(df, os.path.join(config.staging_path, endpoint), ["timestamp", "production_type"])

        elif endpoint == "installed_power":
            years = data['time']
            rows = [
                {"year": int(year), "installed_power": value, "production_type": production['name']}
                for year in years
                for production in data['production_types']
                for value in production['data']
            ]
            df = spark.createDataFrame(rows)
            save_data_to_staging(df, os.path.join(config.staging_path, endpoint), ["year", "production_type"])
    else:
        print(f"No valid data to save for {endpoint}.")

if __name__ == "__main__":
    config = Config()
    endpoints = ["public_power", "price", "installed_power"]
    
    for endpoint in endpoints:
        ingest_data(config, endpoint)