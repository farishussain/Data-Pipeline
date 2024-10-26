# ingest_data.py
import requests
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
from config import API_ENDPOINT, START_DATE, END_DATE, FREQUENCY

# Initialize Spark
spark = SparkSession.builder.appName("EnergyChartsIngestion").getOrCreate()

def fetch_data(api_url):
    response = requests.get(api_url)
    response.raise_for_status()
    return pd.DataFrame(response.json())

# Example function for daily ingestion
def daily_ingest(start_date=START_DATE, end_date=END_DATE):
    data = fetch_data(f"{API_ENDPOINT}/data?start_date={start_date}&end_date={end_date}")
    spark_df = spark.createDataFrame(data)
    spark_df.write.format("delta").mode("overwrite").save("data/delta_tables/power_data")

if __name__ == "__main__":
    daily_ingest()