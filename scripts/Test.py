import requests
import pandas as pd
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Energy Data Ingestion") \
    .getOrCreate()

# API configuration
BASE_URL = "https://api.energy-charts.org/v1"
HEADERS = {"Accept": "application/json"}

def fetch_public_power(start, end):
    response = requests.get(f"{BASE_URL}/public_power?country=de&start={start}&end={end}", headers=HEADERS)
    return response.json()

def fetch_price_data(start, end):
    response = requests.get(f"{BASE_URL}/price?bzn=DE-LU&start={start}&end={end}", headers=HEADERS)
    return response.json()

def fetch_installed_power():
    response = requests.get(f"{BASE_URL}/installed_power?country=de&time_step=monthly", headers=HEADERS)
    return response.json()

def ingest_data(start_date, end_date):
    public_power_data = fetch_public_power(start_date, end_date)
    price_data = fetch_price_data(start_date, end_date)
    installed_power_data = fetch_installed_power()

    # Processing public power data
    public_power_df = pd.DataFrame(public_power_data['production_types'])
    public_power_df['timestamp'] = pd.to_datetime(public_power_data['unix_seconds'], unit='s')
    public_power_df.to_parquet('public_power.parquet', index=False)

    # Processing price data
    price_df = pd.DataFrame(price_data['price'])
    price_df['timestamp'] = pd.to_datetime(price_data['unix_seconds'], unit='s')
    price_df.to_parquet('price_data.parquet', index=False)

    # Processing installed power data
    installed_power_df = pd.DataFrame(installed_power_data['production_types'])
    installed_power_df['timestamp'] = pd.to_datetime(installed_power_data['time'])
    installed_power_df.to_parquet('installed_power.parquet', index=False)

if __name__ == "__main__":
    ingest_data("2024-01-01", "2024-10-01")  # Example start and end dates