import requests
import datetime
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from delta import DeltaTable

# Initialize Spark session with Delta support
spark = SparkSession.builder \
    .appName("Energy Data Pipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0") \
    .getOrCreate()

API_BASE_URL = "https://api.energy-charts.info"
ENDPOINT = "/total_power"

def fetch_power_data(start_date, end_date):
    """
    Fetch power data from the Energy-Charts API for the specified date range.
    """
    url = f"{API_BASE_URL}{ENDPOINT}"
    params = {
        "start_date": start_date,
        "end_date": end_date
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        # Debug: print the entire response content for troubleshooting
        print("API Response Status Code:", response.status_code)
        print("API Response Content:", response.text)  # Print full response to check format
        
        # Parse and return JSON
        json_data = response.json()
        
        # Debug: Check if the response contains expected structure
        if isinstance(json_data, list):
            print("Data is in expected list format.")
        elif isinstance(json_data, dict):
            print("Data is in dictionary format. Keys:", json_data.keys())
        else:
            print("Data is in unexpected format:", type(json_data))
        
        return json_data

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

def save_data(data, path):
    """
    Save data in a format that Spark can ingest for Delta Lake (parquet).
    """
    if data:
        df = spark.createDataFrame(data)
        # Convert data to Delta format
        df.write.format("delta").mode("append").save(path)
        print(f"Data saved to {path} in Delta format.")
    else:
        print("No data to save.")

def validate_data(data):
    """
    Perform basic data quality checks.
    """
    if not data or not isinstance(data, list):
        print("Data validation failed: No data or incorrect format returned from API.")
        return False
    required_keys = {"timestamp", "value"}  # Example fields to verify
    for entry in data:
        if not isinstance(entry, dict) or not required_keys.issubset(entry.keys()):
            print("Data validation failed: Missing required fields or incorrect entry format.")
            return False
    print("Data validation passed.")
    return True

def prepare_date_range(frequency):
    """
    Prepare date ranges based on the ingestion frequency.
    """
    today = datetime.date.today()
    if frequency == "daily":
        start_date = today
        end_date = today
    elif frequency == "weekly":
        start_date = today - datetime.timedelta(days=7)
        end_date = today
    elif frequency == "monthly":
        start_date = today - datetime.timedelta(days=30)
        end_date = today
    else:
        raise ValueError("Unsupported frequency. Use 'daily', 'weekly', or 'monthly'.")
    
    return start_date.isoformat() + "T00:00Z", end_date.isoformat() + "T23:59Z"

def main():
    # Choose frequency for data ingestion
    frequency = os.getenv("INGESTION_FREQUENCY", "daily")  # or "weekly", "monthly"
    
    # Define date range for data fetching
    start_date, end_date = prepare_date_range(frequency)
    
    # Fetch data
    data = fetch_power_data(start_date, end_date)
    
    # Validate data
    if validate_data(data):
        # Save data to Delta Lake
        delta_path = "/mnt/delta/energy_data"
        save_data(data, delta_path)
    else:
        print("Data ingestion aborted due to validation failure.")

if __name__ == "__main__":
    main()