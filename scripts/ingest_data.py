import requests
import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import DeltaTable

# Initialize Spark session with Delta support
spark = SparkSession.builder \
    .appName("Energy Data Pipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .getOrCreate()

API_BASE_URL = "https://api.energy-charts.info"
ENDPOINTS = {
    "public_power": "/public_power",
    "price": "/price",  # Replace with actual endpoint
    "installed_power": "/installed_power"  # Replace with actual endpoint
}

STAGING_PATH = "/workspaces/baywa-data-pipeline/data"  # Staging directory for raw data

def fetch_data(endpoint, start_date, end_date):
    """
    Fetch data from the Energy-Charts API for the specified date range and endpoint.
    """
    url = f"{API_BASE_URL}{ENDPOINTS[endpoint]}"
    params = {
        "start_date": start_date,
        "end_date": end_date
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        json_data = response.json()
        return json_data  # Return the full response for further processing

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

def save_data_to_staging(data, endpoint):
    """
    Save the raw data to a staging area in Delta format.
    """
    if data:
        timestamps = data['unix_seconds']
        production_types = data['production_types']

        # Convert timestamps to datetime and prepare a DataFrame
        datetime_index = [datetime.datetime.fromtimestamp(ts) for ts in timestamps]
        rows = []
        for production in production_types:
            name = production['name']
            values = production['data']
            for timestamp, value in zip(datetime_index, values):
                rows.append({"timestamp": timestamp, "value": value, "production_type": name})

        df = spark.createDataFrame(rows)
        
        # Save to staging
        staging_path = os.path.join(STAGING_PATH, endpoint)
        ensure_delta_path_exists(staging_path)
        df.write.format("delta").mode("append").save(staging_path)
        print(f"Data saved to staging at {staging_path}.")
    else:
        print("No data to save.")

def ensure_delta_path_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Created directory: {path}")
    else:
        print(f"Directory already exists: {path}")

def validate_data(data):
    if not data:
        print("Data validation failed: No data returned from API.")
        return False

    if 'unix_seconds' not in data or 'production_types' not in data:
        print("Data validation failed: Expected keys not found in response.")
        return False

    if not isinstance(data['production_types'], list):
        print("Data validation failed: Expected 'production_types' to be a list.")
        return False

    print("Data validation passed.")
    return True

def prepare_date_range(frequency, custom_start=None, custom_end=None):
    today = datetime.date.today()
    if custom_start and custom_end:
        start_date = custom_start
        end_date = custom_end
    elif frequency == "daily":
        start_date = today
        end_date = today
    elif frequency == "weekly":
        start_date = today - datetime.timedelta(days=7)
        end_date = today
    elif frequency == "monthly":
        start_date = today - datetime.timedelta(days=30)
        end_date = today
    else:
        raise ValueError("Unsupported frequency. Use 'daily' or 'monthly'.")
    
    return start_date.isoformat() + "T00:00Z", end_date.isoformat() + "T23:59Z"

def ingest_data(frequency, endpoint):
    start_date, end_date = prepare_date_range(frequency)

    # Fetch data
    data = fetch_data(endpoint, start_date, end_date)
    
    # Validate and save data to staging
    if validate_data(data):
        save_data_to_staging(data, endpoint)
    else:
        print("Data ingestion aborted due to validation failure.")

def main():
    frequency = os.getenv("INGESTION_FREQUENCY", "daily")  # or "monthly"
    
    # Ingest public power data
    ingest_data(frequency, "public_power")
    
    # Ingest price data (adjust frequency as needed)
    ingest_data(frequency, "price")
    
    # Ingest installed power data (adjust frequency as needed)
    ingest_data("monthly", "installed_power")

if __name__ == "__main__":
    main()