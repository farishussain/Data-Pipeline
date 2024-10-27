import requests
import pandas as pd
from config import Config

def fetch_data(api_url, params):
    response = requests.get(api_url, params=params)
    response.raise_for_status()
    return response.json()

def ingest_power_data(config):
    power_data = fetch_data(config.api_urls["power_data"], {"start": config.start_date, "end": config.end_date})
    df = pd.DataFrame(power_data)
    df.to_csv('data/sample_data/power_data.csv', index=False)

if __name__ == "__main__":
    config = Config()
    ingest_power_data(config)