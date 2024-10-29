# data_quality_checks.py
import requests

def fetch_data(api_url, params):
    """
    Fetches data from a specified API URL with provided parameters.
    """
    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {api_url}: {e}")
        return None

def validate_data(data, data_type):
    """
    Validates that the required keys are present in the data based on the data type.
    """
    required_keys = {
        "price": ["unix_seconds", "price"],
        "public_power": ["unix_seconds", "production_types"],
        "installed_power": ["time", "production_types"]
    }
    
    if not data:
        print("Data validation failed: No data returned from API.")
        return False

    for key in required_keys[data_type]:
        if key not in data:
            print(f"Data validation failed: Missing expected key '{key}'.")
            return False

    print("Data validation passed.")
    return True