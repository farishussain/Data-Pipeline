# config.py
class Config:
    def __init__(self):
        self.api_urls = {
            "public_power": "https://api.energy-charts.info/public_power",
            "price": "https://api.energy-charts.info/price",
            "installed_power": "https://api.energy-charts.info/installed_power"
        }
        self.country = "de"
        self.start_date = "2023-12-31"
        self.end_date = "2023-12-31"
        self.frequency = "yearly" # Set either "daily", "monthly" or "yearly", default value depending on API
        self.staging_path = "/workspaces/baywa-data-pipeline/Data/Staging_Schema"
        self.processed_path = "/workspaces/baywa-data-pipeline/Data/CDC_Schema"
        self.final_path = "/workspaces/baywa-data-pipeline/Data/Information_Schema"
        self.decommission = "false" #boolean true or false, Default Null