# config.py
class Config:
    def __init__(self):
        self.api_urls = {
            "public_power": "https://api.energy-charts.info/public_power",
            "price": "https://api.energy-charts.info/price",
            "installed_power": "https://api.energy-charts.info/installed_power"
        }
        self.start_date = "2023-01-01"
        self.end_date = "2023-12-31"
        self.frequency = "daily"
        self.staging_path = "/workspaces/baywa-data-pipeline/data"