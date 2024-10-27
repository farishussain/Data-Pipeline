class Config:
    def __init__(self):
        self.api_urls = {
            "power_data": "https://example.com/api/power",
            "price_data": "https://example.com/api/price",
            "installed_capacity": "https://example.com/api/installed_capacity"
        }
        self.start_date = "2023-01-01"
        self.end_date = "2023-12-31"
        self.frequency = "daily"