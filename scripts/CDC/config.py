# config.py
class Config:
    def __init__(self):
        self.staging_path = "/workspaces/baywa-data-pipeline/Data/Staging_Schema"
        self.processed_path = "/workspaces/baywa-data-pipeline/Data/CDC_Schema"
        self.final_path = "/workspaces/baywa-data-pipeline/Data/Information_Schema"