'''
This script reads configuration values from a YAML file using the PyYAML library.'''

import os
import yaml

class ConfigReader:
    """Reads configuration values from a YAML file."""

    def __init__(self, config_file):
        """Default Configuration Values"""
        self.NO_OF_DAYS = 5
        self.DAVIATION = 0.05 
        self.OUTLIER_FACTOR = 2 
        self.OUTLIER_PERCENTAGE = 0.5 
        self.MISSING_PERCENTAGE = 0.2
        self.VOLATILITY = 5000
        self.OUTDATED_PERCENTAGE = 0.8
        self.WINDOW_SIZE = 50000  

        """Initialize ConfigReader by loading the YAML configuration file."""
        if not config_file:
            print(f"❌ No Config file provided! Using default values.")
            return
        elif not os.path.exists(config_file):
            print(f"❌ Config file '{config_file}' not found! Using default values.")
            return

        print(f"📖 Loading configuration from: {config_file}")
        with open(config_file, "r", encoding="utf-8") as file:
            config_data = yaml.safe_load(file)
            self.NO_OF_DAYS = config_data.get("NO_OF_DAYS", self.NO_OF_DAYS)
            self.DAVIATION = config_data.get("DAVIATION", self.DAVIATION)
            self.OUTLIER_FACTOR = config_data.get("OUTLIER_FACTOR", self.OUTLIER_FACTOR)
            self.OUTLIER_PERCENTAGE = config_data.get("OUTLIER_PERCENTAGE", self.OUTLIER_PERCENTAGE)
            self.MISSING_PERCENTAGE = config_data.get("MISSING_PERCENTAGE", self.MISSING_PERCENTAGE)
            self.VOLATILITY = config_data.get("VOLATILITY", self.VOLATILITY)
            self.OUTDATED_PERCENTAGE = config_data.get("OUTDATED_PERCENTAGE", self.OUTDATED_PERCENTAGE)
            self.WINDOW_SIZE = config_data.get("WINDOW_SIZE", self.WINDOW_SIZE)
 



# Example Usage:
if __name__ == "__main__":
    file_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    config = ConfigReader(file_path)

    print(config.NO_OF_DAYS)
