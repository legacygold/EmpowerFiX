#config.py

import json
from logging_config import error_logger


def load_stellar_config(file_path):
    try:
        with open(file_path, 'r') as file:
            stellar_config = json.load(file)
            return stellar_config  
    except FileNotFoundError:
        error_logger.error("Config file not found at specified path: %s", file_path)
        return None
    except json.JSONDecodeError:
        error_logger.error("Error decoding JSON in config file: %s", file_path)
        return None
    except Exception as e:  # Broad exception handling for simplicity
        print(f"Failed to load Stellar configuration: {e}")

    
def get_secret_key_file_path():
    drive = input("Enter the drive letter (e.g., 'D:'): ")
    folder = input("Enter the folder name: ")
    file_name = input("Enter the file name with extension (e.g., 'api_keys.json'): ")
    return f"{drive}\\{folder}\\{file_name}"

# Load API credentials based on exchange
exchange = "stellar"  # Example: "coinbase", "stellar", etc.
config_file_path = f"D:\\APIs\\{exchange}_api.json"
stellar_config = load_stellar_config(config_file_path)

# Load the configuration settings from the JSON file
print("Loading secret key credentials...")

# Check secret key sent successfully
if stellar_config:
    secret_key = stellar_config['secret_key']
    # Now you can use 'secret_key' as needed for transactions
else:
    print("Error loading Stellar configuration.")