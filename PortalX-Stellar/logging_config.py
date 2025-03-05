# logging_config.py

import logging
import os

# Define the directory path
LOG_DIR = os.path.join(os.path.dirname(__file__), 'PortalX Console Log')

# Configure logging
logging.basicConfig(filename='stellar_endpoint_data.log', level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

def log_to_file(message):
    logging.info(message)

# Create a logger for errors
error_logger = logging.getLogger('errors')
error_logger.setLevel(logging.ERROR)

# Create a file handler for error messages
error_handler = logging.FileHandler('error.log')
error_handler.setLevel(logging.ERROR)
error_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
error_handler.setFormatter(error_formatter)
error_logger.addHandler(error_handler)


# Create a logger for general info
info_logger = logging.getLogger('info')
info_logger.setLevel(logging.INFO)

# Create a file handler for info messages
info_handler = logging.FileHandler('info.log')
info_handler.setLevel(logging.INFO)
info_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
info_handler.setFormatter(info_formatter)
info_logger.addHandler(info_handler)


# Create a logger for statements that will be both logged and printed
app_logger = logging.getLogger('app')
app_logger.setLevel(logging.INFO)

# Create a file handler for "app.log"
app_handler = logging.FileHandler('app.log')

# Create a formatter for the messages in "app.log"
app_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
app_handler.setFormatter(app_formatter)
app_logger.addHandler(app_handler)

# Create a StreamHandler to print log messages to the console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

# Add both handlers to the "app" logger
app_logger.addHandler(app_handler)
app_logger.addHandler(console_handler)

def setup_cycleset_logger(cycle_set_counter, cycle_type):
    # Define a short label for the cycle type
    cycle_type_label = "sell_buy" if cycle_type == "sell_buy" else "buy_sell"

    # Create a logger
    logger = logging.getLogger(f"cycleset_{cycle_set_counter}_{cycle_type_label}")
    logger.setLevel(logging.INFO)

    # Create a file handler
    file_handler = logging.FileHandler(os.path.join(LOG_DIR, f"cycleset_{cycle_set_counter}_{cycle_type_label}_log.txt"))
    file_handler.setLevel(logging.INFO)

    # Create a formatter and set it for the handler
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(file_handler)

    return logger