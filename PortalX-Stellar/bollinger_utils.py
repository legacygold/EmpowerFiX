# bollinger_utils.py
from tradingview_ta import TA_Handler
import tradingview_ta
from tradingview_ta_utils import create_ta_handler_instance, get_ta_handler_analysis
from datetime import datetime, timedelta
import pytz
import time
import math
from stellar_sdk import Asset
from requests.exceptions import Timeout
from logging_config import app_logger, info_logger, error_logger
from statistics import mean, stdev
from cycle_set_init import definitions
from fetch_stellar_dex_data import get_current_price
from stellar_db import fetch_historical_candles_from_db

print("Tradingview_TA version:", tradingview_ta.__version__)
# Example output: 3.1.3

def calculate_xlm_bollinger_bands(handler):
    # Instantiate TA-Handler
    handler = create_ta_handler_instance()

    # Get handler analysis
    analysis = get_ta_handler_analysis(handler)
    
    return analysis.indicators["BB.upper"], analysis.indicators["BB.lower"]

def calculate_bollinger_bands(closing_prices, window_size, num_std_dev=2):
    # Calculate moving average for the last 'window_size' closing prices
    moving_average = mean(closing_prices[-window_size:])
    info_logger.info("Moving Average: %s", moving_average)

    # Calculate standard deviation for the last 'window_size' closing prices
    std_dev = stdev(closing_prices[-window_size:])
    info_logger.info("Standard Deviation: %s", std_dev)

    upper_bb = moving_average + (num_std_dev * std_dev)
    lower_bb = moving_average - (num_std_dev * std_dev)
    
    info_logger.info("Upper Bollinger Band: %s", upper_bb)
    info_logger.info("Lower Bollinger Band: %s", lower_bb)
    
    return upper_bb, lower_bb

# Function to determine 24 hour mean
def determine_xlm_mean24():
    # Instantiate TA-Handler
    handler = TA_Handler(
        symbol='XLMUSD',
        exchange='COINBASE',
        screener='crypto',
        interval='1d',
        timeout=None
    )

    # Get handler analysis
    analysis = handler.get_analysis()
    
    high_24hr = analysis.indicators["high"]
    low_24hr = analysis.indicators["low"]

    mean24 = (high_24hr + low_24hr) / 2

    info_logger.info("24 hr High: %s, 24 hr Low: %s", high_24hr, low_24hr)
    info_logger.info("24 hr mean: %s", mean24)

    return mean24

# Function to determine 24 hour mean
def determine_mean24(conn, user_config):
    current_time = int(time.time())
    twenty_four_hours_ago = current_time - (24 * 60 * 60)
    high_24hr = float('-inf')
    low_24hr = float('inf')

    # Calculate the last 30 days date range
    end_date_str = datetime.now(pytz.utc).strftime("%Y-%m-%d")
    start_date_str = (datetime.now(pytz.utc) - timedelta(days=30)).strftime("%Y-%m-%d")

    historical_data24 = fetch_historical_candles_from_db(conn, user_config, 86400, start_date_str=start_date_str, end_date_str=end_date_str, last_processed_timestamp=None)

    for entry in historical_data24:
        entry_time = entry[0]
        if twenty_four_hours_ago <= entry_time <= current_time:
            try:
                high = float(entry[5])
                low = float(entry[6])

                if high > high_24hr:
                    high_24hr = high
                if low < low_24hr:
                    low_24hr = low
            except ValueError:
                error_logger.error("Error: unable to determine 24 hr high and low")
                pass

    mean24 = (high_24hr + low_24hr) / 2

    info_logger.info("24 hr High: %s, 24 hr Low: %s", high_24hr, low_24hr)
    info_logger.info("24 hr mean: %s", mean24)

    return mean24

def get_best_bid_ask_prices(base_asset_code, counter_asset_code, base_asset_issuer, counter_asset_issuer, max_retries=3):
    server = definitions["server"]
    retries = 0
    while retries < max_retries:
        try:
            # Handling base and counter assets
            if base_asset_code.upper() == "XLM" and base_asset_issuer == 'native':
                base_asset = Asset.native()
            else:
                base_asset = Asset(base_asset_code, base_asset_issuer)

            if counter_asset_code.upper() == "XLM" and counter_asset_issuer == 'native':
                counter_asset = Asset.native()
            else:
                counter_asset = Asset(counter_asset_code, counter_asset_issuer)

            orderbook = server.orderbook(selling=counter_asset, buying=base_asset).call()
            bids = orderbook.get('bids', [])
            asks = orderbook.get('asks', [])

            if bids and asks:
                best_bid = float(bids[0]['price'])
                best_ask = float(asks[0]['price'])
                print(f"Best bid: {best_bid}, Best ask: {best_ask}")
                return best_bid, best_ask

            print("No bids or asks found in the orderbook. Retrying...")
            retries += 1
            time.sleep(1)  # Wait a bit before retrying

        except Exception as e:
            print(f"An error occurred while fetching the orderbook: {e}. Retrying...")
            retries += 1
            time.sleep(1)  # Wait a bit before retrying

    print("Maximum retries reached. Unable to fetch best bid and ask prices.")
    return None, None

def get_best_bid_ask_prices_with_retry(base_asset_code, counter_asset_code, base_asset_issuer, counter_asset_issuer, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            best_bid, best_ask = get_best_bid_ask_prices(base_asset_code, counter_asset_code, base_asset_issuer, counter_asset_issuer)
            if best_bid is not None and best_ask is not None:
                return best_bid, best_ask
            else:
                print("No bids or asks found in the response. Retrying...")
                retries += 1
                time.sleep(1)  # Adding a delay between retries
        except Exception as e:
            print(f"An error occurred while trying to fetch best bid and ask prices: {e}. Retrying...")
            retries += 1
            time.sleep(1)  # Adding a delay between retries
    print("Maximum retries reached. Unable to fetch best bid and ask prices.")
    return None, None
    
def determine_starting_sell_parameters(current_price, upper_bb, starting_size_B, mean24):
    while True:
        starting_price_sell = calculate_starting_sell_price_with_retry(current_price, upper_bb, starting_size_B, mean24, max_iterations=10)
        
        if starting_price_sell is not None:
            return starting_price_sell
        
        # Wait for a short duration before checking again
        time.sleep(5)  # Adjust the duration as needed

def determine_starting_buy_parameters(current_price, lower_bb, starting_size_Q, mean24):
    while True:
        starting_price_buy = calculate_starting_buy_price_with_retry(current_price, lower_bb, starting_size_Q, mean24, max_iterations=10)
        if starting_price_buy is not None:
            return starting_price_buy
        
        # Wait for a short duration before checking again
        time.sleep(5)  # Adjust the duration as needed

def calculate_starting_sell_price(current_price, upper_bb, starting_size_B, mean24, max_iterations=10):
    quote_increment = float(0.0000001)
    
    # Initialize the starting sell price
    starting_price_sell = None

    iterations = 0
    while iterations < max_iterations:
        current_price = get_current_price(definitions["server"], definitions["base_asset"], definitions["counter_asset"])  # Replace with your function to get the current price
        mean24 = determine_mean24(definitions["conn"], user_config=definitions["user_config"])  # Replace with your function to calculate mean24
        upper_bb, lower_bb = calculate_bollinger_bands(definitions["closing_prices"], definitions["window_size"], num_std_dev=2)
        
        # Check if criteria is met for determining starting sell price
        if current_price > mean24 and starting_size_B > 0:
            # Calculate the rounded price based on quote_increment
            rounded_price = round((upper_bb * 0.9995), -int(math.floor(math.log10(quote_increment))))

            # Ensure the rounded price is at least quote_increment
            if rounded_price < quote_increment:
                rounded_price = quote_increment

            # Retrieve best bid and ask prices
            best_bid, best_ask = get_best_bid_ask_prices_with_retry(definitions["base_asset_code"], definitions["counter_asset_code"], definitions["base_asset_issuer"], definitions["counter_asset_issuer"], max_retries=3)

            # Compare with the calculated starting sell price
            if best_bid and rounded_price > best_bid:
                starting_price_sell = rounded_price  # Place a sell order slightly below upper_bb
                info_logger.info("Current price: %s", current_price)
                app_logger.info("Starting price calculated for sell order: %s", starting_price_sell)
                return starting_price_sell  # Exit the loop if conditions are met

            print("Starting sell order price not favorable based on best bid. Continuing to wait...")
        else:    
            print("Criteria for market conditions not met for placing sell order. Continuing to wait...")

        iterations += 1
        time.sleep(90)  # Adjust the sleep time as needed

    print("Maximum iterations reached. Conditions for determining starting sell price not met. Resetting retries.")
    starting_size_Q = definitions["starting_size_Q"]
    from prices_utils import determine_starting_prices
    return determine_starting_prices(current_price, upper_bb, lower_bb, starting_size_B, starting_size_Q, mean24, quote_increment)

def calculate_starting_sell_price_with_retry(current_price, upper_bb, starting_size_B, mean24, max_iterations=10):
    iterations = 0

    while iterations < max_iterations:
        try:
            return calculate_starting_sell_price(current_price, upper_bb, starting_size_B, mean24, max_iterations=10)
        except Timeout:
            print("Timeout occurred. Retrying...")

        iterations += 1
        time.sleep(90)  # Adjust the sleep time as needed

    print("Maximum iterations reached. Conditions for determining starting sell price not met. Resetting retries.")
    return None

def calculate_starting_buy_price(current_price, lower_bb, starting_size_Q, mean24, max_iterations=10):
    quote_increment = float(0.0000001)
    
    # Initialize the starting buy price
    starting_price_buy = None

    iterations = 0
    while iterations < max_iterations:
        current_price = get_current_price(definitions["server"], definitions["base_asset"], definitions["counter_asset"])  # Replace with your function to get the current price
        mean24 = determine_mean24(definitions["conn"], user_config=definitions["user_config"])  # Replace with your function to calculate mean24
        upper_bb, lower_bb = calculate_bollinger_bands(definitions["closing_prices"], definitions["window_size"], num_std_dev=2)

        # Check if criteria is met for determining starting buy price
        if current_price < mean24 and starting_size_Q > 0:
            # Calculate the rounded price based on quote_increment
            rounded_price = round((lower_bb * 1.0005), -int(math.floor(math.log10(quote_increment))))

            # Ensure the rounded price is at least quote_increment
            if rounded_price < quote_increment:
                rounded_price = quote_increment

            # Retrieve best bid and ask prices
            best_bid, best_ask = get_best_bid_ask_prices_with_retry(definitions["base_asset_code"], definitions["counter_asset_code"], definitions["base_asset_issuer"], definitions["counter_asset_issuer"], max_retries=3)

            # Compare with the calculated starting sell price
            if best_ask and rounded_price < best_ask:
                starting_price_buy = rounded_price  # Place a buy order slightly above lower_bb
                info_logger.info("Current price: %s", current_price)
                app_logger.info("Starting price calculated for buy order: %s", starting_price_buy)
                return starting_price_buy  # Exit the loop if conditions are met

            print("Starting buy order price not favorable based on best ask. Continuing to wait...")
        else:    
            print("Criteria for market conditions not met for placing buy order. Continuing to wait...")

        iterations += 1
        time.sleep(90)  # Adjust the sleep time as needed

    print("Maximum iterations reached. Conditions for determining starting buy price not met. Resetting retries.")
    starting_size_B = definitions["starting_size_B"]
    from prices_utils import determine_starting_prices
    return determine_starting_prices(current_price, upper_bb, lower_bb, starting_size_B, starting_size_Q, mean24, quote_increment)

def calculate_starting_buy_price_with_retry(current_price, lower_bb, starting_size_Q, mean24, max_iterations=10):
    iterations = 0

    while iterations < max_iterations:
        try:
            return calculate_starting_buy_price(current_price, lower_bb, starting_size_Q, mean24, max_iterations=10)
        except Timeout:
            print("Timeout occurred. Retrying...")

        iterations += 1
        time.sleep(5)  # Adjust the sleep time as needed

    print("Maximum iterations reached. Conditions for determining starting sell price not met. Resetting retries.")

# Indicate that bollinger_utils.py module loaded successfully
info_logger.info("bollinger_utils module loaded successfully")
