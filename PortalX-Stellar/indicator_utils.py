# indicator_utils.py

import tradingview_ta
from tradingview_ta import TA_Handler
from datetime import datetime, timedelta
import pytz
import time
import pandas as pd
from logging_config import app_logger, info_logger, error_logger
from cycle_set_init import definitions
from fetch_stellar_dex_data import fetch_historical_candles, fetch_closing_prices, get_current_price
from stellar_db import fetch_historical_candles_from_db
from statistics import mean, stdev



print("Tradingview_TA version:", tradingview_ta.__version__)
# Example output: 3.1.3

def get_indicators():
    
    # Define necessary parameters
    server = definitions["server"]
    conn = definitions["conn"]
    user_config = definitions["user_config"]
    base_asset = definitions["base_asset"]
    counter_asset = definitions["counter_asset"]
    chart_interval = definitions["chart_interval"]

    # Adjusted to directly use chart_interval and added conn parameter

    # Calculate the last 30 days date range
    end_date_str = datetime.now(pytz.utc).strftime("%Y-%m-%d")
    start_date_str = (datetime.now(pytz.utc) - timedelta(days=30)).strftime("%Y-%m-%d")

    # Note: Removed the initial calls that were outside the lambdas to avoid premature execution

    return {
        "candles": lambda: fetch_historical_candles_from_db(conn, user_config, chart_interval, start_date_str, end_date_str),
        "closing_prices": lambda: fetch_closing_prices(fetch_historical_candles_from_db(conn, user_config, chart_interval, start_date_str, end_date_str)),
        "current_price": lambda: get_current_price(server, base_asset, counter_asset),
        "upper_bb": lambda closing_prices=fetch_closing_prices(fetch_historical_candles_from_db(conn, user_config, chart_interval, start_date_str, end_date_str)): calculate_bollinger_bands(closing_prices, user_config["window_size"], num_std_dev=2)[0],  # Assuming calculate_bollinger_bands returns a tuple (upper_bb, lower_bb)
        "lower_bb": lambda closing_prices=fetch_closing_prices(fetch_historical_candles_from_db(conn, user_config, chart_interval, start_date_str, end_date_str)): calculate_bollinger_bands(closing_prices, user_config["window_size"], num_std_dev=2)[1],
        "mean24": lambda: determine_mean24(conn, user_config),
        "current_rsi": lambda: calculate_rsi(base_asset, counter_asset, chart_interval),
        "long_term_ma24": lambda: calculate_long_term_ma24(conn, user_config)
    }

def calculate_xlm_bollinger_bands(handler):
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

def log_bollinger_bands(closing_prices, window_size, num_std_dev=2):
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
    start_date_str = (datetime.now(pytz.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

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


def log_mean24(conn, user_config):
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

def calculate_long_term_ma24(conn, user_config): 

    # Calculate the last 24 hours date range
    end_date_str = datetime.now(pytz.utc).strftime("%Y-%m-%d")
    start_date_str = (datetime.now(pytz.utc) - timedelta(hours=24)).strftime("%Y-%m-%d")
   
    # Fetch historical data for the specified product with a 24-hour interval
    historical_data24 = fetch_historical_candles_from_db(conn, user_config, 86400, start_date_str=start_date_str, end_date_str=end_date_str, last_processed_timestamp=None)

    candles = fetch_historical_candles(historical_data24, definitions["chart_interval"])

    # Extract the close prices from the 24-hour historical data
    closing_prices24 = []
    
    for candle in candles:
        try:
            close_price = float(candle[4])
            closing_prices24.append(close_price)
        except ValueError:
            # Handle invalid data (e.g., non-numeric values) here, if needed
            pass

    if len(closing_prices24) < 24:
        error_logger.error("Not enough data to return 24 hour closing prices")
        time.sleep(300)  # Wait for 5 minutes before retrying

    # Calculate the 24-hour moving average
    long_term_ma24 = sum(closing_prices24) / 24

    return long_term_ma24

def calculate_rsi(base_asset, counter_asset, chart_interval):
    while True:
        # Print statement to indicate RSI calculation
        print("Fetching RSI data...")

        # Define necessary data points
        length = 1209000/chart_interval

        # Extract the close prices from the historical data
        closing_prices = definitions["closing_prices"]

        # Define your variables
        interval_in_seconds = chart_interval
        number_of_days = 15
        data_points_per_day = 24 * 60 * 60 // interval_in_seconds  # Calculate how many data points are in one day

        # Calculate the total number of data points to fetch for the desired number of days
        total_data_points = data_points_per_day * number_of_days

        # Use list slicing to get the last 'total_data_points' elements from the list
        selected_closing_prices = closing_prices[-total_data_points:]

        # 'selected_closing_prices' now contains the closing prices for the last 15 days

        # Ensure we have enough data points for the calculation
        app_logger.info(len(selected_closing_prices))
        if len(closing_prices) >= length:
            break  # Exit the loop if we have enough data

        # If we don't have enough data, wait for some time before retrying
        print("Insufficient data points for RSI calculation. Retrying in 5 minutes...")
        error_logger.error(f"{len(selected_closing_prices)} data points were insuffient for RSI calcualtion.")
        time.sleep(300)  # Wait for 1 minutes before retrying

    # Fill missing or zero values in closing prices with a default value (e.g., 0.0)
    closing_prices = pd.Series(selected_closing_prices).fillna(0.0).tolist()

    # Calculate price changes
    delta = pd.Series(closing_prices).diff(1)
    
    # Separate gains and losses
    gains = delta.where(delta > 0, 0)
    losses = -delta.where(delta < 0, 0)
    
    # Filter out zero values from gains and losses
    gains = gains[gains != 0]
    losses = losses[losses != 0]

    # Calculate average gains and losses
    avg_gain = gains.sum() / len(gains)
    avg_loss = losses.sum() / len(losses)

    # Calculate relative strength (RS)
    rs = avg_gain / avg_loss

    # Calculate RSI
    rsi = 100 - (100 / (1 + rs))

    app_logger.info(f"Calculated current RSI for {base_asset}-{counter_asset}, interval {chart_interval}, length {length}: {rsi}")

    return rsi

def new_calculate_rsi(closing_prices, period=14):
    """
    Calculate the Relative Strength Index (RSI) for a given list of closing prices.
    
    Parameters:
    - closing_prices: List of closing prices for a given product.
    - period: Number of periods to use for RSI calculation, default is 14.
    
    Returns:
    - RSI value.
    """
    if len(closing_prices) < period:
        print("Insufficient data points for RSI calculation.")
        return None

    # Convert the closing prices to a Pandas Series
    prices = pd.Series(closing_prices)

    # Calculate price changes
    delta = prices.diff()

    # Separate gains and losses
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    # Calculate the average gain and average loss
    avg_gain = gain.rolling(window=period, min_periods=1).mean()
    avg_loss = loss.rolling(window=period, min_periods=1).mean()

    # Calculate the Relative Strength (RS)
    rs = avg_gain / avg_loss

    # Calculate the Relative Strength Index (RSI)
    rsi = 100 - (100 / (1 + rs))

    return rsi.iloc[-1]  # Return the last RSI value

def calculate_rsi_chatgpt(candles, period=14):
    # Convert the list of dictionaries into a DataFrame
    candles_df = pd.DataFrame(candles)
    
    # Ensure 'closing_price' is numeric
    candles_df['closing_price'] = pd.to_numeric(candles_df['closing_price'], errors='coerce')
    
    # Calculate price changes
    delta = candles_df['closing_price'].diff()
    
    # Separate gains and losses
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    
    # Calculate the exponential moving average mean of gains and losses
    avg_gain = gain.ewm(span=period, adjust=False).mean()
    avg_loss = loss.ewm(span=period, adjust=False).mean()
  
    # Compute the Relative Strength (RS)
    rs = avg_gain / avg_loss
    
    # Calculate the Relative Strength Index (RSI)
    rsi = 100 - (100 / (1 + rs))
    
    return rsi

