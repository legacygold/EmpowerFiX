# fetch_stellar_dex_data.py

import time
import pandas as pd
from datetime import datetime, timedelta
import pytz
from logging_config import app_logger, info_logger, error_logger

def fetch_historical_data(server, base_asset, counter_asset, start_time, end_time, max_records):
    historical_data = []
    records_fetched = 0

    # Ensure start_time and end_time are timezone-aware
    if start_time.tzinfo is None or start_time.tzinfo.utcoffset(start_time) is None:
        start_time = pytz.UTC.localize(start_time)
    if end_time.tzinfo is None or end_time.tzinfo.utcoffset(end_time) is None:
        end_time = pytz.UTC.localize(end_time)

    try:
        trades_call = server.trades().for_asset_pair(base_asset, counter_asset).order(desc=True).limit(200)

        while records_fetched < max_records:
            trades_response = trades_call.call()
            trades = trades_response['_embedded']['records']

            if not trades:
                break  # Exit if no more trades to process

            for trade in trades:
                trade_time = datetime.strptime(trade['ledger_close_time'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=pytz.UTC)
                
                # Ensure all required fields are present
                if all(field in trade for field in ['base_amount', 'price', 'counter_amount', 'base_asset_type', 'counter_asset_type']):
                    # Only process and count trades within the specified time window
                    if start_time <= trade_time <= end_time:
                        historical_data.append(trade)
                        records_fetched += 1
                        
                    elif trade_time < start_time:
                        # Finished collecting relevant trades.
                        return historical_data

            # Prepare for the next page of results.
            if trades:
                last_trade = trades[-1]
                trades_call.cursor(last_trade['paging_token'])

    except Exception as e:
        error_logger.error(f"Failed to fetch historical data: {str(e)}")
        return None  # Consider handling error more gracefully or retrying

    return historical_data

# Example: Fetching historical data using last_paging_token

# Assuming you have added last_paging_token to your database schema

def fetch_historical_data_with_paging_token(server, base_asset, counter_asset, last_paging_token, max_records):
    historical_data = []
    records_fetched = 0

    trades_call = server.trades().for_asset_pair(base_asset, counter_asset).limit(200)
    if last_paging_token:
        trades_call.cursor(last_paging_token)

    while records_fetched < max_records:
        trades_response = trades_call.call()
        trades = trades_response['_embedded']['records']
        
        if not trades:
            break  # No more trades to process

        for trade in trades:
            historical_data.append(trade)
            records_fetched += 1
            if records_fetched >= max_records:
                break

        last_trade = trades[-1]
        trades_call.cursor(last_trade['paging_token'])

    # Update the last_paging_token in the database with the paging_token of the last trade fetched
    return historical_data, last_trade['paging_token']

def fetch_historical_candles(historical_data, chart_interval):
    try:
        candles = []

        # Debug: Print the total number of trades and timestamp range
        info_logger.info(f"Total trades: {len(historical_data)}")
        if historical_data:
            info_logger.info(f"Latest trade time: {historical_data[-1]['timestamp']}")
            info_logger.info(f"Earliest trade time: {historical_data[0]['timestamp']}")

            # Assume historical_data is sorted in ascending order by time
            start_time = historical_data[0]['timestamp'] if isinstance(historical_data[0]['timestamp'], datetime) else datetime.strptime(historical_data[0]['timestamp'], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.UTC)
            end_time = historical_data[-1]['timestamp'] if isinstance(historical_data[-1]['timestamp'], datetime) else datetime.strptime(historical_data[-1]['timestamp'], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.UTC)
            total_duration = (end_time - start_time).total_seconds()
            num_intervals = int(total_duration / chart_interval)
            last_known_price = None  # Initialize outside the intervals loop
            last_paging_token = None  # Initialize the last paging token

            for i in range(num_intervals):
                interval_start = start_time + timedelta(seconds=i * chart_interval)
                interval_end = interval_start + timedelta(seconds=chart_interval)

                interval_trades = [
                    trade for trade in historical_data 
                    if interval_start <= (
                        trade['timestamp'] if isinstance(trade['timestamp'], datetime) 
                        else datetime.strptime(trade['timestamp'], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.UTC)
                    ) < interval_end
                ]

                if interval_trades:
                    prices = [trade['price'] for trade in interval_trades]
                    opening_price = prices[0]
                    closing_price = prices[-1]
                    highest_price = max(prices)
                    lowest_price = min(prices)
                    last_known_price = closing_price  # Update last known price
                    last_paging_token = interval_trades[-1].get('paging_token')  # Update with the closing trade's paging token
                elif last_known_price is not None:
                    # For intervals without trades, use the last known price
                    opening_price = closing_price = highest_price = lowest_price = last_known_price
                else:
                    # If there's no last known price yet, skip this interval
                    continue

                candle = {
                    'interval_start': interval_start,
                    'interval_end': interval_end,
                    'opening_price': opening_price,
                    'closing_price': closing_price,
                    'highest_price': highest_price,
                    'lowest_price': lowest_price,
                    'last_paging_token': last_paging_token  # Include the last paging token in the candle data
                }
                candles.append(candle)

            info_logger.info(f"Current size of 'interval_trades': {len(interval_trades)}")

        info_logger.info("Latest candle: %s", candle)

        return candles
    except Exception as e:
        error_logger.error(f"Failed to fetch historical candles data: {str(e)}")
        return []  # Return an empty list in case of error

def fetch_closing_prices(candles):
    """
    Extracts closing prices from a list of candlestick data previously fetched.

    Parameters:
    - candles: A list of dictionaries, each representing a candlestick with 'interval_start', 'interval_end', 'opening_price', 'closing_price', 'highest_price', 'lowest_price'.

    Returns:
    - A list of closing prices extracted from the candlestick data.
    """
    try:
        # Extract closing prices from the candlestick data
        closing_prices = [candle['closing_price'] for candle in candles]

        return closing_prices

    except Exception as e:
        error_logger.error(f"Failed to fetch closing prices data: {str(e)}")
        return None
   
def get_current_price(server, base_asset, counter_asset):
    try:
        trades = server.trades()\
            .for_asset_pair(base_asset, counter_asset)\
            .order("desc")\
            .limit(1)\
            .call()
        latest_trade = trades['_embedded']['records'][0]
        current_price = float(latest_trade['price']['n']) / float(latest_trade['price']['d'])
        app_logger.info(f"Current price for {base_asset.code}-{counter_asset.code} is: {current_price}")

        return current_price
    
    except Exception as e:
        error_logger.error(f"Failed to fetch current price: {str(e)}")
        return None



def calculate_old_rsi(server, base_asset, counter_asset, chart_interval):
    while True:
        # Print statement to indicate RSI calculation
        print("Fetching RSI data...")

        # Define necessary data points
        length = 360 * chart_interval
        total_data_points = 400 * chart_interval  # Slightly increased to ensure enough data

        # Initialize the end_time to the current time
        end_time = datetime.now(pytz.UTC)

        # Convert chart_interval and total_data_points to seconds and create a timedelta
        time_delta = timedelta(seconds=chart_interval * total_data_points)

        # Subtract timedelta from end_time to get start_time
        start_time = end_time - time_delta

        # Calculate the number of intervals based on the data range and chart_interval
        num_intervals = int((end_time - start_time).total_seconds() / chart_interval)

        # Make an API request and get historical data
        historical_data = fetch_historical_data(server, base_asset, counter_asset, start_time, end_time, max_records=1000000)
        candles = fetch_historical_candles(historical_data, chart_interval)

        # Initialize an empty list to collect closing prices
        closing_prices = []

        # Extract closing prices and append to the list
        closing_prices.extend([candle['closing_price'] for candle in candles])

        # Ensure we have enough data points for the calculation
        app_logger.info(len(closing_prices))
        if len(closing_prices) >= length:
            break  # Exit the loop if we have enough data

        # If we don't have enough data, wait for some time before retrying
        print("Insufficient data points for RSI calculation. Retrying in 5 minutes...")
        error_logger.error(f"{len(closing_prices)} data points were insuffient for RSI calcualtion.")
        time.sleep(300)  # Wait for 1 minutes before retrying

    # Fill missing or zero values in closing prices with a default value (e.g., 0.0)
    closing_prices = pd.Series(closing_prices).fillna(0.0).tolist()

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



