# prices_utils.py

import time
import math
from requests.exceptions import Timeout
from stellar_sdk import Asset
from logging_config import app_logger, info_logger, error_logger
from cycle_set_init import definitions
from indicator_utils import get_indicators


def determine_starting_prices(starting_size_B, starting_size_Q):
    starting_price_sell = None
    starting_price_buy = None
    quote_increment = float(0.0000001)

    # Define dynamic indicator parameters
    indicators = get_indicators()
    current_price = indicators["current_price"]
    mean24 = indicators["mean24"]

    if starting_size_B > 0:
        if current_price > mean24 and starting_size_Q >=0:
            # Market is favorable for sell order
            print("Market is favorable for sell order")
            starting_price_sell = determine_starting_sell_parameters(starting_size_B)

            # We still calculate a buy price if quote assets are available
            if starting_price_sell is not None and starting_size_Q > 0:
                rounded_buy_estimate = round((starting_price_sell * 0.995), -int(math.floor(math.log10(quote_increment))))

                # Ensure the rounded price is at least quote_increment
                if rounded_buy_estimate < quote_increment:
                    rounded_buy_estimate = quote_increment
            
                starting_price_buy = rounded_buy_estimate
                info_logger.info("Starting price determined for buy order: %s", starting_price_buy)

        if current_price < mean24:
            if starting_size_Q == 0:
                # Market is not favorable for sell order
                print("Market is not favorable for a sell order now")
                print("Waiting for favorable market conditions for sell order...")
                starting_price_sell = determine_starting_sell_parameters(starting_size_B)

            # If quote assets are available market is favorable for buy order
            if starting_size_Q > 0:
                print("Market is favorable for buy order")
                starting_price_buy = determine_starting_buy_parameters(starting_size_Q)

                # Will still calculate a sell price because base assets available
                if starting_price_buy is not None:
                    rounded_sell_estimate = round((starting_price_buy * 1.005), -int(math.floor(math.log10(quote_increment))))

                    # Ensure the rounded price is at least quote_increment
                    if rounded_sell_estimate < quote_increment:
                        rounded_sell_estimate = quote_increment

                    starting_price_sell = rounded_sell_estimate
                    info_logger.info("Starting price determined for sell order: %s", starting_price_sell)

    elif starting_size_B == 0:
        if current_price > mean24 and starting_size_Q > 0:
            # Market is not favorable for buy order 
            print("Market is not favorable for a buy order now")
            print("Waiting for favorable market conditions for buy order...")
            starting_price_buy = determine_starting_buy_parameters(starting_size_Q)

        if current_price < mean24 and starting_size_Q > 0:
            # Market is favorable for buy order
            print("Market is favorable for buy order")
            starting_price_buy = determine_starting_buy_parameters(starting_size_Q)

    return starting_price_sell, starting_price_buy
    
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

            app_logger.info("No bids or asks found in the orderbook. Retrying...")
            retries += 1
            time.sleep(1)  # Wait a bit before retrying

        except Exception as e:
            error_logger.error(f"An error occurred while fetching the orderbook: {e}. Retrying...")
            retries += 1
            time.sleep(1)  # Wait a bit before retrying

    app_logger.info("Maximum retries reached. Unable to fetch best bid and ask prices.")
    return None, None

def get_best_bid_ask_prices_with_retry(base_asset_code, counter_asset_code, base_asset_issuer, counter_asset_issuer, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            best_bid, best_ask = get_best_bid_ask_prices(base_asset_code, counter_asset_code, base_asset_issuer, counter_asset_issuer)
            if best_bid is not None and best_ask is not None:
                return best_bid, best_ask
            else:
                app_logger.info("No bids or asks found in the response. Retrying...")
                retries += 1
                time.sleep(1)  # Adding a delay between retries
        except Exception as e:
            error_logger.error(f"An error occurred while trying to fetch best bid and ask prices: {e}. Retrying...")
            retries += 1
            time.sleep(1)  # Adding a delay between retries

    app_logger.info("Maximum retries reached. Unable to fetch best bid and ask prices.")
    return None, None
    
def determine_starting_sell_parameters(starting_size_B):
    while True:
        starting_price_sell = calculate_starting_sell_price_with_retry(starting_size_B, max_iterations=10)
        
        if starting_price_sell is not None:
            return starting_price_sell
        
        # Wait for a short duration before checking again
        time.sleep(5)  # Adjust the duration as needed

def determine_starting_buy_parameters(starting_size_Q):
    while True:
        starting_price_buy = calculate_starting_buy_price_with_retry(starting_size_Q, max_iterations=10)
        if starting_price_buy is not None:
            return starting_price_buy
        
        # Wait for a short duration before checking again
        time.sleep(5)  # Adjust the duration as needed

def calculate_starting_sell_price(starting_size_B, max_iterations=10):
    quote_increment = float(0.0000001)
    
    # Initialize the starting sell price
    starting_price_sell = None

    iterations = 0
    while iterations < max_iterations:
        # Define dynamic indicator parameters
        indicators = get_indicators()
        current_price = indicators["current_price"]
        upper_bb = indicators["upper_bb"]
        mean24 = indicators["mean24"]
                
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

    return determine_starting_prices(starting_size_B, starting_size_Q)

def calculate_starting_sell_price_with_retry(starting_size_B, max_iterations=10):
    iterations = 0

    while iterations < max_iterations:
        try:
            return calculate_starting_sell_price(starting_size_B, max_iterations=10)
        except Timeout:
            print("Timeout occurred. Retrying...")

        iterations += 1
        time.sleep(90)  # Adjust the sleep time as needed

    print("Maximum iterations reached. Conditions for determining starting sell price not met. Resetting retries.")
    return None

def calculate_starting_buy_price(starting_size_Q, max_iterations=10):
    quote_increment = float(0.0000001)
    
    # Initialize the starting buy price
    starting_price_buy = None

    iterations = 0
    while iterations < max_iterations:
        # Define dynamic indicator parameters
        indicators = get_indicators()
        current_price = indicators["current_price"]
        lower_bb = indicators["lower_bb"]
        mean24 = indicators["mean24"]
        
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

    return determine_starting_prices(starting_size_B, starting_size_Q)

def calculate_starting_buy_price_with_retry(starting_size_Q, max_iterations=10):
    iterations = 0

    while iterations < max_iterations:
        try:
            return calculate_starting_buy_price(starting_size_Q, max_iterations=10)
        except Timeout:
            print("Timeout occurred. Retrying...")

        iterations += 1
        time.sleep(5)  # Adjust the sleep time as needed

    print("Maximum iterations reached. Conditions for determining starting sell price not met. Resetting retries.")

def determine_next_open_sell_order_price(profit_percent, max_iterations=10, timeout=600):
    quote_increment = float(0.0000001)
    start_time = time.time()

    # Print statement to indicate opening price determination
    print("Determining next opening cycle sell order price...")

    # Initialize next opening cycle sell order prices
    open_price_sell = None

    iterations = 0
    while iterations < max_iterations:
        # Define dynamic indicator parameters
        indicators = get_indicators()
        current_price = indicators["current_price"]
        long_term_ma24 = indicators["long_term_ma24"]

        # Check if the timeout has been exceeded
        if time.time() - start_time > timeout:
            raise Timeout("Timeout occurred while waiting for market conditions to be met")
        
        # Determine trend direction
        upward_trend = current_price > long_term_ma24

        # Check scenarios based on trend and RSI
        if upward_trend:
            while current_rsi <= 50:
                # Keep checking RSI until it's greater than 50
                current_rsi = indicators["current_rsi"]  # Update RSI value
                time.sleep(90)  # Wait for a while before checking again

            # Trend is upward, RSI > 50
            upper_bb = indicators["upper_bb"]
            open_price_sell = float(round(max(current_price * (1 + profit_percent), 1.001 * upper_bb), -int(math.floor(math.log10(float(quote_increment))))))
        else:
            while current_rsi <= 50:
                # Keep checking RSI until it's greater than or equal to 50
                current_rsi = indicators["current_rsi"]  # Update RSI value
                time.sleep(90)  # Wait for a while before checking again

            # Trend is downward, RSI > 50
            upper_bb = indicators["upper_bb"]
            open_price_sell = float(round(min(current_price * (1 + profit_percent), 0.999 * upper_bb), -int(math.floor(math.log10(float(quote_increment))))))

        if open_price_sell is not None:
            # Retrieve best bid and ask prices
            best_bid, best_ask = get_best_bid_ask_prices_with_retry(definitions["base_asset_code"], definitions["counter_asset_code"], definitions["base_asset_issuer"], definitions["counter_asset_issuer"], max_retries=3)

            # Compare with the calculated starting sell price
            if best_bid and open_price_sell > best_bid:
                app_logger.info("Next opening cycle sell price: %s", open_price_sell)
                return open_price_sell
            else:
                print("Opening cycle price not favorable based on best bid. Continuing to wait...")

        time.sleep(90)  # Adjust the sleep time as needed

def determine_next_open_sell_order_price_with_retry(profit_percent, iterations=0, depth=0, max_iterations=10, max_depth=1000000):

    while iterations < max_iterations:
        try:
            return determine_next_open_sell_order_price(profit_percent, max_iterations=10, timeout=600)
        except Timeout:
            print("Timeout occurred. Retrying...")

        iterations += 1
        time.sleep(90)  # Adjust the sleep time as needed

    print("Maximum iterations reached. Conditions for determining opening sell price not met.")
    return determine_next_open_sell_order_price_with_retry(profit_percent, iterations, depth + 1, max_iterations, max_depth)

def determine_next_open_buy_order_price(profit_percent, max_iterations=10, timeout=600):
    quote_increment = float(0.0000001)
    start_time = time.time()

    # Print statement to indicate opening price determination
    print("Determining next opening cycle buy order price...")

    # Initialize next opening cycle buy order price
    open_price_buy = None

    iterations = 0
    while iterations < max_iterations:
        # Define dynamic indicator parameters
        indicators = get_indicators()
        current_price = indicators["current_price"]
        long_term_ma24 = indicators["long_term_ma24"]

        # Check if the timeout has been exceeded
        if time.time() - start_time > timeout:
            raise Timeout("Timeout occurred while waiting for market conditions to be met. Resetting retries...")
        
        # Recalculate current_price inside the loop
        current_price = indicators["current_price"]  # Replace with your function to get the current price
        # Define long term 24 hour moving average to determine trend
        long_term_ma24 = indicators["long_term_ma24"]

        # Determine trend direction
        upward_trend = current_price > long_term_ma24

        # Check scenarios based on trend and RSI
        if upward_trend:
            while current_rsi >= 50:
                # Keep checking RSI until it's less than 50
                current_rsi = indicators["current_rsi"] # Update RSI value
                time.sleep(90)  # Wait for a while before checking again

            # Trend is upward, RSI < 50
            lower_bb = indicators["lowwer_bb"]
            open_price_buy = float(round(max(current_price * (1 - profit_percent), 1.001 * lower_bb), -int(math.floor(math.log10(float(quote_increment))))))

        else:
            while current_rsi >= 50:
                # Keep checking RSI until it's less than 50
                current_rsi = indicators["current_rsi"]  # Update RSI value
                time.sleep(90)  # Wait for a while before checking again

            # Trend is downward, RSI < 50
            lower_bb = indicators["lowwer_bb"]
            open_price_buy = float(round(min(current_price * (1 - profit_percent), 0.999 * lower_bb), -int(math.floor(math.log10(float(quote_increment))))))

        if open_price_buy is not None:
            # Retrieve best bid and ask prices
            best_bid, best_ask = get_best_bid_ask_prices_with_retry(definitions["base_asset_code"], definitions["counter_asset_code"], definitions["base_asset_issuer"], definitions["counter_asset_issuer"], max_retries=3)

            # Compare with the calculated starting sell price
            if best_ask and open_price_buy < best_ask:
                app_logger.info("Next opening cycle buy price: %s", open_price_buy)
                return open_price_buy
            else:
                print("Opening cycle price not favorable based on best ask. Continuing to wait...")

        time.sleep(90)  # Adjust the sleep time as needed

def determine_next_open_buy_order_price_with_retry(profit_percent, iterations=0, depth=0, max_iterations=10, max_depth=1000000):

    while iterations < max_iterations:
        try:
            return determine_next_open_buy_order_price(profit_percent, max_iterations=10, timeout=600)
        except Timeout:
            print("Timeout occurred. Retrying...")

        iterations += 1
        time.sleep(90)  # Adjust the sleep time as needed

    print("Maximum iterations reached. Conditions for determining opening buy price not met. Resetting retries...")
    return determine_next_open_buy_order_price_with_retry(profit_percent, iterations, depth + 1, max_iterations, max_depth)
