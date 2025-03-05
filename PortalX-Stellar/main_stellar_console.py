# main_stellar_console.py

import sys
import time
import multiprocessing
from multiprocessing import Process, Event, Lock
from stellar_sdk import Server
from cycle_set_init import get_definitions, definitions
from stellar_db import get_db_connection, archive_old_data, fetch_trading_pairs_from_db, fetch_last_update_info, extract_data_for_historical_data_db, start_streams, generate_and_update_candle_data_for_all_intervals
from fetch_stellar_dex_data import fetch_historical_data_with_paging_token
from cycle_set_sell_buy_utils import CycleSetSellBuy
from cycle_set_buy_sell_utils import CycleSetBuySell

# Create an event to signal when the user input is ready
user_input_event = Event()

# Create a lock to synchronize the starting of cycle sets
cycle_set_lock = Lock()

server = Server("https://horizon.stellar.org")

print("Starting main_console.py")

def initial_setup_and_data_extraction(conn):
    trading_pairs = fetch_trading_pairs_from_db(conn)

    if definitions['pair'] not in trading_pairs:
        print("New trading pair detected. Extracting historical data...")
        extract_data_for_historical_data_db(definitions['base_asset_code'], definitions['base_asset_issuer'], definitions['counter_asset_code'], definitions['counter_asset_issuer'])
    else:  
        fetch_historical_data_with_paging_token(server, definitions["base_asset"], definitions["counter_asset"], last_paging_token, max_records=5000000)
        print("Historical data extraction complete.")

    return

def generate_and_update_candle_data_loop(conn, definitions):
    while True:
        # Generate and update candles
        last_update_time, _ = fetch_last_update_info(conn, definitions["base_asset_code"], definitions["base_asset_issuer"], definitions["counter_asset_code"], definitions["counter_asset_issuer"], data_source="raw_trade_update_log")
        last_processed_timestamp = last_update_time # Define most recent timestamp for raw historical trade data
        generate_and_update_candle_data_for_all_intervals(conn, definitions["base_asset_code"], definitions["base_asset_issuer"], definitions["counter_asset_code"], definitions["counter_asset_issuer"], last_processed_timestamp)
        time.sleep(60) # Pause before repeating loop 

def start_cycleset_sell_buy(user_config):
    with cycle_set_lock:
        print("Starting 'sell_buy' cycle set...")
        print(user_config)
        process_sell_buy = Process(target=CycleSetSellBuy.create_and_start_cycle_set_sell_buy, args=(user_config,))
        process_sell_buy.name = 'cycleset_sell_buy'
        print(process_sell_buy.name)
        process_sell_buy.start()
        # Reset user_config to None after starting the process
        user_config = None
        print(user_config)

        return user_config

def start_cycleset_buy_sell(user_config):
    with cycle_set_lock:
        print("Starting 'buy_sell' cycle set...")
        print(user_config)
        process_buy_sell = Process(target=CycleSetBuySell.create_and_start_cycle_set_buy_sell, args=(user_config,))
        process_buy_sell.name = 'cycleset_buy_sell'
        print(process_buy_sell.name)
        process_buy_sell.start()
        # Reset user_config to None after starting the process
        user_config = None
        print(user_config)

        return user_config

def main(user_config):
    
    cycle_set_started = True  # Flag to indicate if a cycle set has been started

    while cycle_set_started:

        # Confirm cycle set type to start
        print("Confirm cycle set type to start:")
        print("1. Sell Buy Cycle Set")
        print("2. Buy Sell Cycle Set")
        print("3. Exit")
        choice = int(input("Enter your choice (1, 2, or 3): "))

        if choice == 1:
            start_cycleset_sell_buy(user_config)
            
        elif choice == 2:
            start_cycleset_buy_sell(user_config)
    
        elif choice == 3:
            print("Exiting PortalX...")
            sys.exit()
        else:
            print("Invalid choice. Please enter 1, 2, or 3.")

        # Reset user_config to None after starting the process
        user_config = None

    return user_config

def start_cycle_set_interaction():
    candle_update_process = None
    # Define initial global user_config
    user_config = definitions["user_config"]

    cycle_set_started = False  # Flag to indicate if a cycle set has been started

    while not cycle_set_started:

        # Confirm cycle set type to start
        print("Confirm cycle set type to start:")
        print("1. Sell Buy Cycle Set")
        print("2. Buy Sell Cycle Set")
        print("3. Exit")
        choice = int(input("Enter your choice (1, 2, or 3): "))

        if candle_update_process:
            if choice == 1:
                start_cycleset_sell_buy(user_config)
                cycle_set_started = True  # Set flag after starting cycle set
            elif choice == 2:
                start_cycleset_buy_sell(user_config)
                cycle_set_started = True  # Set flag after starting cycle set
            elif choice == 3:
                print("Exiting PortalX...")
                sys.exit()
            else:
                print("Invalid choice. Please enter 1, 2, or 3.")

        # Reset user_config to None after starting the process
        user_config = None

    return user_config

def prompt_user_to_start_new_cycleset():
    user_config = None
    while True:
        if user_config is None:
            definitions = get_definitions()  # Refresh user_config if it's reset to None
            if definitions['pair'] not in trading_pairs:
                print("New trading pair detected. Extracting historical data...")
                extract_data_for_historical_data_db(conn, definitions['base_asset_code'], definitions['base_asset_issuer'], definitions['counter_asset_code'], definitions['counter_asset_issuer'])

            main(definitions["user_config"])  # Assuming main can work with the updated user_config directly
        time.sleep(60)  # Wait before prompting again

if __name__ == "__main__":

    conn = get_db_connection()

    trading_pairs = fetch_trading_pairs_from_db(conn)

    last_update_time, last_paging_token = fetch_last_update_info(conn, definitions["base_asset_code"], definitions["base_asset_issuer"], definitions["counter_asset_code"], definitions["counter_asset_issuer"], data_source="raw_trade_update_log")
    
    if definitions['pair'] not in trading_pairs:
        print(f"The pair {definitions["pair"]} is not in the database. Adding pair to database and extracing historical data. Please wait...")
        extract_data_for_historical_data_db(conn, definitions['base_asset_code'], definitions['base_asset_issuer'], definitions['counter_asset_code'], definitions['counter_asset_issuer'])
    else:  
        # Generate and update candles
        last_update_time, last_paging_token = fetch_last_update_info(conn, definitions["base_asset_code"], definitions["base_asset_issuer"], definitions["counter_asset_code"], definitions["counter_asset_issuer"], data_source="raw_trade_update_log")
        last_processed_timestamp = last_update_time # Define most recent timestamp for raw historical trade data
        generate_and_update_candle_data_for_all_intervals(conn, definitions["base_asset_code"], definitions["base_asset_issuer"], definitions["counter_asset_code"], definitions["counter_asset_issuer"], last_processed_timestamp)
        
