# cycle_set_sell_buy_utils.py

import sys
import threading
import time
from logging_config import app_logger, info_logger, error_logger, setup_cycleset_logger
import json
import requests
from cycle_set_init import definitions
from prices_utils import determine_starting_prices, determine_next_open_sell_order_price_with_retry
from order_utils import place_starting_open_sell_order, place_next_opening_cycle_sell_order, place_next_closing_cycle_buy_order, wait_for_order, get_order_status, cancel_orders
from order_processing_utils import open_limit_sell_order_processing, close_limit_buy_order_processing
from compounding_utils import calculate_close_limit_buy_compounding_amt_Q, determine_next_close_size_Q_limit, determine_next_open_size_B_limit, calculate_open_limit_sell_compounding_amt_B

# Define the recursion limit (adjust this value as needed)
RECURSION_LIMIT = 1000000

# Set the recursion limit
sys.setrecursionlimit(RECURSION_LIMIT)

# Define locks
sell_buy_lock = threading.Lock()
menu_lock = threading.Lock()
sell_buy_cycle_start_lock = threading.Lock()
sell_buy_cycle_lock = threading.Lock()
print_lock = threading.Lock()

# Create a lock to prevent concurrent modification of user_config
user_config_sell_buy_lock = threading.Lock()

# Initialize the counters for CycleSet (sell_buy) instances
sell_buy_cycle_set_counter = 0

# Create a list to hold CycleSetSellBuy instances
cycle_sets_sell_buy = []

# Definine necessary parameters
server = definitions["server"]
network_passphrase = definitions["network_passphrase"]
conn = definitions["conn"]
secret_key = definitions["secret_key"]
user_config = definitions["user_config"]
base_asset_code = definitions["base_asset_code"],
base_asset_issuer = definitions["base_asset_issuer"],
counter_asset_code = definitions["counter_asset_code"],
counter_asset_issuer = definitions["counter_asset_issuer"]
pair = definitions["pair"]
starting_size_B = definitions["starting_size_B"]
starting_size_Q = definitions["starting_size_Q"]
profit_percent = definitions["profit_percent"]
wait_period_unit = definitions["wait_period_unit"]
first_order_wait_period = definitions["first_order_wait_period"]
chart_interval = definitions["chart_interval"]
num_intervals = definitions["num_intervals"]
window_size = definitions["window_size"]
compound_percent = definitions["compound_percent"]
compounding_option = definitions["compounding_option"]
stacking = definitions["stacking"]
step_price = definitions["step_price"]
base_asset = definitions["base_asset"]
counter_asset = definitions["counter_asset"]

base_increment = float(0.0000001)
quote_increment = float(0.0000001)
decimal_places = 7

# Define order processing parameters dictionary with initialized values
order_processing_params = {
    "total_received_Q_ols": None,
    "total_spent_B_ols": None,
    "residual_amt_B_ols": None,
    "total_received_B_olb": None,
    "total_spent_Q_olb": None,
    "residual_amt_Q_olb": None,
    "total_received_B_clb": None,
    "total_spent_Q_clb": None,
    "residual_amt_Q_clb": None,
    "total_received_Q_cls": None,
    "total_spent_B_cls": None,
    "residual_amt_B_cls": None,
    "total_received_Q_oms": None,
    "total_spent_B_oms": None,
    "residual_amt_B_oms": None,
    "total_received_B_omb": None,
    "total_spent_Q_omb": None,
    "residual_amt_Q_omb": None,
    "total_received_B_cmb": None,
    "total_spent_Q_cmb": None,
    "residual_amt_Q_cmb": None,
    "total_received_Q_cms": None,
    "total_spent_B_cms": None,
    "residual_amt_B_cms": None
}
class CycleSetSellBuy:

    # Class attribute to store the count of instances
    sell_buy_counter = 0
    sell_buy_cycle_count = 0

    # Initialize recursion_depth
    recursion_depth = 0

    # Class attribute to store all created cycle set instances
    cycleset_sell_buy_instances = cycle_sets_sell_buy
    
    def __init__(
            self, 
            sell_buy_logger,
            base_asset_code,
            base_asset_issuer,
            counter_asset_code,
            counter_asset_issuer,
            pair,
            starting_size_B,
            profit_percent,
            compound_percent,
            compounding_option,
            wait_period_unit,
            first_order_wait_period,
            chart_interval,
            num_intervals,
            window_size,
            stacking=False,
            step_price=False,
            cycle_type='',
            starting_dollar_value=None,
            current_dollar_value=None,
            percent_gain_loss_dollar=None,
            percent_gain_loss_base=None,
            percent_gain_loss_quote=None,
            average_profit_percent_per_hour=None,
            average_profit_percent_per_day=None,
            completed_sell_buy_cycles=0,
        ):  

        self.cycle_type = "sell_buy"

        with print_lock:
      
            app_logger.info(f"Creating CycleSet with cycle_type: {self.cycle_type}")

            # Initialize and increment cycle set counters based on cycle type
            self.cycle_type == "sell_buy"
            CycleSetSellBuy.sell_buy_counter += 1
            self.cycleset_sell_buy_number = CycleSetSellBuy.sell_buy_counter
         
            app_logger.info(f"CycleSet {self.cycleset_sell_buy_number} ({self.cycle_type}) counted")

            # Initialize cycle counts for sell_buy and buy_sell
            cycle_type == 'sell_buy'
            CycleSetSellBuy.sell_buy_cycle_count = 0  # Reset to '0' for a new CycleSet instance
            self.cycle_sell_buy_number = CycleSetSellBuy.sell_buy_cycle_count + 1

            app_logger.info(f"Cycle {self.cycle_sell_buy_number} (sell_buy) counted")

        self.cycleset_sell_buy_instance_id = f"CycleSet {self.cycleset_sell_buy_number} (sell_buy)"
        self.base_asset_code = base_asset_code
        self.base_asset_issuer = base_asset_issuer
        self.counter_asset_code = counter_asset_code
        self.counter_asset_issuer = counter_asset_issuer
        self.pair = pair
        self.starting_size = starting_size_B
        self.profit_percent = profit_percent
        self.compound_percent = compound_percent
        self.compounding_option = compounding_option
        self.wait_period_unit = wait_period_unit
        self.first_order_wait_period = first_order_wait_period
        self.chart_interval = chart_interval
        self.num_intervals = num_intervals
        self.window_size = window_size
        self.stacking = stacking
        self.step_price = step_price
        self.sell_buy_orders = []  # List to store cycle set order IDs
        self.cycle_sell_buy_instances = []  # Initialize an empty list to store cycle instances within a cycle set
        self.cycleset_sell_buy_running = False # States whether a cycle set is running or not
        self.cycleset_sell_buy_status = "Pending" # Describes the status as either: "Pending", "Active", "Failed", or "Stopped"
        self.completed_sell_buy_cycles = completed_sell_buy_cycles
        self.open_size_B = 0
        self.open_size_B_history = []  # Dictionary to store opening cycle sizes of base currency for each cycle
        self.residual_amt_B_list = [] # Dictionary to store residual amounts of base currency not used in sell orders
        self.residual_amt_Q_list = [] # Dictionary to store residual amounts of quote currency not used in buy orders
        self.sell_buy_cycleset_lock = threading.Lock()
        self.starting_dollar_value = starting_dollar_value
        self.current_dollar_value = current_dollar_value
        self.percent_gain_loss_dollar = percent_gain_loss_dollar
        self.percent_gain_loss_base = percent_gain_loss_base
        self.percent_gain_loss_quote = percent_gain_loss_quote
        self.average_profit_percent_per_hour = average_profit_percent_per_hour
        self.average_profit_percent_per_day = average_profit_percent_per_day
        self.sell_buy_logger = sell_buy_logger
 
    # Other methods...
        
    def add_sell_buy_cycle(self, open_size_B, cycle_type):
        # Assuming other attributes like product_id, etc., are set in the CycleSet instance
        cycle_type == "sell_buy"
        CycleSetSellBuy.sell_buy_cycle_count += 1
        self.cycle_sell_buy_number = CycleSetSellBuy.sell_buy_cycle_count

        cycle_sell_buy_instance = CycleSellBuy(open_size_B, cycle_set_sell_buy_instance=self, cycle_type="sell_buy")
        self.cycle_sell_buy_instances.append(cycle_sell_buy_instance)

        return cycle_sell_buy_instance, self.cycle_sell_buy_number

    def place_next_sell_buy_cycle_orders(self, open_size_B, open_price_sell, sell_buy_cycle_instance):
        try:
            # Update 'cycle_number' in the CycleSet class
            self.cycle_sell_buy_number = sell_buy_cycle_instance.cycle_sell_buy_number
            app_logger.info("Self: %s", self)

            with self.sell_buy_cycleset_lock:
                print(f"Thread ID: {threading.get_ident()} - Timestamp: {time.time()} - Before important step")
                # Place next opening cycle sell order

                print("Placing the next opening cycle sell order...")
                open_order_id_sell = place_next_opening_cycle_sell_order(server, network_passphrase, secret_key, base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, open_size_B, open_price_sell)
                
                if open_order_id_sell is not None:
                    self.sell_buy_orders.append(open_order_id_sell)
                    app_logger.info("Opening cycle sell order placed successfully: %s", open_order_id_sell)
                    self.cycle_sell_buy_running = True  # Set the cycle running status to True
                    self.cycle_sell_buy_status = f"CyclSet {self.cycleset_sell_buy_number} ({self.cycle_type}) Cycle {sell_buy_cycle_instance.cycle_sell_buy_number}: Active-Opening Cycle Sell Order"
                    self.sell_buy_logger.info(self.cycle_sell_buy_status)

                if open_order_id_sell is None:
                    error_logger.error(f"Opening cycle sell order not found for CycleSet {self.cycleset_sell_buy_number} {self.cycle_type}. Stopping the current cycle set.")
                    return
                
                print(f"Thread ID: {threading.get_ident()} - Timestamp: {time.time()} - After important step")

            with self.sell_buy_cycleset_lock:
                print(f"Thread ID: {threading.get_ident()} - Timestamp: {time.time()} - Before important step")        
                
                # Call wait_for_order function
                order_details = wait_for_order(api_key, api_secret, open_order_id_sell, max_retries=3)
           
                # Check if the order_details is None
                if order_details is None:
                    # Handle the case where wait_for_order did not complete successfully
                    error_logger.error(f"Opening sell order status not found for CycleSet {self.cycleset_sell_buy_number} ({self.cycle_type}) Cycle {sell_buy_cycle_instance.cycle_sell_buy_number}. Stopping the current cycle set.")
                    self.cycleset_sell_buy_running = False # Set cycle set attribute to not running
                    self.cycleset_sell_buy_status = f"CyclSet {self.cycleset_sell_buy_number} ({self.cycle_type}) Failed"
                    self.cycle_sell_buy_status = f"CyclSet {self.cycleset_sell_buy_number} ({self.cycle_type}) Cycle {sell_buy_cycle_instance.cycle_sell_buy_number}: Failed-Opening Cycle Sell Order"
                    self.sell_buy_logger.info(self.cycle_sell_buy_status)
                    print("Opening sell order failed. Cycle failed. Cycle set stopped.")
                    return
            
                # Process opening cycle sell order amount spent, fees, and amount to be received
                print("Processing opening cycle sell order assets...")
                order_processing_params = open_limit_sell_order_processing(open_size_B, order_details, order_processing_params = {})
                total_received_Q_ols = order_processing_params["total_received_Q_ols"] 
                total_spent_B_ols = order_processing_params["total_spent_B_ols"]
                residual_amt_B_ols = order_processing_params["residual_amt_B_ols"]
                self.residual_amt_B_list.append(residual_amt_B_ols)

                try:
                    app_logger.info("Value of order_processing_params: %s", order_processing_params)
                    if order_processing_params is not None:
                        # Calculate closing cycle buy price
                        print("Calculating closing cycle buy price...")
                        close_price_buy = round(open_price_sell * (1 - profit_percent), decimal_places)
                        app_logger.info("Closing cycle buy price calculated: %s", close_price_buy)
                        self.cycle_sell_buy_status = f"CyclSet {self.cycleset_sell_buy_number} ({self.cycle_type}) Cycle {sell_buy_cycle_instance.cycle_sell_buy_number}: Pending-Closing Buy Order"
                        self.sell_buy_logger.info(self.cycle_sell_buy_status)

                except Exception as e:
                        error_logger.error(f"Error in order processing: {e}")

                if order_processing_params is None:
                    # Handle the case where open_limit_sell_order_processing did not complete successfully
                    error_logger.error(f"Order processing parameters not found for opening cycle sell order for CycleSet {self.cycleset_sell_buy_number}({self.cycle_type}) Cycle {sell_buy_cycle_instance.cycle_sell_buy_number}. Stopping the current cycle set.")
                    self.cycleset_sell_buy_running = False # Set cycle set attribute to not running
                    self.cycleset_sell_buy_status = "Failed"
                    self.cycle_sell_buy_status = f"CyclSet {self.cycleset_sell_buy_number} ({self.cycle_type}) Cycle {sell_buy_cycle_instance.cycle_sell_buy_number}: Failed-Opening Sell Order"
                    self.sell_buy_logger.info(self.cycle_sell_buy_status)
                    print("Order processing failed. Cycle not completed. Cycle set stopped. Check for any open orders related to this request on exchange and handle manually.")       
                    return
                
                # Calculate starting (cycle 1) close cycle compounding amount and next starting close cycle size
                compounding_amt_Q_clb, no_compounding_Q_limit_clb = calculate_close_limit_buy_compounding_amt_Q(total_received_Q_ols, total_spent_B_ols, close_price_buy, quote_increment)
                close_size_Q = determine_next_close_size_Q_limit(compounding_option, total_received_Q_ols, no_compounding_Q_limit_clb, compounding_amt_Q_clb, compound_percent)

                if close_size_Q is not None:
                    print("Closing cycle compounding and next size calculated successfully")

                if close_size_Q is None:
                    error_logger.error(f"Closing cycle size could not be determined for closing buy order for CycleSet {self.cycleset_sell_buy_number}. Stopping the current cycle set.")
                    return

                print(f"Thread ID: {threading.get_ident()} - Timestamp: {time.time()} - After important step")

            with self.sell_buy_cycleset_lock:
                print(f"Thread ID: {threading.get_ident()} - Timestamp: {time.time()} - Before important step")

                # Place closing cycle buy order
                print("Placing the closing cycle buy order...")
                close_order_id_buy = place_next_closing_cycle_buy_order(server, network_passphrase, secret_key, base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, close_size_Q, close_price_buy)

                if close_order_id_buy is not None:
                    self.sell_buy_orders.append(close_order_id_buy)    
                    app_logger.info("Closing cycle buy order placed successfully: %s", close_order_id_buy)
                    self.cycle_sell_buy_status = f"CyclSet {self.cycleset_sell_buy_number} ({self.cycle_type}) Cycle {sell_buy_cycle_instance.cycle_sell_buy_number}: Active-Closing Buy Order"
                    self.sell_buy_logger.info(self.cycle_sell_buy_status)

                if close_order_id_buy is None:
                    error_logger.error(f"Closing buy order not found for CycleSet {self.cycleset_sell_buy_number}. Stopping the current cycle set.")
                    return
                
                print(f"Thread ID: {threading.get_ident()} - Timestamp: {time.time()} - After important step")

            with self.sell_buy_cycleset_lock:
                print(f"Thread ID: {threading.get_ident()} - Timestamp: {time.time()} - Before important step")  

                # Call wait_for_order function
                order_details = wait_for_order(api_key, api_secret, close_order_id_buy, max_retries=3)

                # Check if the order_details is None
                if order_details is None:
                    # Handle the case where wait_for_order did not complete successfully
                    error_logger.error(f"Closing buy order status not found for CycleSet {self.cycleset_sell_buy_number} ({self.cycle_type}) Cycle {sell_buy_cycle_instance.cycle_sell_buy_number}. Stopping the current cycle set.")
                    self.cycleset_sell_buy_running = False # Set cycle set attribute to not running
                    self.cycle_sell_buy_running = False
                    self.cycleset_sell_buy_status = f"CyclSet {self.cycleset_sell_buy_number} ({self.cycle_type}) Failed"
                    self.cycle_sell_buy_status = f"CyclSet {self.cycleset_sell_buy_number} ({self.cycle_type}) Cycle {sell_buy_cycle_instance.cycle_sell_buy_number}: Failed-Closing Buy Order"
                    self.sell_buy_logger.info(self.cycle_sell_buy_status)
                    print("Closing buy order failed. Cycle not completed and has been stopped. Cycle set stopped.")
                    return

                if order_details is not None:
                    print("Closing cycle buy order completed successfully")           
        
                # Process closing cycle buy order amount spent, fees, and amount to be received
                print("Processing closing cycle buy order assets...")
                order_processing_params = close_limit_buy_order_processing(close_size_Q, order_details, order_processing_params = {})
                total_received_B_clb = order_processing_params["total_received_B_clb"]
                total_spent_Q_clb = order_processing_params["total_spent_Q_clb"]
                residual_amt_Q_clb = order_processing_params["residual_amt_Q_clb"]
                self.residual_amt_Q_list.append(residual_amt_Q_clb)

                if order_processing_params is not None:
                    app_logger.info(f"CyclSet {self.cycleset_sell_buy_number} ({self.cycle_type}) Cycle{sell_buy_cycle_instance.cycle_sell_buy_number} completed.")
                    self.cycle_status = f"CyclSet {self.cycleset_sell_buy_number} ({self.cycle_type}) Cycle {sell_buy_cycle_instance.cycle_sell_buy_number}: Completed-Pending Next Opening Sell Order"
                    self.sell_buy_logger.info(self.cycle_status)

                if order_processing_params is None:
                    # Handle the case where close_limit_buy_order_processing did not complete successfully
                    error_logger.error(f"Order processing parameters not found for closing cycle buy order for CycleSet {self.cycleset_sell_buy_number} ({self.cycle_type}) Cycle {sell_buy_cycle_instance.cycle_sell_buy_number}. Stopping the current cycle set. Check for any open orders related to this request on exchange and handle manually.")
                    self.cycleset_sell_buy_running = False # Set cycle set attribute to not running
                    self.cycle_sell_buy_running = False  # Set the cycle running status to False
                    self.cycleset_sell_buy_status = f"CyclSet {self.cycleset_sell_buy_number} ({self.cycle_type}) Failed"
                    self.cycle_sell_buy_status = f"CyclSet {self.cycleset_sell_buy_number} ({self.cycle_type}) Cycle {sell_buy_cycle_instance.cycle_sell_buy_number}: Failed-Closing Buy Order"
                    self.sell_buy_logger.info(self.cycle_status)
                    print("Order processing failed. Cycle not completed. Cycle set stopped. Check for any open orders related to this request on exchange and handle manually.")
                    return
                
                print(f"Thread ID: {threading.get_ident()} - Timestamp: {time.time()} - After important step")

                # Gather data for logic to determine price for next opening cycle sell order for repeating cycle

            with self.sell_buy_cycleset_lock:
                print(f"Thread ID: {threading.get_ident()} - Timestamp: {time.time()} - Before important step")
                
                # Determine next opening cycle sell price
                print("Determining next opening cycle sell order price...")
                open_price_sell = determine_next_open_sell_order_price_with_retry(profit_percent, iterations=0, depth=0, max_iterations=10, max_depth=1000000)
                app_logger.info("Opening cycle sell order price determined: %s", open_price_sell)

                if open_price_sell is not None:
                    # Calculate compounding amount and size for next opening cycle sell order
                    print("Calculating compounding amount and next opening cycle sell order size...")
                    compounding_amount_B_ols, no_compounding_B_limit_ols = calculate_open_limit_sell_compounding_amt_B(total_received_B_clb, total_spent_Q_clb, open_price_sell, base_increment)
                    open_size_B = determine_next_open_size_B_limit(compounding_option, total_received_B_clb, no_compounding_B_limit_ols, compounding_amount_B_ols, compound_percent)
                
                if open_price_sell is None:
                    error_logger.error(f"Failed to determine next opening cycle price for next sell order for CycleSet {self.cycleset_sell_buy_number}. Stopping the current cycle set.")
                    return
                
                if open_size_B is not None:
                    # Append the base currency size value to the history list of the current instance
                    self.open_size_B_history.append(open_size_B)

                if open_size_B is None:
                    error_logger.error(f"Opening cycle size could not be determined for openining sell order for CycleSet {self.cycleset_sell_buy_number}. Stopping the current cycle set.")
                    return
                
                print(f"Thread ID: {threading.get_ident()} - Timestamp: {time.time()} - After important step")

            with self.sell_buy_cycleset_lock:
                print(f"Thread ID: {threading.get_ident()} - Timestamp: {time.time()} - Before important step")
                # Create a new Cycle instance
                sell_buy_cycle_instance, self.cycle_sell_buy_number = self.add_sell_buy_cycle(open_size_B, "sell_buy")

                sell_buy_cycle_instance.cycle_sell_buy_number = self.cycle_sell_buy_number
                
                # Add the cycle instance to the list in the CycleSet and Cycle
                self.cycle_sell_buy_instances.append(sell_buy_cycle_instance)
                sell_buy_cycle_instance.cycle_sell_buy_instances.append(sell_buy_cycle_instance)

                print("Sell_buy cycle completed.")
                self.sell_buy_logger.info("Opening sell order ID: %s, Closing buy order ID: %s", open_order_id_sell, close_order_id_buy)

                # Call methods on the Cycle instance
                app_logger.info(f"Starting next sell_buy cycle, Cycle {sell_buy_cycle_instance.cycle_sell_buy_number} of CycleSet {self.cycleset_sell_buy_number} ({self.cycle_type})")
                
                print(f"Thread ID: {threading.get_ident()} - Timestamp: {time.time()} - After important step")

            # Increment the recursion depth counter
            self.recursion_depth += 1

            # Check if the recursion depth exceeds the limit
            if self.recursion_depth >= RECURSION_LIMIT:
                print("Recursion limit reached. Stopping recursion.")
                return open_order_id_sell, close_order_id_buy, None  # Return None for the recursive call

            # Call the next function without handling recursion here
            next_order_ids = self.place_next_sell_buy_cycle_orders(open_size_B, open_price_sell, sell_buy_cycle_instance)
            return open_order_id_sell, close_order_id_buy, next_order_ids
            
        except requests.exceptions.RequestException as e:
            # Handle request exceptions
            error_logger.error(f"An error occurred in place_next_sell_buy_cycle_orders: {e}")
            error_logger.error(f"Status code: {e.response.status_code}" if hasattr(e, 'response') and e.response is not None else "")
            # Add additional logging to capture specific details
            error_logger.exception("RequestException details:", exc_info=True)
            return None, None, None

        except json.JSONDecodeError as e:
            # Handle JSON decoding errors
            error_logger.error(f"Error decoding JSON response in place_next_sell_buy_cycle_orders: {e}")
            # Add additional logging to capture specific details
            error_logger.exception("JSONDecodeError details:", exc_info=True)
            return None, None, None

        except Exception as e:
            # Handle other unexpected errors
            error_logger.error(f"Thread ID: {threading.get_ident()} - Timestamp: {time.time()} - An unexpected error occurred in place_next_sell_buy_cycle_orders: {e}")
            # Add additional logging to capture specific details
            error_logger.exception("Unexpected error details:", exc_info=True)
            return None, None, None

    def place_starting_sell_buy_cycle_orders(self, starting_size_B, sell_buy_cycle_instance):

        try:
            self.cycleset_sell_buy_status = f"CyclSet {self.cycleset_sell_buy_number} {self.cycle_type} Started"
            self.cycle_sell_buy_status = f"CyclSet {self.cycleset_sell_buy_number} {self.cycle_type} Cycle {self.cycle_sell_buy_number}: Pending-Starting Opening Sell Order"
            app_logger.info(self.cycle_sell_buy_status)

            with self.sell_buy_cycleset_lock:
                print(f"Thread ID: {threading.get_ident()} - Timestamp: {time.time()} - Before important step")

                # Determine both starting prices
                app_logger.info("Determining starting price for sell order...")
                starting_price_sell, starting_price_buy = determine_starting_prices(starting_size_B, starting_size_Q)

                # Use the starting_price_sell for the sell side
                # starting_price_buy can be ignored or set to None

                print(f"Thread ID: {threading.get_ident()} - Timestamp: {time.time()} - After important step")

            with self.sell_buy_cycleset_lock:
                print(f"Thread ID: {threading.get_ident()} - Timestamp: {time.time()} - Before important step")   
                # Place starting (cycle 1) opening cycle sell order
                print("Placing the starting opening cycle sell order...")
                open_order_id_sell = place_starting_open_sell_order(server, network_passphrase, secret_key, base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, starting_size_B, starting_price_sell)
                
                if open_order_id_sell is not None:
                    self.sell_buy_orders.append(open_order_id_sell)
                    app_logger.info("Starting opening cycle sell order placed successfully: %s", open_order_id_sell)
                    self.cycle_sell_buy_running = True  # Set the cycle running status to True
                    self.cycleset_sell_buy_status = f"CyclSet {self.cycleset_sell_buy_number} {self.cycle_type} Active"
                    self.cycle_sell_buy_status = f"CyclSet {self.cycleset_sell_buy_number} {self.cycle_type} Cycle {self.cycle_sell_buy_number}: Active-Starting Opening Sell Order"
                    self.sell_buy_logger.info(self.cycle_sell_buy_status)

                if open_order_id_sell is None:
                    error_logger.error(f"Starting opening sell order not found for CycleSet {self.cycleset_sell_buy_number} {self.cycle_type}. Stopping the current cycle set.")
                    return
                
                print(f"Thread ID: {threading.get_ident()} - Timestamp: {time.time()} - After important step")

            with self.sell_buy_cycleset_lock:
                print(f"Thread ID: {threading.get_ident()} - Timestamp: {time.time()} - Before important step") 
                
                # Call wait_for_order function
                order_details = wait_for_order(api_key, api_secret, open_order_id_sell, max_retries=3)

                # Check if the order_details is None
                if order_details is None:
                    # Handle the case where wait_for_order did not complete successfully
                    error_logger.error(f"Starting opening sell order status not found for CycleSet {self.cycleset_sell_buy_number}. Stopping the current cycle set.")
                    self.cycleset_sell_buy_running = False # Set cycle set attribute to not running
                    self.cycleset_sell_buy_status = f"CyclSet {self.cycleset_sell_buy_number} {self.cycle_type} Failed"
                    self.cycle_sell_buy_status = f"CyclSet {self.cycleset_sell_buy_number} ({self.cycle_type} Cycle {self.cycle_sell_buy_number}: Failed-Starting Opening Sell Order"
                    self.sell_buy_logger.info(self.cycle_sell_buy_status)
                    print("Starting opening sell order failed. Cycle failed. Cycle set stopped.")
                    return
                
                # Process starting opening cycle sell order amount spent, fees, and amount to be received
                print("Processing starting opening cycle sell order assets...")
                order_processing_params = open_limit_sell_order_processing(starting_size_B, order_details, order_processing_params = {})
                total_received_Q_ols = order_processing_params["total_received_Q_ols"] 
                total_spent_B_ols = order_processing_params["total_spent_B_ols"]
                residual_amt_B_ols = order_processing_params["residual_amt_B_ols"]
                self.residual_amt_B_list.append(residual_amt_B_ols)

                try:
                    app_logger.info("Value of order_processing_params: %s", order_processing_params)
                    if order_processing_params is not None:

                        # Calculate starting (cycle 1) closing cycle buy price
                        print("Calculating starting closing cycle buy price...")
                        close_price_buy = round(starting_price_sell * (1 - profit_percent), decimal_places)
                        app_logger.info("Closing cycle buy price calculated: %s", close_price_buy)
                        self.cycle_sell_buy_status = f"CyclSet {self.cycleset_sell_buy_number} ({self.cycle_type}) Cycle {self.cycle_sell_buy_number}: Pending-Starting Closing Buy Order"
                        self.sell_buy_logger.info(self.cycle_sell_buy_status)

                except Exception as e:
                    error_logger.error(f"Error in order processing: {e}")

                if order_processing_params is None:
                    # Handle the case where open_limit_sell_order_processing did not complete successfully
                    error_logger.error(f"Order processing parameters not found for opening cycle sell order for CycleSet {self.cycleset_sell_buy_number}. Stopping the current cycle set.")
                    self.cycleset_sell_buy_running = False # Set cycle set attribute to not running
                    self.cycleset_sell_buy_status = f"CyclSet {self.cycleset_sell_buy_number} ({self.cycle_type}) Failed"
                    self.cycle_sell_buy_status = f"CyclSet {self.cycleset_sell_buy_number} ({self.cycle_type}) Cycle {self.cycle_sell_buy_number}: Failed-Starting Opening Sell Order"
                    self.sell_buy_logger.info(self.cycle_sell_buy_status)
                    print("Order processing failed. Cycle not completed. Cycle set stopped. Check for any open orders related to this request on exchange and handle manually.")       
                    return
                
                # Calculate starting (cycle 1) close cycle compounding amount and next starting close cycle size
                compounding_amt_Q_clb, no_compounding_Q_limit_clb = calculate_close_limit_buy_compounding_amt_Q(total_received_Q_ols, total_spent_B_ols, close_price_buy, quote_increment)
                next_size_Q = determine_next_close_size_Q_limit(compounding_option, total_received_Q_ols, no_compounding_Q_limit_clb, compounding_amt_Q_clb, compound_percent)

                if next_size_Q is not None:
                    print("Starting closing cycle compounding and next size calculated successfully")

                if next_size_Q is None:
                    error_logger.error(f"Next size could not be determined for closing buy order for CycleSet {self.cycleset_sell_buy_number}. Stopping the current cycle set.")
                    return
                
                print(f"Thread ID: {threading.get_ident()} - Timestamp: {time.time()} - After important step")

            with self.sell_buy_cycleset_lock:
                print(f"Thread ID: {threading.get_ident()} - Timestamp: {time.time()} - Before important step")

                # Place starting closing cycle buy order
                print("Placing the starting closing cycle buy order...")
                close_order_id_buy = place_next_closing_cycle_buy_order(server, network_passphrase, secret_key, base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, close_size_Q, close_price_buy)

                if close_order_id_buy is not None:
                    self.sell_buy_orders.append(close_order_id_buy)    
                    app_logger.info("Starting closing cycle buy order placed successfully: %s", close_order_id_buy)
                    self.cycle_sell_buy_status = f"CyclSet {self.cycleset_sell_buy_number} {self.cycle_type} Cycle {self.cycle_sell_buy_number}: Active-Starting Closing Buy Order"
                    self.sell_buy_logger.info(self.cycle_sell_buy_status)

                if close_order_id_buy is None:
                    error_logger.error(f"Starting closing buy order not found for CycleSet {self.cycleset_sell_buy_number}. Stopping the current cycle set.")
                    return
                
                print(f"Thread ID: {threading.get_ident()} - Timestamp: {time.time()} - After important step")

            with self.sell_buy_cycleset_lock: 
                print(f"Thread ID: {threading.get_ident()} - Timestamp: {time.time()} - Before important step")

                # Call wait_for_order function
                order_details = wait_for_order(api_key, api_secret, close_order_id_buy, max_retries=3)

                # Check if the order_details is None
                if order_details is None:
                    # Handle the case where wait_for_order did not complete successfully
                    error_logger.error(f"Starting closing buy order status not found for CycleSet {self.cycleset_sell_buy_number}. Stopping the current cycle set.")
                    self.cycleset_sell_buy_running = False # Set cycle set attribute to not running
                    self.cycle_sell_buy_running = False
                    self.cycleset_sell_buy_status = f"CyclSet {self.cycleset_sell_buy_number} {self.cycle_type} Failed"
                    self.cycle_sell_buy_status = f"CyclSet {self.cycleset_sell_buy_number} {self.cycle_type} Cycle {self.cycle_sell_buy_number}: Failed-Starting Closing Buy Order"
                    self.sell_buy_logger.info(self.cycle_sell_buy_status)
                    print("Starting closing buy order failed. Cycle not completed and has been stopped. Cycle set stopped.")
                    return
                
                if order_details is not None:
                    print("Starting closing cycle buy order completed successfully")               
        
                # Process starting closing cycle buy order amount spent, fees, and amount to be received
                print("Processing starting closing cycle buy order assets...")
                close_size_Q = next_size_Q
                order_processing_params = close_limit_buy_order_processing(close_size_Q, order_details, order_processing_params = {})
                total_received_B_clb = order_processing_params["total_received_B_clb"]
                total_spent_Q_clb = order_processing_params["total_spent_Q_clb"]
                residual_amt_Q_clb = order_processing_params["residual_amt_Q_clb"]
                self.residual_amt_Q_list.append(residual_amt_Q_clb)

                if order_processing_params is not None:
                    print("Starting sell_buy cycle completed.")
                    self.cycle_sell_buy_status = f"CyclSet {self.cycleset_sell_buy_number} {self.cycle_type} Cycle {self.cycle_sell_buy_number}: Pending-Next Opening Sell Order"
                    self.sell_buy_logger.info(self.cycle_sell_buy_status)

                if order_processing_params is None:
                    # Handle the case where open_limit_sell_order_processing did not complete successfully
                    error_logger.error(f"Order processing parameters not found for closing cycle buy order for CycleSet {self.cycleset_sell_buy_number}. Stopping the current cycle set. Check for any open orders related to this request on exchange and handle manually.")
                    self.cycleset_sell_buy_running = False # Set cycle set attribute to not running
                    self.cycle_sell_buy_running = False  # Set the cycle running status to False
                    self.cycleset_sell_buy_status = f"CyclSet {self.cycleset_sell_buy_number} {self.cycle_type} Failed"
                    self.cycle_sell_buy_status = f"CyclSet {self.cycleset_sell_buy_number} {self.cycle_type} Cycle {self.cycle_sell_buy_number}: Failed-Starting Closing Buy Order"
                    self.sell_buy_logger.info(self.cycle_sell_buy_status)
                    print("Order processing failed. Cycle not completed. Cycle set stopped. Check for any open orders related to this request on exchange and handle manually.")
                    return
                
                print(f"Thread ID: {threading.get_ident()} - Timestamp: {time.time()} - After important step")
                                       
                # Gather data for logic to determine price for next opening cycle sell order for repeating cycle

            with self.sell_buy_cycleset_lock:
                print(f"Thread ID: {threading.get_ident()} - Timestamp: {time.time()} - Before important step")

                # Determine next opening cycle sell price
                print("Determining next opening cycle sell order price...")
                open_price_sell = determine_next_open_sell_order_price_with_retry(profit_percent, iterations=0, depth=0, max_iterations=10, max_depth=1000000)
                app_logger.info("Opening cycle sell order price determined: %s", open_price_sell)

                if open_price_sell is not None:
                    # Calculate compounding amount and size for next opening cycle sell order
                    print("Calculating compounding amount and next opening cycle sell order size...")
                    compounding_amount_B_ols, no_compounding_B_limit_ols = calculate_open_limit_sell_compounding_amt_B(total_received_B_clb, total_spent_Q_clb, open_price_sell, base_increment)
                    open_size_B = determine_next_open_size_B_limit(compounding_option, total_received_B_clb, no_compounding_B_limit_ols, compounding_amount_B_ols, compound_percent)
                    app_logger.info("open_size_B: %s", open_size_B)

                if open_price_sell is None:
                    error_logger.error(f"Failed to determine next opening cycle price for next sell order for CycleSet {self.cycleset_sell_buy_number}. Stopping the current cycle set.")
                    return
                
                if open_size_B is not None:
                    # Append the base currency size value to the history list of the current instance
                    self.open_size_B_history.append(open_size_B)

                if open_size_B is None:
                    error_logger.error(f"Unable to determine next opening cycle sell order size for CycleSet {self.cycleset_sell_buy_number}. Stopping the current cycle set.")
                    return
                
                print(f"Thread ID: {threading.get_ident()} - Timestamp: {time.time()} - After important step")

            with self.sell_buy_cycleset_lock:
                print(f"Thread ID: {threading.get_ident()} - Timestamp: {time.time()} - Before important step")

                # Create a Cycle instance
                sell_buy_cycle_instance, self.cycle_sell_buy_number = self.add_sell_buy_cycle(open_size_B, "sell_buy")

                sell_buy_cycle_instance.cycle_sell_buy_number = self.cycle_sell_buy_number
            
                # Add the cycle instance to the list in the CycleSet and Cycle
                self.cycle_sell_buy_instances.append(sell_buy_cycle_instance)
                sell_buy_cycle_instance.cycle_sell_buy_instances.append(sell_buy_cycle_instance)

                print("Starting sell_buy cycle completed.")
                self.sell_buy_logger.info("Opening sell order ID: %s, Closing buy order ID: %s", open_order_id_sell, close_order_id_buy)

                # Pass the new cycle instance to the next iteration of cycle order creation
                app_logger.info(f"Starting next sell_buy cycle, Cycle {sell_buy_cycle_instance.cycle_sell_buy_number} of CycleSet {self.cycleset_sell_buy_number} {self.cycle_type}")
                
                print(f"Thread ID: {threading.get_ident()} - Timestamp: {time.time()} - After important step")

            # Increment the recursion depth counter
            self.recursion_depth += 1

            # Call the next function without handling recursion here
            next_order_ids = self.place_next_sell_buy_cycle_orders(open_size_B, open_price_sell, sell_buy_cycle_instance)
            return open_order_id_sell, close_order_id_buy, next_order_ids
        
        except requests.exceptions.RequestException as e:
            # Handle request exceptions
            error_logger.error(f"An error occurred in place_starting_sell_buy_cycle_orders: {e}")
            error_logger.error(f"Status code: {e.response.status_code}" if hasattr(e, 'response') and e.response is not None else "")
            # Add additional logging to capture specific details
            error_logger.exception("RequestException details:", exc_info=True)
            return None, None, None

        except json.JSONDecodeError as e:
            # Handle JSON decoding errors
            error_logger.error(f"Error decoding JSON response in place_starting_sell_buy_cycle_orders: {e}")
            # Add additional logging to capture specific details
            error_logger.exception("JSONDecodeError details:", exc_info=True)
            return None, None, None

        except Exception as e:
            # Handle other unexpected errors
            error_logger.error(f"Thread ID: {threading.get_ident()} - Timestamp: {time.time()} - An unexpected error occurred in place_starting_sell_buy_cycle_orders: {e}")
            # Add additional logging to capture specific details
            error_logger.exception("Unexpected error details:", exc_info=True)
            return None, None, None 
    
    def start_sell_buy_starting_cycle(self, user_config, sell_buy_cycle_set_counter):
        with sell_buy_cycle_start_lock:
            # Check if there is new user input
            if user_config:
                with print_lock:
                    app_logger.info(f"Cycle Set {sell_buy_cycle_set_counter} (sell_buy) starting cycle initiated...")

                # Call the add_cycle() function to create the Cycle instance
                sell_buy_cycle_instance, self.cycle_sell_buy_number = self.add_sell_buy_cycle(open_size_B=starting_size_B, cycle_type="sell_buy")
            
                # Place starting orders for 'sell-buy' cycle and get order IDs
                sell_buy_order_ids = self.place_starting_sell_buy_cycle_orders(starting_size_B, sell_buy_cycle_instance)

                return sell_buy_cycle_instance, sell_buy_order_ids
            else:
                error_logger.error("Invalid user configuration. CycleSet and Cycle not started.")

    def create_and_start_cycle_set_sell_buy(user_config):
        with sell_buy_lock:
            print("Creating 'sell_buy' cycle set...")
            global sell_buy_cycle_set_counter
            sell_buy_cycle_set_counter += 1

            # Set up logger for the sell-buy cycle set
            sell_buy_logger = setup_cycleset_logger(sell_buy_cycle_set_counter, "sell_buy")

            # Now you can use the sell_buy_logger to log messages
            sell_buy_logger.info(f"Cycle Set {sell_buy_cycle_set_counter} (sell_buy) created and started")
            
            try:
                if "starting_size_B" in definitions:
                    # Create one sell-buy cycle set instance
                    starting_size = definitions["starting_size_B"]
                    cycle_type = "sell_buy"
                    new_cycle_set_sell_buy = CycleSetSellBuy(
                        sell_buy_logger,
                        definitions["base_asset_code"],
                        definitions["base_asset_issuer"],
                        definitions["counter_asset_code"],
                        definitions["counter_asset_issuer"],
                        definitions["pair"],
                        definitions["starting_size_B"],
                        definitions["profit_percent"],
                        definitions["wait_period_unit"],
                        definitions["first_order_wait_period"],
                        definitions["chart_interval"],
                        definitions["interval_label"],
                        definitions["num_intervals"],
                        definitions["window_size"],
                        definitions["compound_percent"],
                        definitions["compounding_option"],
                        definitions["stacking"],
                        definitions["step_price"],
                        definitions["base_asset"],
                        definitions["counter_asset"],
                        cycle_type=cycle_type,
                    )
                    new_cycle_set_sell_buy.cycleset_sell_buy_running = True
                    cycle_sets_sell_buy.append(new_cycle_set_sell_buy)

                    # Create a Cycle instance and pass the parent CycleSet
                    new_cycle_sell_buy = CycleSellBuy(
                        starting_size,
                        new_cycle_set_sell_buy, 
                        "sell_buy",  # Cycle type
                    )

                    new_cycle_set_sell_buy.cycle_sell_buy_instances.append(new_cycle_sell_buy)

                    # Start the sell-buy cycle set
                    new_cycle_set_sell_buy.start_sell_buy_starting_cycle(user_config, sell_buy_cycle_set_counter)
                    
                    return new_cycle_set_sell_buy  # Return the newly created CycleSet and log
                
                else:
                    error_logger.error("Sell-Buy cycle set not created. 'starting_size_B' is missing in user_config.")

            except Exception as e:
                # Handle exceptions or errors
                error_logger.error(f"An error occurred in the main loop: {e}")

    def get_open_orders(self):
        open_orders = [order for order in self.orders if get_order_status(order) == 'OPEN']
        return open_orders
    
    def cancel_open_orders(self, orders_to_cancel):
        cancel_results = cancel_orders(orders_to_cancel)
        return cancel_results
    
    def stop(self):
        with print_lock:
            # Access the last cycle in the set
            if self.cycle_sell_buy_instances:
                last_cycle = self.cycle_sell_buy_instances[-1]
            
                # Get the open orders in the last cycle
                open_orders = last_cycle.get_open_orders()

                if open_orders:
                    # Initiate cancel requests for open orders
                    cancel_results = self.cancel_open_orders(open_orders)  # Use your cancel_orders function
                    if cancel_results:
                        print("Cancel requests initiated successfully.")
                    else:
                        print("Failed to initiate cancel requests.")
            
            self.running = False  # This flag prevents further cycling

    def cycleset_is_running(self):
        return self.cycleset_sell_buy_running
    
    def get_status(self):
        with print_lock:
            if not self.cycle_sell_buy_instances:
                info_logger.info(f"Cycle Set {self.cycleset_sell_buy_number} ({self.cycle_type}) has no completed cycles yet.")
                return
            
            last_cycle = self.cycle_sell_buy_instances[-1]
            
            status_info = [
                f"Cycle Set {self.cycleset_sell_buy_number} ({self.cycle_type}) status: {self.cycleset_sell_buy_status}",
                f"Completed cycles: {last_cycle}",
                f"Cycle {self.cycle_sell_buy_number} status: {self.cycle_sell_buy_status}",
            ]

            info_logger.info("Status info: %s", status_info)
            
            open_orders = last_cycle.get_open_orders()
            if open_orders:
                status_info.append("Current open order:")
                for order in open_orders:
                    status_info.append(f"  - Order ID: {order.order_id}")
                    status_info.append(f"    Order Status: {order.status}")
                    status_info.append(f"    Order Details: {order.details}")
            
            # Join all status information
            status_message = "\n".join(status_info)
            
            info_logger.info("Status message: %s", status_message)

    def get_cycleset_data(self):
        # Calculate and return a dictionary of relevant data
    
        cycleset_sell_buy_data = {
            'cycle_set_sell_buy_number': self.cycleset_sell_buy_number,
            'cycleset_sell_buy_instance_id': self.cycleset_sell_buy_instance_id,
            'product_id': self.product_id,
            'starting_size': self.starting_size,
            'profit_percent': self.profit_percent,
            'taker_fee': self.taker_fee,
            'maker_fee': self.maker_fee,
            'compound_percent': self.compound_percent,
            'compounding_option': self.compounding_option,
            'wait_period_unit': self.wait_period_unit,
            'first_order_wait_period': self.first_order_wait_period,
            'chart_interval': self.chart_interval,
            'num_intervals': self.num_intervals,
            'window_size': self.window_size,
            'stacking': self.stacking,
            'step_price': self.step_price,
            'cycle_type': self.cycle_type,
            'orders': self.sell_buy_orders,
            'cycle_sell_buy_instances': self.cycle_sell_buy_instances,
            'cycle_set_sell_buy_running': self.cycleset_sell_buy_running,
            'cycleset_sell_buy_status': self.cycleset_sell_buy_status,
            'completed_sell_buy_cycles': self.completed_sell_buy_cycles,
            'starting_dollar_value': self.starting_dollar_value,
            'current_dollar_value': self.current_dollar_value,
            'percent_gain_loss_dollar': self.percent_gain_loss_dollar,
            'percent_gain_loss_base': self.percent_gain_loss_base,
            'percent_gain_loss_quote': self.percent_gain_loss_quote,
            'average_profit_percent_per_hour': self.average_profit_percent_per_hour,
            'average_profit_percent_per_day': self.average_profit_percent_per_day
            # ... other data
        }
    
        self.sell_buy_logger.info("Cycle set data: %s", cycleset_sell_buy_data)
        return cycleset_sell_buy_data

class CycleSellBuy:
    # Class attribute to store the count of instances
    count = 0

    # Class attribute to store all created cycle set instances
    cycle_sell_buy_instances = []

    def __init__(
            self, 
            open_size_B,
            cycle_set_sell_buy_instance,
            cycle_type,
        ):

        # Initialize cycle counts for sell_buy
        cycle_type == 'sell_buy'
        CycleSellBuy.sell_buy_cycle_count = 0  # Reset to '0' for a new CycleSet instance
        self.cycle_sell_buy_number = CycleSellBuy.sell_buy_cycle_count + 1

        self.open_size = open_size_B 
        self.cycle_set_sell_buy_instance = cycle_set_sell_buy_instance
        self.cycle_type = "sell_buy"
        self.cycle_sell_buy_instance_id = f"Cycle (sell_buy) cycle_number: {self.cycle_sell_buy_number}"
        self.cycle_sell_buy_running = False  # Initialize the cycle running attribute
        self.cycle_sell_buy_status = "Pending" # Can have statuses: 'Pending', 'Pending-Opening Sell Order', 'Active-Opening Sell Order', 'Pending-Closing Buy Order', 'Active-Closing Buy Order', 'Pending-Opening Buy Order', 'Active-Opening Buy Order', 'Pending-Closing Sell Order', 'Active Closing Sell Order', or 'Completed'
        self.sell_buy_orders = []  # Initialize as an empty list
        self.sell_buy_cycle_lock = threading.Lock()

    # Other methods...


    def check_order_status(self, order_id):
        order_data = get_order_status(order_id)
        if order_data:
            order_status = order_data.get("status")
            return order_status
        else:
            # Handle the case when the API request fails
            return "API request failed"
    def get_open_orders(self):
        open_orders = [order_id for order_id in self.sell_buy_orders if get_order_status(order_id) == 'OPEN']
        return open_orders
    
    def cancel_open_orders(self, open_orders):
        cancel_results = cancel_orders(open_orders)
        return cancel_results

    def cycle_is_running(self):
        # Check if a cycle is running
        for order_id in self.sell_buy_orders:
            order_status = get_order_status(order_id)  # Implement this function to get order status
            if order_status != "CANCELLED":
                self.cycle_sell_buy_running = True
            else:
                self.cycle_sell_buy_running = False

# Access the instance_id to identify instances
for cycleset_sell_buy_instance in CycleSetSellBuy.cycleset_sell_buy_instances:
    app_logger.info(f"Cycle Set (sell_buy) Instance ID: {cycleset_sell_buy_instance.cycleset_sell_buy_instance_id}, Product ID: {cycleset_sell_buy_instance.product_id}, Starting Size: {cycleset_sell_buy_instance.starting_size}, Profit Percent: {cycleset_sell_buy_instance.profit_percent}, Taker Fee: {cycleset_sell_buy_instance.taker_fee}, Maker Fee: {cycleset_sell_buy_instance.maker_fee}, Compound Percent: {cycleset_sell_buy_instance.compound_percent}, Compounding Option: {cycleset_sell_buy_instance.compounding_option}, Wait Period Unit: {cycleset_sell_buy_instance.wait_period_unit}, First Order Wait Period: {cycleset_sell_buy_instance.first_order_wait_period}, Chart Intervals: {cycleset_sell_buy_instance.chart_intervals}, Number Intervals: {cycleset_sell_buy_instance.num_intervals}, Window Size: {cycleset_sell_buy_instance.window_size}, Stacking: {cycleset_sell_buy_instance.stacking}, Step Price: {cycleset_sell_buy_instance.step_price}, Cycle Type: {cycleset_sell_buy_instance.cycle_type}")

