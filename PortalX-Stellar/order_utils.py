#order_utils.py

from stellar_sdk import Keypair, TransactionBuilder, Asset
import time
from logging_config import app_logger, info_logger, error_logger
import requests
from requests.exceptions import RequestException
import http.client
import json
from cycle_set_init import definitions

# Define API credentials
server = definitions["server"]
network_passphrase = definitions["network_passphrase"]
secret_key = definitions["secret_key"]

# Define necessary parameters
base_asset_code = definitions["base_asset_code"]
base_asset_issuer = definitions["base_asset_issuer"]
counter_asset_code = definitions["counter_asset_code"]
counter_asset_issuer = definitions["counter_asset_issuer"]
starting_size_B = definitions["starting_size_B"]
starting_size_Q = definitions["starting_size_Q"]


def fetch_offer_id_from_transaction(server, transaction_id):   
    # Fetch operations for the given transaction_id
    operations = server.operations().for_transaction(transaction_id=transaction_id).call()
    
    for operation in operations['_embedded']['records']:
        # Check if the operation is a manage sell or buy offer
        if operation['type'] == 'manage_sell_offer' or operation['type'] == 'manage_buy_offer':
            return operation.get('id')  # This is the offer_id for the operation
    
    return None

# Function for placing starting (cycle 1) opening cycle sell order(s)
def place_starting_open_sell_order(server, network_passphrase, secret_key, base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, starting_size_B, starting_price_sell):
    print("Loading place_starting_open_sell_order...")

    # Define assets
    if base_asset_code == "XLM" and base_asset_issuer == "native":
        sell_asset = Asset.native()
    else:
        sell_asset = Asset(base_asset_code, base_asset_issuer)
    
    if counter_asset_code == "XLM" and counter_asset_issuer == "native":
        buy_asset = Asset.native()
    else:
        buy_asset = Asset(counter_asset_code, counter_asset_issuer)
    
    # Load account
    source_account = server.load_account(account_id=Keypair.from_secret(secret_key).public_key)

    # Fetch the current base fee from the network
    base_fee = server.fetch_base_fee()

    try:
        # Build transaction
        transaction = (
            TransactionBuilder(
                source_account=source_account,
                network_passphrase=network_passphrase,
                base_fee=base_fee,  # Use dynamic base fee
            )
            .append_manage_sell_offer_op(
                selling_asset=sell_asset,
                buying_asset=buy_asset,
                amount=str(starting_size_B),
                price=str(starting_price_sell),
                offer_id=0  # 0 for new offer
            )
            .set_timeout(30)
            .build()
        )

        # Sign and submit transaction and receive response
        transaction.sign(Keypair.from_secret(secret_key))
        transaction_response = server.submit_transaction(transaction)

        if transaction_response.get("successful", False):
            info_logger.info(transaction_response)
            transaction_id = transaction_response.get("id")
            offer_id = fetch_offer_id_from_transaction(transaction_id)
            
            if offer_id is not None:
                info_logger.info(f"Starting opening cycle sell order placed successfully. Transaction ID: {transaction_id}, Offer ID: {offer_id}")
                return offer_id
            else:
                error_logger.error(f"No offer_id could be fetched for starting opening cycle sell order, Transaction ID: {transaction_id}.")
                return None
        else:
            error_logger.error("Starting opening cycle sell order transaction submission failed.")
            return None
        
    except Exception as e:
        error_logger.error(f"Exception during starting opening cycle sell order transaction submission: {str(e)}")
        return None

# Function for placing starting (cycle 1) opening cycle buy order(s)
def place_starting_open_buy_order(server, network_passphrase, secret_key, base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, starting_size_Q, starting_price_buy):
    print("Loading place_starting_open_buy_order...")

    # Define assets
    if base_asset_code == "XLM" and base_asset_issuer == "native":
        buy_asset = Asset.native()
    else:
        buy_asset = Asset(base_asset_code, base_asset_issuer)
    
    if counter_asset_code == "XLM" and counter_asset_issuer == "native":
        sell_asset = Asset.native()
    else:
        sell_asset = Asset(counter_asset_code, counter_asset_issuer)
    
    # Load account
    source_account = server.load_account(account_id=Keypair.from_secret(secret_key).public_key)

    # Fetch the current base fee from the network
    base_fee = server.fetch_base_fee()

    try:
        # Build transaction
        transaction = (
            TransactionBuilder(
                source_account=source_account,
                network_passphrase=network_passphrase,
                base_fee=base_fee,  # Use dynamic base fee
            )
            .append_manage_buy_offer_op(
                buying_asset=buy_asset,
                selling_asset=sell_asset,
                amount=str(starting_size_Q),
                price=str(starting_price_buy),
                offer_id=0  # 0 for new offer
            )
            .set_timeout(30)
            .build()
        )

        # Sign and submit transaction and receive response
        transaction.sign(Keypair.from_secret(secret_key))
        transaction_response = server.submit_transaction(transaction)

        if transaction_response.get("successful", False):
            info_logger.info(transaction_response)
            transaction_id = transaction_response.get("id")
            offer_id = fetch_offer_id_from_transaction(transaction_id)
            
            if offer_id is not None:
                info_logger.info(f"Starting opening cycle buy order placed successfully. Transaction ID: {transaction_id}, Offer ID: {offer_id}")
                return offer_id
            else:
                error_logger.error(f"No offer_id could be fetched for starting opening cycle buy order, Transaction ID: {transaction_id}.")
                return None
        else:
            error_logger.error("Starting opening cycle buy order transaction submission failed.")
            return None
        
    except Exception as e:
        error_logger.error(f"Exception during starting opening cycle buy order transaction submission: {str(e)}")
        return None
    
# Function for placing next opening cycle sell order(s)
def place_next_opening_cycle_sell_order(server, network_passphrase, secret_key, base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, open_size_B, open_price_sell):
    print("Loading place_next_opening_cycle_sell_order...")

    # Define assets
    if base_asset_code == "XLM" and base_asset_issuer == "native":
        sell_asset = Asset.native()
    else:
        sell_asset = Asset(base_asset_code, base_asset_issuer)
    
    if counter_asset_code == "XLM" and counter_asset_issuer == "native":
        buy_asset = Asset.native()
    else:
        buy_asset = Asset(counter_asset_code, counter_asset_issuer)
    
    # Load account
    source_account = server.load_account(account_id=Keypair.from_secret(secret_key).public_key)

    # Fetch the current base fee from the network
    base_fee = server.fetch_base_fee()

    try:
        # Build transaction
        transaction = (
            TransactionBuilder(
                source_account=source_account,
                network_passphrase=network_passphrase,
                base_fee=base_fee,  # Use dynamic base fee
            )
            .append_manage_sell_offer_op(
                selling_asset=sell_asset,
                buying_asset=buy_asset,
                amount=str(open_size_B),
                price=str(open_price_sell),
                offer_id=0  # 0 for new offer
            )
            .set_timeout(30)
            .build()
        )

        # Sign and submit transaction and receive response
        transaction.sign(Keypair.from_secret(secret_key))
        transaction_response = server.submit_transaction(transaction)

        if transaction_response.get("successful", False):
            info_logger.info(transaction_response)
            transaction_id = transaction_response.get("id")
            offer_id = fetch_offer_id_from_transaction(transaction_id)
            
            if offer_id is not None:
                info_logger.info(f"Opening cycle sell order placed successfully. Transaction ID: {transaction_id}, Offer ID: {offer_id}")
                return offer_id
            else:
                error_logger.error(f"No offer_id could be fetched for opening cycle sell order, Transaction ID: {transaction_id}.")
                return None
        else:
            error_logger.error("Opening cycle sell order transaction submission failed.")
            return None
        
    except Exception as e:
        error_logger.error(f"Exception during opening cycle sell order transaction submission: {str(e)}")
        return None
    
# Function for placing next opening cycle buy order(s)
def place_next_opening_cycle_buy_order(server, network_passphrase, secret_key, base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, open_size_Q, open_price_buy):
    print("Loading place_next_opening_cycle_buy_order...")

    # Define assets
    if base_asset_code == "XLM" and base_asset_issuer == "native":
        buy_asset = Asset.native()
    else:
        buy_asset = Asset(base_asset_code, base_asset_issuer)
    
    if counter_asset_code == "XLM" and counter_asset_issuer == "native":
        sell_asset = Asset.native()
    else:
        sell_asset = Asset(counter_asset_code, counter_asset_issuer)
    
    # Load account
    source_account = server.load_account(account_id=Keypair.from_secret(secret_key).public_key)

    # Fetch the current base fee from the network
    base_fee = server.fetch_base_fee()

    try:
        # Build transaction
        transaction = (
            TransactionBuilder(
                source_account=source_account,
                network_passphrase=network_passphrase,
                base_fee=base_fee,  # Use dynamic base fee
            )
            .append_manage_buy_offer_op(
                buying_asset=buy_asset,
                selling_asset=sell_asset,
                amount=str(open_size_Q),
                price=str(open_price_buy),
                offer_id=0  # 0 for new offer
            )
            .set_timeout(30)
            .build()
        )

        # Sign and submit transaction and receive response
        transaction.sign(Keypair.from_secret(secret_key))
        transaction_response = server.submit_transaction(transaction)

        if transaction_response.get("successful", False):
            info_logger.info(transaction_response)
            transaction_id = transaction_response.get("id")
            offer_id = fetch_offer_id_from_transaction(transaction_id)
            
            if offer_id is not None:
                info_logger.info(f"Opening cycle buy order placed successfully. Transaction ID: {transaction_id}, Offer ID: {offer_id}")
                return offer_id
            else:
                error_logger.error(f"No offer_id could be fetched for opening cycle buy order, Transaction ID: {transaction_id}.")
                return None
        else:
            error_logger.error("Opening cycle buy order transaction submission failed.")
            return None
        
    except Exception as e:
        error_logger.error(f"Exception during opening cycle buy order transaction submission: {str(e)}")
        return None

# Function for placing next closing cycle buy order(s)
def place_next_closing_cycle_buy_order(server, network_passphrase, secret_key, base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, close_size_Q, close_price_buy):
    print("Loading place_next_closing_cycle_buy_order...")

    # Define assets
    if base_asset_code == "XLM" and base_asset_issuer == "native":
        buy_asset = Asset.native()
    else:
        buy_asset = Asset(base_asset_code, base_asset_issuer)
    
    if counter_asset_code == "XLM" and counter_asset_issuer == "native":
        sell_asset = Asset.native()
    else:
        sell_asset = Asset(counter_asset_code, counter_asset_issuer)
    
    # Load account
    source_account = server.load_account(account_id=Keypair.from_secret(secret_key).public_key)

    # Fetch the current base fee from the network
    base_fee = server.fetch_base_fee()

    try:
        # Build transaction
        transaction = (
            TransactionBuilder(
                source_account=source_account,
                network_passphrase=network_passphrase,
                base_fee=base_fee,  # Use dynamic base fee
            )
            .append_manage_buy_offer_op(
                buying_asset=buy_asset,
                selling_asset=sell_asset,
                amount=str(close_size_Q),
                price=str(close_price_buy),
                offer_id=0  # 0 for new offer
            )
            .set_timeout(30)
            .build()
        )

        # Sign and submit transaction and receive response
        transaction.sign(Keypair.from_secret(secret_key))
        transaction_response = server.submit_transaction(transaction)

        if transaction_response.get("successful", False):
            info_logger.info(transaction_response)
            transaction_id = transaction_response.get("id")
            offer_id = fetch_offer_id_from_transaction(transaction_id)
            
            if offer_id is not None:
                info_logger.info(f"Closing cycle buy order placed successfully. Transaction ID: {transaction_id}, Offer ID: {offer_id}")
                return offer_id
            else:
                error_logger.error(f"No offer_id could be fetched for closing cycle buy order, Transaction ID: {transaction_id}.")
                return None
        else:
            error_logger.error("Closing cycle buy order transaction submission failed.")
            return None
        
    except Exception as e:
        error_logger.error(f"Exception during closing cycle buy order transaction submission: {str(e)}")
        return None

# Function for placing next closing cycle sell order(s)
def place_next_closing_cycle_sell_order(server, network_passphrase, secret_key, base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, close_size_B, close_price_sell):
    print("Loading place_next_closing_cycle_sell_order...")

    # Define assets
    if base_asset_code == "XLM" and base_asset_issuer == "native":
        sell_asset = Asset.native()
    else:
        sell_asset = Asset(base_asset_code, base_asset_issuer)
    
    if counter_asset_code == "XLM" and counter_asset_issuer == "native":
        buy_asset = Asset.native()
    else:
        buy_asset = Asset(counter_asset_code, counter_asset_issuer)
    
    # Load account
    source_account = server.load_account(account_id=Keypair.from_secret(secret_key).public_key)

    # Fetch the current base fee from the network
    base_fee = server.fetch_base_fee()

    try:
        # Build transaction
        transaction = (
            TransactionBuilder(
                source_account=source_account,
                network_passphrase=network_passphrase,
                base_fee=base_fee,  # Use dynamic base fee
            )
            .append_manage_sell_offer_op(
                selling_asset=sell_asset,
                buying_asset=buy_asset,
                amount=str(close_size_B),
                price=str(close_price_sell),
                offer_id=0  # 0 for new offer
            )
            .set_timeout(30)
            .build()
        )

        # Sign and submit transaction and receive response
        transaction.sign(Keypair.from_secret(secret_key))
        transaction_response = server.submit_transaction(transaction)

        if transaction_response.get("successful", False):
            info_logger.info(transaction_response)
            transaction_id = transaction_response.get("id")
            offer_id = fetch_offer_id_from_transaction(transaction_id)
            
            if offer_id is not None:
                info_logger.info(f"Closing cycle sell order placed successfully. Transaction ID: {transaction_id}, Offer ID: {offer_id}")
                return offer_id
            else:
                error_logger.error(f"No offer_id could be fetched for closing cycle sell order, Transaction ID: {transaction_id}.")
                return None
        else:
            error_logger.error("Closing cycle sell order transaction submission failed.")
            return None
        
    except Exception as e:
        error_logger.error(f"Exception during closing cycle sell order transaction submission: {str(e)}")
        return None
    
def waiting_period_conditions(unit, interval):
    print("Loading waiting_period_conditions...")
    if unit == 'minutes':
        waiting_period = interval * 60
    elif unit == 'hours':
        waiting_period = interval * 3600
    elif unit == 'days':
        waiting_period = interval * 86400
    else:
        raise ValueError("Invalid unit. Please use 'minutes', 'hours', or 'days'.")

    start_time = time.time()
    elapsed_time = 0

    while elapsed_time < waiting_period:
        # Check elapsed time
        elapsed_time = time.time() - start_time

        # Add a sleep to avoid constant checking and reduce CPU usage
        time.sleep(1)  # Sleep for 1 second between checks

    return waiting_period, elapsed_time

def retry_request(func, max_retries=3, initial_delay=5):
    attempts = 0

    while attempts < max_retries:
        try:
            return func()
        except RequestException as e:
            print(f"An error occurred: {e}")
            print(f"Retry attempt {attempts + 1}/{max_retries}")
            time.sleep(initial_delay * (2 ** attempts))
            attempts += 1

    print("Maximum retry attempts reached. Exiting.")
    exit(1)

# Now, you can use waiting_period and elapsed_time as needed in your logic.
# Once waiting_period is reached, you can initiate secondary logic.

def get_order_details(conn, api_key, api_secret, order_id, max_retries):
    
    # Retry logic with exponential backoff
    retries = 0
    while True:
        try:
            conn.request("GET", f"/api/v3/brokerage/orders/historical/{order_id}")
            res = conn.getresponse()
            data = res.read()

            if not data:
                error_logger.error("Empty response in get_order_details.")
                error_logger.error(f"Status code: {res.status}")
                raise RequestException("Empty response")

            return json.loads(data.decode("utf-8"))

        except RequestException as e:
            error_logger.error(f"An error occurred in get_order_details: {e}")
            error_logger.error(f"Status code: {res.status}")

            if retries >= max_retries:
                error_logger.error("Max retries reached in get_order_details. Automation process disrupted. Orders may need manual handling.")
                print("Max retries reached in get_order_details. Automation process disrupted. Orders may need manual handling.")
                return None

            retries += 1
            wait_time = 5 * (2 ** retries)
            time.sleep(wait_time)

def handle_timeout():
    print("Timeout occurred. No response received within the specified time.")
    # Add any additional actions you want to take when a timeout occurs

def wait_for_order(api_key, api_secret, order_id, max_retries=3, timeout=600):
    conn = http.client.HTTPSConnection("api.coinbase.com")

    # Initial request to get order details with retry logic
    order_details = retry_request(lambda: get_order_details(conn, api_key, api_secret, order_id, max_retries), max_retries, initial_delay=5)

    # Print the order details if available
    if order_details is not None and "order" in order_details:
        app_logger.info("Initial order details: %s", order_details["order"])
        print("Waiting for order to fill...")
    else:
        app_logger.info("Initial order details not found.")

    while True:
        try:
            # Resend the request to get the latest order details
            order_details = get_order_details(conn, api_key, api_secret, order_id, max_retries)

            if order_details is None:
                continue  # Retry if empty response is encountered

            # Check if the "order" key exists in the order_details dictionary
            if "order" in order_details and "status" in order_details["order"]:
                # Check if the order status is FILLED and completion percentage is 100
                if (
                    order_details["order"]["status"] == "FILLED"
                    and float(order_details["order"].get("completion_percentage", 0)) == 100
                ):
                    app_logger.info("Current order status: %s", order_details["order"]["status"])
                    app_logger.info("Order filled successfully. Order details: %s", order_details)
                    return order_details
                else:
                    # If the order is not filled yet or completion percentage is not 100, wait and check again
                    time.sleep(10)
            else:
                # If the "order" or "status" key is not found, log the error and retry
                error_logger.error("Order status not found.")
                time.sleep(10)  # Add a delay before retrying

        except Exception as e:
            error_logger.error(f"An error occurred in wait_for_order: {e}")
            time.sleep(10)  # Add a delay before retrying

def get_order_status(order_id):
    url = f"https://api.coinbase.com/api/v3/brokerage/orders/historical/{order_id}"
    response = requests.get(url)

    if response.status_code == 200:
        order_data = response.json()
        return order_data.get("order", {})
    else:
        # Handle API request failure
        return None
    
def cancel_orders(order_ids):
    url = "https://api.coinbase.com/api/v3/brokerage/orders/batch_cancel"
    payload = {
        "order_ids": order_ids
    }
    headers = {
        "Content-Type": "application/json"
    }

    response = requests.post(url, json=payload, headers=headers)

    if response.status_code == 200:
        cancel_results = response.json().get("results", [])
        return cancel_results
    else:
        # Handle API request failure
        return None
