# cycle_set_init.py

import requests
from config import secret_key
from starting_input import get_user_starting_input
from stellar_sdk import Asset, Server, Keypair, Network, exceptions
from stellar_db import get_db_connection, fetch_trading_pairs_from_db

def create_or_select_account_for_testnet():
    # Ask the user if they want to use an existing account or create a new one
    choice = input("Do you want to use an existing testnet account or create a new one? (existing/new): ").lower()
    
    if choice == "new":
        # Generate a new Stellar keypair for the testnet account
        keypair = Keypair.random()
        public_key = keypair.public_key
        print("A new Stellar testnet account has been created.")
        print("Public Key (Address):", public_key)
        print("Secret Key: IMPORTANT! Save this key in a secure location. It will not be shown again.", keypair.secret)
        return public_key
    elif choice == "existing":
        public_key = input("Enter the public key of your existing testnet account: ")
        
        return public_key
    else:
        print("Invalid choice. Exiting.")
        return

def check_funding_and_fund_with_friendbot(public_key):
    server = Server("https://horizon-testnet.stellar.org")
    
    try:
        # Attempt to load the account to check if it exists
        server.load_account(public_key)
        print("Account already exists on the testnet.")
        return True
    except exceptions.NotFoundError:
        # Account does not exist, so attempt to fund it using Friendbot
        friendbot_url = f"https://friendbot.stellar.org?addr={public_key}"
        response = requests.get(friendbot_url)
        if response.status_code == 200:
            print("The test account has been successfully funded with test Lumens (XLM).")
            return True
        else:
            print("Failed to fund the test account using Friendbot. Please check the public key or try again later.")
            return False

def get_definitions():

    # Configuration to determine which network to use
    USE_TESTNET = input("Enter 'True' or 'False' for whether to use Stellar testnet: ").strip().lower() == "true"

    if USE_TESTNET:
        public_key = "GCP5NVA6ASEGGP4PGTOQE5KHM5D2U7YV2R7R7KQMJG72RMS3AG6546MR"
        if public_key:
            check_funding_and_fund_with_friendbot(public_key)
        else:
            # Placeholder for future logic when not using the testnet.
            # TODO: Implement the following logic for selecting or creating a public key when using the live network.
            public_key = create_or_select_account_for_testnet()
            check_funding_and_fund_with_friendbot(public_key)
            # This section will be updated to dynamically handle live network scenarios.
            pass
            

        server = Server("https://horizon-testnet.stellar.org")
        network_passphrase = Network.TESTNET_NETWORK_PASSPHRASE
    else:
        server = Server("https://horizon.stellar.org")
        network_passphrase = Network.PUBLIC_NETWORK_PASSPHRASE

    # Define server
    server = Server("https://horizon.stellar.org")

    # Define Stellar Horizon connection
    conn = get_db_connection()

    # Define user_config
    user_config = get_user_starting_input()

    # Define other necessary parameters
    base_asset_code = user_config["base_asset_code"]
    base_asset_issuer = user_config["base_asset_issuer"]
    counter_asset_code = user_config["counter_asset_code"]
    counter_asset_issuer = user_config["counter_asset_issuer"]
    pair = user_config["pair"]

    trading_pairs = fetch_trading_pairs_from_db(conn)

    for pair in trading_pairs:
        base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer = pair
        pair = base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer
        
    # Create Asset objects correctly
    base_asset = Asset.native() if base_asset_code.upper() == "XLM" else Asset(base_asset_code, base_asset_issuer)
    counter_asset = Asset.native() if counter_asset_code.upper() == "XLM" else Asset(counter_asset_code, counter_asset_issuer)


    interval = user_config["chart_interval"]
    interval_label = f"{interval//60}m" if interval < 3600 else f"{interval//3600}h"

    

    return {
        "server": server,
        "network_passphrase": network_passphrase,
        "conn": conn,
        "secret_key": secret_key, 
        "user_config": user_config, 
        "base_asset_code": user_config["base_asset_code"],
        "base_asset_issuer": user_config["base_asset_issuer"],
        "counter_asset_code": user_config["counter_asset_code"],
        "counter_asset_issuer": user_config["counter_asset_issuer"],
        "pair": pair,
        "starting_size_B": int(user_config["starting_size_B"]),
        "starting_size_Q": int(user_config["starting_size_Q"]),
        "profit_percent": float(user_config["profit_percent"]),
        "wait_period_unit": user_config["wait_period_unit"],
        "first_order_wait_period": user_config["first_order_wait_period"],
        "chart_interval": user_config["chart_interval"],
        "interval_label": interval_label,
        "num_intervals": user_config["num_intervals"],
        "window_size": user_config["window_size"],
        "compound_percent": float(user_config["compound_percent"]),
        "compounding_option": user_config["compounding_option"],
        "stacking": user_config["stacking"],
        "step_price": user_config["step_price"], 
        "base_asset": base_asset,
        "counter_asset": counter_asset,    
    }

definitions = get_definitions()

