# stellar_endpoint_data.py

from logging_config import log_to_file
from stellar_sdk import Server, Asset

def fetch_verbose_data(endpoint, **kwargs):
    server = Server("https://horizon.stellar.org")
    response = None

    try:
        if endpoint == "trades":
            # Expecting Asset objects directly
            response = server.trades().for_asset_pair(kwargs['base_asset'], kwargs['counter_asset']).limit(10).order("desc").call()
        elif endpoint == "offers":
            response = server.offers().for_account(kwargs['account_id']).limit(10).call()
        elif endpoint == "accounts":
            response = server.accounts().account_id(kwargs['account_id']).call()
        elif endpoint == "transactions":
            response = server.transactions().for_account(kwargs['account_id']).limit(10).call()
        elif endpoint == "payments":
            response = server.payments().for_account(kwargs['account_id']).limit(10).call()
        elif endpoint == "liquidity_pools":
            response = server.liquidity_pools().liquidity_pool_id(kwargs['liquidity_pool_id']).call()
        # You can add more elif blocks for other endpoints like "operations", etc.
        else:
            print(f"Unsupported endpoint: {endpoint}")
            return

        # Log detailed info to file and print summary to console
        log_to_file(f"Endpoint: {endpoint}, Data: {response}")
        print(f"Fetched data from endpoint: {endpoint}")

    except Exception as e:
        log_to_file(f"Error fetching data from endpoint: {endpoint}, Error: {str(e)}")
        print(f"Error fetching data from endpoint: {endpoint}. Check log for details.")


    return response

# Dynamic input collection based on chosen endpoint
endpoint = input("Enter the endpoint you wish to get data for: ")

kwargs = {}
if endpoint in ["trades", "offers"]:
    base_asset_code = input("Enter the base asset code: ")
    base_asset_issuer = input("Enter the base asset issuer (or 'native' for XLM): ")
    counter_asset_code = input("Enter the counter asset code: ")
    counter_asset_issuer = input("Enter the counter asset issuer (or 'native' for XLM): ")

    # Create Asset objects correctly
    base_asset = Asset.native() if base_asset_code.upper() == "XLM" else Asset(base_asset_code, base_asset_issuer)
    counter_asset = Asset.native() if counter_asset_code.upper() == "XLM" else Asset(counter_asset_code, counter_asset_issuer)

    kwargs = {
        'base_asset': base_asset,
        'counter_asset': counter_asset,
    }
if endpoint in ["offers", "accounts", "transactions", "payments"]:  # Extend as necessary
    account_id = input("Enter the account ID (public address) you wish to get data for: ")
    kwargs['account_id'] = account_id
if endpoint == "liquidity_pools":
    liquidity_pool_id = input("Enter the liquidity pool ID of the LP you wish to get data for: ")
    kwargs['liquidity_pool_id'] = liquidity_pool_id

fetch_verbose_data(endpoint, **kwargs)