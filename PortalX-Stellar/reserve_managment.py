# reserve_management.py

from stellar_sdk import Server, Asset

def get_xlm_usdc_exchange_rate():
    server = Server(horizon_url="https://horizon.stellar.org")

    # Define the assets
    xlm_asset = Asset.native()  # XLM is the native asset
    usdc_asset = Asset("USDC", "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN")  # USDC with its issuer

    # Get the orderbook
    orderbook = server.orderbook(selling=usdc_asset, buying=xlm_asset).call()
    asks = orderbook['asks']

    if asks:
        # Taking the lowest ask price as the exchange rate
        exchange_rate = float(asks[0]['price'])
        return exchange_rate
    else:
        return None

# Example usage
exchange_rate = get_xlm_usdc_exchange_rate()
if exchange_rate:
    print(f"Current USDC to XLM exchange rate: {exchange_rate}")
else:
    print("No exchange rate data available.")

