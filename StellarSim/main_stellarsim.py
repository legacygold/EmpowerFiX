# main_stellarsim.py
from stellar_data_retrieval import get_asset_input, fetch_sim_orderbook_data
from orderbook2 import OrderBook, Order
import time

def main():
    # Example: User defines which asset pair they want to simulate
    base_asset, base_asset_issuer = get_asset_input()
    quote_asset, quote_asset_issuer = get_asset_input()

    sim_orderbook = OrderBook(base_asset, quote_asset)  # Initialize simulated order book with dynamic assets

    while True:
        fetch_sim_orderbook_data(sim_orderbook, base_asset, quote_asset, base_asset_issuer, quote_asset_issuer)
        # Further user interaction logic here
        # Display updated order book, handle user actions etc.
        # Consider implementing a sleep interval to manage polling frequency
        time.sleep(60)  # Example polling interval, adjust as needed

if __name__ == "__main__":
    main()

