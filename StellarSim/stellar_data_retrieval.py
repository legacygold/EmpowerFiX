# stellar_data_retrieval.py
from stellar_sdk import Server, Asset
from orderbook2 import Order, OrderBook

server = Server("https://horizon.stellar.org")

def initialize_orderbooks(market_pairs):
    """
    Initialize OrderBook instances for each market pair.
    """
    orderbooks = {}
    for pair in market_pairs:
        asset_code_base, asset_code_quote = pair.split("-")
        orderbooks[pair] = OrderBook(asset_code_base, asset_code_quote)
    return orderbooks

def fetch_and_update_orderbook(orderbook, base_asset_code, quote_asset_code, base_issuer=None, quote_issuer=None):
    """
    Fetch offers from the Stellar mainnet and update the given orderbook.
    This function needs to implement the logic to fetch offers, match them, and then add to the orderbook.
    """
    pass  # Implement fetching, matching, and updating logic here

def get_asset_input():
    """
    Dynamically gets user input to determine the asset to be used.
    For XLM, the issuer is not needed. For custom assets, the issuer is required.
    """
    asset_code = input("Enter the asset code (e.g., 'XLM', 'USD', etc.): ").upper()
    if asset_code == "XLM":
        return Asset.native()
    else:
        issuer = input("Enter the issuer's account ID for the asset: ")
        if not issuer:
            raise ValueError("Issuer must be provided for custom assets.")
        return Asset(asset_code, issuer)  

def get_offers_for_selling(asset_code, issuer=None):
    asset = Asset.native() if asset_code == "XLM" else Asset(asset_code, issuer)
    offers_selling = server.offers().for_selling(asset).limit(200).call()
    print(offers_selling)
    return offers_selling["records"]

def get_offers_for_buying(asset_code, issuer=None):
    asset = Asset.native() if asset_code == "XLM" else Asset(asset_code, issuer)
    offers_buying = server.offers().for_buying(asset).limit(200).call()
    print(offers_buying)
    return offers_buying["records"]
    
def match_offers(offers_selling, offers_buying):
    matched_offers = []
    buying_offer_ids = {offer['id'] for offer in offers_buying}
    for offer in offers_selling:
        if offer['id'] in buying_offer_ids:
            matched_offers.append(offer)
    return matched_offers

def add_matched_offers_to_orderbook(sim_orderbook, matched_offers, order_type, selling_asset, buying_asset):
    for offer in matched_offers:
        order = Order(offer_id=offer['id'], orderType=order_type, price=offer['price'], amount=offer['amount'], selling_asset=selling_asset, buying_asset=buying_asset, is_user_order=False)
        sim_orderbook.add_order(order)

def fetch_sim_orderbook_data(sim_orderbook, asset_code_base, asset_code_quote, issuer_base=None, issuer_quote=None):
    offers_selling_base = get_offers_for_selling(asset_code_base, issuer_base)
    offers_buying_quote = get_offers_for_buying(asset_code_quote, issuer_quote)
    matched_offers_asks = match_offers(offers_selling_base, offers_buying_quote)
    add_matched_offers_to_orderbook(sim_orderbook, matched_offers_asks, 'sell', asset_code_base, asset_code_quote)

    # Repeat for buying offers and bids
    offers_selling_quote = get_offers_for_selling(asset_code_quote, issuer_quote)
    offers_buying_base = get_offers_for_buying(asset_code_base, issuer_base)
    matched_offers_bids = match_offers(offers_selling_quote, offers_buying_base)
    add_matched_offers_to_orderbook(sim_orderbook, matched_offers_bids, 'buy', asset_code_base, asset_code_quote)

def main():
    # Define market pairs you're interested in
    market_pairs = ["XLM-USDC", "yXLM-yUSDC", "AFR-XLM","AFR-USDC", "AFR-yXLM", "AFR-yUSDC"]  # Example market pairs

    # Initialize OrderBooks for each market
    orderbooks = initialize_orderbooks(market_pairs)

    # Example loop to update each OrderBook
    for pair, sim_orderbook in orderbooks.items():
        asset_code_base, asset_code_quote = pair.split("-")
        fetch_and_update_orderbook(sim_orderbook, asset_code_base, asset_code_quote)  # You'll need to adjust parameters based on your actual function

    # Here, you can implement logic to periodically update orderbooks, user interactions, etc.

if __name__ == "__main__":
    main()
