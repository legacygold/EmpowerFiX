#orderbook.py
def get_user_order():
    order_type = input("Enter order type (buy/sell): ")
    price = float(input("Enter price: "))
    amount = int(input("Enter amount: "))
    return Order(None, order_type, price, amount)
class Order:
    def __init__(self, orderId, orderType, price, amount):
        self.orderId = orderId
        self.orderType = orderType
        self.price = price
        self.amount = amount

class OrderBook:
    def __init__(self, selling_asset, buying_asset):
        self.buyOrders = []
        self.sellOrders = []
        self.selling_asset = selling_asset
        self.buying_asset = buying_asset
        self.current_price = None  # Initialize current price

    def add_order(self, order):
        if order.orderType == 'buy':
            self.buyOrders.append(order)
            self.buyOrders.sort(key=lambda x: x.price, reverse=True)
        elif order.orderType == 'sell':
            self.sellOrders.append(order)
            self.sellOrders.sort(key=lambda x: x.price)

    def display_order_book(self):
        print("Order Book:")
        print(f"BUY ORDERS ({self.buying_asset} for {self.selling_asset}):")
        print(f"Amount ({self.selling_asset})\tPrice ({self.buying_asset})\tTotal ({self.buying_asset})")
        for order in self.buyOrders:
            total = order.amount * order.price
            print(f"{order.amount}\t{order.price}\t{total}")

        print(f"\nSELL ORDERS ({self.selling_asset} for {self.buying_asset}):")
        print(f"Amount ({self.selling_asset})\tPrice ({self.buying_asset})\tTotal ({self.buying_asset})")
        for order in self.sellOrders:
            total = order.amount * order.price
            print(f"{order.amount}\t{order.price}\t{total}")

        # Display Current Price and Spread
        if self.current_price:
            print(f"Current Price: {self.current_price}")
        if self.buyOrders and self.sellOrders:
            spread = self.sellOrders[0].price - self.buyOrders[0].price
            print(f"Spread: {spread}")

        # Add additional metrics calculations here

    def match_orders(self):
        while self.buyOrders and self.sellOrders:
            topBuy = self.buyOrders[0]
            topSell = self.sellOrders[0]

            if topBuy.price >= topSell.price:
                executed_amount = min(topBuy.amount, topSell.amount)
                executed_price = topSell.price if topBuy.amount >= topSell.amount else topBuy.price
                self.current_price = executed_price

                print(f"Executing trade: {executed_amount} at {executed_price}")

                # Update orders
                topBuy.amount -= executed_amount
                topSell.amount -= executed_amount

                # Remove order if fully executed
                if topBuy.amount <= 0:
                    self.buyOrders.pop(0)
                if topSell.amount <= 0:
                    self.sellOrders.pop(0)

            else:
                break

        self.display_order_book() # Call the display_order_book function after matching orders

# Example Usage for XLM/USDC pair
orderBook = OrderBook('XLM', 'USDC')
orderBook.add_order(Order(1, 'buy', 0.10, 100))
orderBook.add_order(Order(2, 'sell', 0.10, 100))

# Add orders from user input
user_order = get_user_order()
orderBook.add_order(user_order)

orderBook.match_orders()  # Simulate order matching and display order book
