#orderbook2.py
import uuid

class Order:
    def __init__(self, orderId, orderType, price, amount, selling_asset, buying_asset, is_user_order=False):
        self.orderId = orderId
        self.orderType = orderType
        self.price = price
        self.amount = amount
        self.selling_asset = selling_asset
        self.buying_asset = buying_asset
        self.is_user_order = is_user_order
        self.unique_id = str(uuid.uuid4())  # Generate a unique identifier for the order

    def __str__(self):
        return f"{self.selling_asset}/{self.buying_asset} {self.orderType} order: {self.amount} at {self.price}, Order ID: {self.orderId}, Unique ID: {self.unique_id}"

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
        else:  # Assuming orderType is 'sell'
            self.sellOrders.append(order)
            self.sellOrders.sort(key=lambda x: x.price)
        print(f"Order added: {order}")

    def match_orders(self):
        trade_executed = False  # Flag to track if a trade was executed
        while self.buyOrders and self.sellOrders and self.buyOrders[0].price >= self.sellOrders[0].price:
            topBuy = self.buyOrders[0]
            topSell = self.sellOrders[0]
            executed_price = topSell.price  # Execute at the sell order's price
            executed_amount = min(topBuy.amount, topSell.amount)

            # Print details of the trade execution before the actual logic
            print(f"Executing trade: Buy {executed_amount} {self.selling_asset} at {executed_price} {self.buying_asset} per {self.selling_asset}")

            topBuy.amount -= executed_amount
            topSell.amount -= executed_amount
            self.current_price = executed_price
            trade_executed = True  # Set flag to true as a trade was executed

            if topBuy.amount == 0:
                self.buyOrders.pop(0)
            if topSell.amount == 0:
                self.sellOrders.pop(0)

        if trade_executed:
            # This statement confirms the trade has been executed, useful for debugging and user feedback
            print(f"Trade executed: {executed_amount} {self.selling_asset} bought at {executed_price} {self.buying_asset} per {self.selling_asset}")

    # Display_user_orders to filter only user orders
    def display_user_orders(self):
        print("Your Orders:")
        order_number = 1
        for order in self.buyOrders + self.sellOrders:
            if order.is_user_order:
                print(f"{order_number}. {order.orderType} {order.amount} {order.selling_asset} at {order.price} {order.buying_asset} per {order.selling_asset}")
                order_number += 1
    
    def display_order_book(self):
        print("Order Book:")

        print(f"SELL ORDERS ({self.selling_asset} for {self.buying_asset})")
        print(f"Amount Selling ({self.selling_asset})\tPrice ({self.buying_asset})\tTotal Received ({self.buying_asset})")
        for order in reversed(self.sellOrders):  # Assuming sellOrders are sorted in ascending order
            total = order.amount * order.price
            print(f"{order.amount}      \t{order.price}     \t{total}")

        if self.current_price:
            spread = 0
            if self.buyOrders and self.sellOrders:
                spread = round((self.sellOrders[0].price - self.buyOrders[0].price) / self.buyOrders[0].price * 100, 2)
            print(f"Current Price: {self.current_price}              Spread: {spread}%") # Display Current Price and Spread

        for order in self.buyOrders:  # Reversing the buyOrders list to display in descending order
            total = order.amount * order.price
            print(f"{order.amount}      \t{order.price}     \t{total}")
        print(f"Amount Buying ({self.selling_asset})\tPrice ({self.buying_asset})\tTotal Spent ({self.buying_asset})")
        print(f"BUY ORDERS ({self.buying_asset} for {self.selling_asset})")

        # Display user's orders
        self.display_user_orders()
        
    def update_order(self, unique_id, new_price, new_amount):
        updated = False
        for orderList in [self.buyOrders, self.sellOrders]:
            for order in orderList:
                if order.unique_id == unique_id:
                    order.price = new_price
                    order.amount = new_amount
                    updated = True
                    break  # Breaks the inner loop once the order is found and updated
            if updated:
                # Re-sort the specific list where the order was found and updated
                orderList.sort(key=lambda x: x.price, reverse=(orderList == self.buyOrders))
                print(f"Order {unique_id} updated successfully.")
                break  # Breaks the outer loop since the order has been found and updated
        if not updated:
            print(f"Order {unique_id} not found.")

    def cancel_order(self, unique_id):
        # Find and remove the order by orderId
        for orderList in [self.buyOrders, self.sellOrders]:
            for i, order in enumerate(orderList):
                if order.unique_id == unique_id:
                    del orderList[i]
                    print(f"Order {unique_id} cancelled successfully")
                    return 
        return f"Order {unique_id} not found"

    def find_order_by_number(self, number):
        all_orders = self.buyOrders + self.sellOrders
        if 0 < number <= len(all_orders):
            return all_orders[number - 1]
        return None
    
    # Function to handle the process of editing or cancelling an order
    def edit_or_cancel_order(self):
        decision = input("Would you like to edit or cancel an order? (yes/no): ").lower()
        if decision == 'yes':
            self.display_user_orders()
            order_number = int(input("Enter the number of the order to edit or cancel: "))
            selected_order = self.find_order_by_number(order_number)
            if selected_order:
                action = input("Do you want to edit (e) or cancel (c) this order? ")
                if action.lower() == 'e':
                    new_price = float(input("Enter new price: "))
                    new_amount = int(input("Enter new amount: "))
                    self.update_order(selected_order.unique_id, new_price, new_amount)
                    self.display_order_book()
                elif action.lower() == 'c':
                    self.cancel_order(selected_order.unique_id)
                    self.display_order_book()
            else:
                print("Invalid order number.")
        elif decision != 'no':
            print("Invalid response.")

def is_number(s):
    try:
        float(s)  # for int, long and float
    except ValueError:
        return False

    return True

def get_user_order():
    while True:
        orderBook.edit_or_cancel_order()
        order_type = input("Enter order type (buy/sell) or press 'Enter' to edit or cancel and order: ").lower()
        if order_type not in ['buy', 'sell']:
            print("Invalid order type. Please enter 'buy' or 'sell' or proceed to edit or cancel an order.")
            continue

        selling_asset = input("Enter selling asset: ")
        if is_number(selling_asset):
            confirmation = input("Are you sure this is the asset name? (yes/no): ").lower()
            if confirmation != 'yes':
                continue

        buying_asset = input("Enter buying asset: ")
        if is_number(buying_asset):
            confirmation = input("Are you sure this is the asset name? (yes/no): ").lower()
            if confirmation != 'yes':
                continue
        
        price = float(input("Enter price: "))
        amount = int(input("Enter amount: "))
        return Order(None, order_type, price, amount, selling_asset, buying_asset, is_user_order=True)

# Main loop
def main():    
    while True:
        # Display the current state of the order book
        orderBook.display_order_book()

        # Get new order from user
        user_order = get_user_order()
        orderBook.add_order(user_order)

        # Match and update orders in the order book
        orderBook.match_orders()

if __name__ == "__main__":
    selling_asset = input("Enter selling asset: ")
    buying_asset = input("Enter buying asset: ")
    orderBook = OrderBook(selling_asset, buying_asset)
    main()