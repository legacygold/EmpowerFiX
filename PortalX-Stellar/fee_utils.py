# fee_utils.py

from stellar_sdk import Server

server = Server("https://horizon.stellar.org")
# Fetch the current base fee
base_fee = server.fetch_base_fee()
print(f"Current base fee: {base_fee} stroops")

# Calculate the total fee for a transaction with multiple operations
num_operations = 3
total_fee = base_fee * num_operations
print(f"Total fee for a transaction with {num_operations} operations: {total_fee} stroops")
