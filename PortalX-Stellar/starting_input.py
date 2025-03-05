# starting_input.py

import time
import sys
from logging_config import info_logger
from user_input import collect_sell_buy_input, collect_buy_sell_input

def get_user_starting_input():
    while True:
        # Prompt user to choose whether to start a cycle set or skip
        print("Do you want to start a cycle set?")
        print("1. Yes")
        print("2. No (Skip)")
        print("3. Exit")
        choice = int(input("Enter your choice (1, 2, or 3): "))

        if choice == 1:
            # Prompt user to choose the type of cycle set
            print("Choose the type of cycle set to start:")
            print("1. Sell Buy Cycle Set")
            print("2. Buy Sell Cycle Set")
            choice = int(input("Enter your choice (1 or 2): "))

            # Depending on user choice, collect user input for the corresponding cycle set
            if choice == 1:
                user_config_sell_buy = collect_sell_buy_input()
                user_config = user_config_sell_buy
            elif choice == 2:
                user_config_buy_sell = collect_buy_sell_input()
                user_config = user_config_buy_sell

            info_logger.info(user_config)
            
            return user_config
        
        elif choice == 2:
            # If user chooses not to start a cycle set, simply continue looping
            print("No cycle set started. Waiting for user input...")
            time.sleep(60)  # Adjust the sleep duration as needed

        elif choice == 3:
            print("Exiting PortalX...")
            sys.exit()
            
        else:
            print("Invalid choice. Please enter 1, 2, or 3.")
