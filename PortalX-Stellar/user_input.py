# user_input.py


def collect_user_input():
    print("User input function for Stellar DEX called")
    
    base_asset_code = input("Enter the base asset code: ")
    base_asset_issuer = input("Enter the base asset issuer (or 'native' for native XLM): ")
    counter_asset_code = input("Enter the quote asset code: ")
    counter_asset_issuer = input("Enter the quote asset issuer (or 'native' for native XLM): ")
    pair = input("Enter the trading pair with base asset first and quote asset second (e.g. XLM-USDC):")
    starting_size_B = float(input("Enter the starting size of the base currency to trade with: "))
    starting_size_Q = float(input("Enter the starting size of the quote currency to trade with: "))
    profit_percent = float(input("Enter the desired profit percentage for each trade: "))
    compound_percent = float(input("Enter the desired compounding percentage: "))
    compounding_option = input("Enter the compounding option (e.g., '100' for full compounding, 'partial' for partial compounding): ")
    wait_period_unit = input("Enter units for wait period interval ('minutes', 'hours', 'days'): ")
    first_order_wait_period = int(input("Enter the wait period interval in {} for the first order: ".format(wait_period_unit)))

    # Administrator inputs
    chart_interval = int(input("Enter the chart interval in seconds for indicator calculations: "))
    num_intervals = int(input("Enter the number of intervals for historical data fetch: "))
    window_size = int(input("Enter the window size for calculating Bollinger Bands: "))
    stacking = input("Enter 'True' or 'False' for whether to place multiple stacked price orders:")
    step_price = input("Enter 'True' or 'False' for whether to step up or down order prices based on market fluctuations:" )
    
    # Other inputs remain similar but adjusted for Stellar's context
    
    user_config = {
        "base_asset_code": base_asset_code,
        "base_asset_issuer": base_asset_issuer if base_asset_issuer.lower() != "native" else None,
        "counter_asset_code": counter_asset_code,
        "counter_asset_issuer": counter_asset_issuer if counter_asset_issuer.lower() != "native" else None,
        "pair": pair,
        "starting_size_B": starting_size_B,
        "starting_size_Q": starting_size_Q,
        "profit_percent": profit_percent,
        "compound_percent": compound_percent,
        "compounding_option": compounding_option,
        "wait_period_unit": wait_period_unit,
        "first_order_wait_period": first_order_wait_period,
        "chart_interval": chart_interval,
        "num_intervals": num_intervals,
        "window_size": window_size,
        "stacking": stacking,
        "step_price": step_price,
        # Include other parameters as needed
    }
    
    return user_config

def collect_sell_buy_input():
    print("Sell Buy Cycle Set Input")
    base_asset_code = input("Enter the base asset code: ")
    base_asset_issuer = input("Enter the base asset issuer (or 'native' for native XLM): ")
    counter_asset_code = input("Enter the quote asset code: ")
    counter_asset_issuer = input("Enter the quote asset issuer (or 'native' for native XLM): ")
    pair = input("Enter the trading pair with base asset first and quote asset second (e.g. XLM-USDC):")
    starting_size_B = float(input("Enter the starting size of the base currency to trade with: "))
    profit_percent = float(input("Enter the desired profit percentage for each cycle: "))
    compound_percent = float(input("Enter the desired compounding percentage: "))
    compounding_option = input("Enter the compounding option (e.g., '100' for full compounding, 'partial' for partial compounding): ")
    wait_period_unit = input("Enter units for wait period interval ('minutes', 'hours', 'days'): ")
    first_order_wait_period = int(input("Enter the wait period interval in {} for the first order: ".format(wait_period_unit)))
    
    # Administrator: Enter Bollinger band calculation parameters
    chart_interval = int(input("Enter the chart interval (in seconds) you wish to trade in (e.g., 60 for a 1-minute chart): "))
    num_intervals = int(input("Enter the number of intervals you want to fetch: "))
    window_size = int(input("Enter the window size for calculating Bollinger Bands: "))
    stacking = input("Enter 'True' or 'False' for whether to place multiple stacked price orders:")
    step_price = input("Enter 'True' or 'False' for whether to step up or down order prices based on market fluctuations:" )

    user_config = {
        "base_asset_code": base_asset_code,
        "base_asset_issuer": base_asset_issuer,
        "counter_asset_code": counter_asset_code,
        "counter_asset_issuer": counter_asset_issuer,
        "pair": pair,
        "starting_size_B": starting_size_B,
        "starting_size_Q": 0,  # Placeholder for 'buy_sell' specific parameter
        "profit_percent": profit_percent,
        "compound_percent": compound_percent,
        "compounding_option": compounding_option,
        "wait_period_unit": wait_period_unit,
        "first_order_wait_period": first_order_wait_period,
        "chart_interval": chart_interval,
        "num_intervals": num_intervals,
        "window_size": window_size,
        "stacking": stacking,
        "step_price": step_price,
    }

    return user_config

def collect_buy_sell_input():
    print("Buy Sell Cycle Set Input")
    base_asset_code = input("Enter the base asset code: ")
    base_asset_issuer = input("Enter the base asset issuer (or 'native' for native XLM): ")
    counter_asset_code = input("Enter the quote asset code: ")
    counter_asset_issuer = input("Enter the quote asset issuer (or 'native' for native XLM): ")
    pair = input("Enter the trading pair with base asset first and quote asset second (e.g. XLM-USDC):")
    starting_size_Q = float(input("Enter the starting size of the quote currency to trade with: "))
    profit_percent = float(input("Enter the desired profit percentage for each cycle: "))
    compound_percent = float(input("Enter the desired compounding percentage: "))
    compounding_option = input("Enter the compounding option (e.g., '100' for full compounding, 'partial' for partial compounding): ")
    wait_period_unit = input("Enter units for wait period interval ('minutes', 'hours', 'days'): ")
    first_order_wait_period = int(input("Enter the wait period interval in {} for the first order: ".format(wait_period_unit)))
    
    # Administrator: Enter Bollinger band calculation parameters
    chart_interval = int(input("Enter the chart interval (in seconds) you wish to trade in (e.g., 60 for a 1-minute chart): "))
    num_intervals = int(input("Enter the number of intervals you want to fetch: "))
    window_size = int(input("Enter the window size for calculating Bollinger Bands: "))
    stacking = input("Enter 'True' or 'False' for whether to place multiple stacked price orders:")
    step_price = input("Enter 'True' or 'False' for whether to step up or down order prices based on market fluctuations:" )

    user_config = {
        "base_asset_code": base_asset_code,
        "base_asset_issuer": base_asset_issuer,
        "counter_asset_code": counter_asset_code,
        "counter_asset_issuer": counter_asset_issuer,
        "pair": pair,
        "starting_size_B": 0,  # Placeholder for 'sell_buy' specific parameter
        "starting_size_Q": starting_size_Q,
        "profit_percent": profit_percent,
        "compound_percent": compound_percent,
        "compounding_option": compounding_option,
        "wait_period_unit": wait_period_unit,
        "first_order_wait_period": first_order_wait_period,
        "chart_interval": chart_interval,
        "num_intervals": num_intervals,
        "window_size": window_size,
        "stacking": stacking,
        "step_price": step_price,
    }

    return user_config

def get_valid_choice(prompt, valid_choices):
    while True:
        user_input = input(prompt)

        if user_input.isdigit() and int(user_input) in valid_choices:
            return int(user_input)
        else:
            print("Invalid input. Please enter a valid choice.")

