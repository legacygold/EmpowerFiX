# compounding_utils.py

from logging_config import info_logger


# Function to calculate close limit buy order compounding quote currency amounts
def calculate_close_limit_buy_compounding_amt_Q(total_received_Q, total_spent_B, close_price_buy):
    # Determine number decimal places of quote_increment (an integer)
    decimal_places = 0.0000001
    
    # Calculate the amount of quote currency used if no compounding and amount available for compounding for close limit buy order
    no_compounding_Q_limit_clb = round((total_spent_B * close_price_buy), decimal_places)
    compounding_amt_Q_clb = total_received_Q - no_compounding_Q_limit_clb

    info_logger.info("Amount of quote currency compounded for close cycle limit buy order: %s", compounding_amt_Q_clb)
    info_logger.info("Amount of quote currency to be used if no compounding for close cycle limit buy order: %s", no_compounding_Q_limit_clb)
    
    return compounding_amt_Q_clb, no_compounding_Q_limit_clb

# Function to calculate close market buy order compounding quote currency amounts
def calculate_close_market_buy_compounding_amt_Q(total_received_Q, total_spent_B, close_price_buy):
    # Determine number decimal places of quote_increment (an integer)
    decimal_places = 0.0000001
    
    # Calculate the amount of quote currency used if no compounding and amount available for compounding for close market buy order
    no_compounding_Q_market_cmb = round((total_spent_B * close_price_buy), decimal_places)
    compounding_amt_Q_cmb = total_received_Q - no_compounding_Q_market_cmb

    info_logger.info("Amount of quote currency compounded for close cycle market buy order: %s", compounding_amt_Q_cmb)
    info_logger.info("Amount of quote currency to be used if no compounding for close cycle market buy order: %s", no_compounding_Q_market_cmb)
    
    return compounding_amt_Q_cmb, no_compounding_Q_market_cmb

# Function to calculate close limit sell order compounding base currency amounts
def calculate_close_limit_sell_compounding_amt_B(total_received_B, total_spent_Q, close_price_sell):
    # Determine number decimal places of bas_increment (an integer)
    decimal_places = 0.0000001
    
    # Calculate the amount of base currency used if no compounding and amount available for compounding for close limit sell order
    no_compounding_B_limit_cls = round((total_spent_Q / close_price_sell), decimal_places)
    compounding_amt_B_cls = total_received_B - no_compounding_B_limit_cls

    info_logger.info("Amount of base currency compounded for close cycle limit sell order: %s", compounding_amt_B_cls)
    info_logger.info("Amount of base currency to be used if no compounding for close cycle limit sell order: %s", no_compounding_B_limit_cls)

    return compounding_amt_B_cls, no_compounding_B_limit_cls

# Function to calculate close market sell order compounding base currency amounts
def calculate_close_market_sell_compounding_amt_B(total_received_B, total_spent_Q, close_price_sell):
    # Determine number decimal places of bas_increment (an integer)
    decimal_places = 0.0000001
    
    # Calculate the amount of base currency used if no compounding and amount available for compounding for close market sell order
    no_compounding_B_market_cms = round((total_spent_Q / close_price_sell), decimal_places)
    compounding_amt_B_cms = total_received_B - no_compounding_B_market_cms

    info_logger.info("Amount of base currency compounded for close cycle market sell order: %s", compounding_amt_B_cms)
    info_logger.info("Amount of base currency to be used if no compounding for close cycle market sell order: %s", no_compounding_B_market_cms)

    return compounding_amt_B_cms, no_compounding_B_market_cms

# Function to determine the next close_size_Q for limit order
def determine_next_close_size_Q_limit(compounding_option, total_received_Q, no_compounding_Q_limit, compounding_amt_Q, compound_percent):
    if compounding_option == "100":
        close_size_Q = total_received_Q
    elif compounding_option == "partial":
        close_size_Q = float(no_compounding_Q_limit + (compounding_amt_Q * (compound_percent / 100)))
    else:
        raise ValueError("Invalid compounding option")
    
    # Print next close cycle quote currency size
    info_logger.info("Next close cycle limit order quote currency size: %s", close_size_Q)

    return close_size_Q 

# Function to determine the next close_size_B for limit order
def determine_next_close_size_B_limit(compounding_option, total_received_B, no_compounding_B_limit, compounding_amt_B, compound_percent):
    if compounding_option == "100":
        close_size_B = total_received_B
    elif compounding_option == "partial":
        close_size_B = float(no_compounding_B_limit + (compounding_amt_B * (compound_percent / 100)))
    else:
        raise ValueError("Invalid compounding option")
    
    # Print next close cycle base currency size
    info_logger.info("Next close cycle limit order base currency size: %s", close_size_B)

    return close_size_B 

# Function to determine the next close_size_Q for market order
def determine_next_close_size_Q_market(compounding_option, total_received_Q, no_compounding_Q_market, compounding_amt_Q, compound_percent):
    if compounding_option == "100":
        close_size_Q = total_received_Q
    elif compounding_option == "partial":
        close_size_Q = float(no_compounding_Q_market + (compounding_amt_Q * (compound_percent / 100)))
    else:
        raise ValueError("Invalid compounding option")
    
    # Print next close cycle quote currency size
    info_logger.info("Next close cycle market order quote currency size: %s", close_size_Q)

    return close_size_Q

# Function to determine the next close_size_B for limit order
def determine_next_close_size_B_market(compounding_option, total_received_B, no_compounding_B_market, compounding_amt_B, compound_percent):
    if compounding_option == "100":
        close_size_B = total_received_B
    elif compounding_option == "partial":
        close_size_B = float(no_compounding_B_market + (compounding_amt_B * (compound_percent / 100)))
    else:
        raise ValueError("Invalid compounding option")
    
    # Print next close cycle base currency size
    info_logger.info("Next close cycle limit order base currency size: %s", close_size_B)

    return close_size_B 

# Function to calculate open limit sell order compounding base currency amounts
def calculate_open_limit_sell_compounding_amt_B(total_received_B, total_spent_Q, open_price_sell):
    # Determine number decimal places of bas_increment (an integer)
    decimal_places = 0.0000001
    
    # Calculate the amount of base currency used if no compounding and amount available for compounding for open limit sell order
    no_compounding_B_limit_ols = round((total_spent_Q / open_price_sell), decimal_places)
    compounding_amt_B_ols = total_received_B - no_compounding_B_limit_ols

    info_logger.info("Amount of base currency compounded for open cycle limit sell order: %s", compounding_amt_B_ols)
    info_logger.info("Amount of base currency to be used if no compounding for open cycle limit sell order: %s", no_compounding_B_limit_ols)

    return compounding_amt_B_ols, no_compounding_B_limit_ols

# Function to calculate open market sell order compounding base currency amounts
def calculate_open_market_sell_compounding_amt_B(total_received_B, total_spent_Q, open_price_sell):
    # Determine number decimal places of bas_increment (an integer)
    decimal_places = 0.0000001
    
    # Calculate the amount of base currency used if no compounding and amount available for compounding for open market sell order
    no_compounding_B_market_oms = round((total_spent_Q / open_price_sell), decimal_places)
    compounding_amt_B_oms = total_received_B - no_compounding_B_market_oms

    info_logger.info("Amount of base currency compounded for open cycle market sell order: %s", compounding_amt_B_oms)
    info_logger.info("Amount of base currency to be used if no compounding for open cycle market sell order: %s", no_compounding_B_market_oms)

    return compounding_amt_B_oms, no_compounding_B_market_oms

# Function to calculate open limit buy order compounding quote currency amounts
def calculate_open_limit_buy_compounding_amt_Q(total_received_Q, total_spent_B, open_price_buy):
    # Determine number decimal places of quote_increment (an integer)
    decimal_places = 0.0000001
    
    # Calculate the amount of quote currency used if no compounding and amount available for compounding for open limit buy order
    no_compounding_Q_limit_olb = round((total_spent_B * open_price_buy), decimal_places)
    compounding_amt_Q_olb = total_received_Q - no_compounding_Q_limit_olb

    info_logger.info("Amount of quote currency compounded for open cycle limit buy order: %s", compounding_amt_Q_olb)
    info_logger.info("Amount of quote currency to be used if no compounding for open cycle limit buy order: %s", no_compounding_Q_limit_olb)
    
    return compounding_amt_Q_olb, no_compounding_Q_limit_olb

# Function to calculate open market buy order compounding quote currency amounts
def calculate_open_market_buy_compounding_amt_Q(total_received_Q, total_spent_B, open_price_buy):
    # Determine number decimal places of quote_increment (an integer)
    decimal_places = 0.0000001
    
    # Calculate the amount of quote currency used if no compounding and amount available for compounding for open market buy order
    no_compounding_Q_market_omb = round((total_spent_B * open_price_buy), decimal_places)
    compounding_amt_Q_omb = total_received_Q - no_compounding_Q_market_omb

    info_logger.info("Amount of quote currency compounded for open cycle market buy order: %s", compounding_amt_Q_omb)
    info_logger.info("Amount of quote currency to be used if no compounding for open cycle market buy order: %s", no_compounding_Q_market_omb)
    
    return compounding_amt_Q_omb, no_compounding_Q_market_omb

# Function to determine the next open_size_B
def determine_next_open_size_B_limit(compounding_option, total_received_B, no_compounding_B_limit, compounding_amt_B, compound_percent):
    if compounding_option == "100":
        open_size_B = total_received_B
    elif compounding_option == "partial":
        open_size_B = float(no_compounding_B_limit + (compounding_amt_B * (compound_percent / 100)))
    else:
        raise ValueError("Invalid compounding option")
    return open_size_B

# Function to determine the next open_size_Q
def determine_next_open_size_Q_limit(compounding_option, total_received_Q, no_compounding_Q_limit, compounding_amt_Q, compound_percent):
    if compounding_option == "100":
        open_size_Q = total_received_Q
    elif compounding_option == "partial":
        open_size_Q = float(no_compounding_Q_limit + (compounding_amt_Q * (compound_percent / 100)))
    else:
        raise ValueError("Invalid compounding option")
    return open_size_Q

# Function to determine the next open_size_B
def determine_next_open_size_B_market(compounding_option, total_received_B, no_compounding_B_market, compounding_amt_B, compound_percent):
    if compounding_option == "100":
        open_size_B = total_received_B
    elif compounding_option == "partial":
        open_size_B = float(no_compounding_B_market + (compounding_amt_B * (compound_percent / 100)))
    else:
        raise ValueError("Invalid compounding option")
    return open_size_B

# Function to determine the next open_size_Q
def determine_next_open_size_Q_market(compounding_option, total_received_Q, no_compounding_Q_market, compounding_amt_Q, compound_percent):
    if compounding_option == "100":
        open_size_Q = total_received_Q
    elif compounding_option == "partial":
        open_size_Q = float(no_compounding_Q_market + (compounding_amt_Q * (compound_percent / 100)))
    else:
        raise ValueError("Invalid compounding option")
    return open_size_Q
