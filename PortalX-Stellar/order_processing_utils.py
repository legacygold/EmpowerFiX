# order_processing_utils.py

from logging_config import app_logger, info_logger, error_logger


def open_limit_sell_order_processing(open_size_B, order_details, order_processing_params = {}):
    # Extract relevant information from order_details
    filled_value = float(order_details["order"]["filled_value"])
    total_fees = float(order_details["order"]["total_fees"])
    total_value_after_fees = float(order_details["order"]["total_value_after_fees"])
    filled_size = float(order_details["order"]["filled_size"])
    
    # Determine decimal places for rounding
    quote_decimals = 7
    base_decimals = 7
    
    try:
        # Calculate the subtotal in quote currency
        subtotal_Q_ols = filled_value

        # Calculate the fee in quote currency
        fee_Q_ols = total_fees

        # Calculate the total received in quote currency after deducting fees
        total_received_Q_ols = total_value_after_fees

        # Rename the total amount of the base currency spent
        total_spent_B_ols = filled_size

        # Determine any residual base currency not spent
        residual_amt_B_ols = open_size_B - filled_size

        # Round the subtotal, fee, and total received to quote_increment decimal places
        subtotal_Q_ols = round(subtotal_Q_ols, quote_decimals)
        fee_Q_ols = round(fee_Q_ols, quote_decimals)
        total_received_Q_ols = round(total_received_Q_ols, quote_decimals)

        # Round the total spent and residual base currency to base_increment decimal places
        total_spent_B_ols = round(total_spent_B_ols, base_decimals)  
        residual_amt_B_ols = round(residual_amt_B_ols, base_decimals)


        # Store results in the dictionary
        order_processing_params["total_spent_B_ols"] = total_spent_B_ols
        order_processing_params["total_received_Q_ols"] = total_received_Q_ols
        order_processing_params["residual_amt_B_ols"] = residual_amt_B_ols

        info_logger.info("Subtotal for open limit sell order: %s", subtotal_Q_ols)
        info_logger.info("Fee for open limit sell order: %s", fee_Q_ols)
        info_logger.info("Total open cycle amount of base currency spent: %s", total_spent_B_ols)
        info_logger.info("Total open cycle amount of quote currency received: %s", total_received_Q_ols)
        info_logger.info("Residual open cycle amount of base currency not spent: %s", residual_amt_B_ols)
        app_logger.info("Processed order parameters: %s", order_processing_params)

        return order_processing_params

    except Exception as e:
        # Log the exception and handle it accordingly
        error_logger.error(f"An error occurred in open_limit_sell_order_processing: {e}")
        return None

def open_limit_buy_order_processing(open_size_Q, order_details, order_processing_params = {}):
    # Extract relevant information from order_details
    filled_value = float(order_details["order"]["filled_value"])
    total_fees = float(order_details["order"]["total_fees"])
    total_value_after_fees = float(order_details["order"]["total_value_after_fees"])
    filled_size = float(order_details["order"]["filled_size"])

    # Determine decimal places for rounding
    quote_decimals = 7
    base_decimals = 7

    try:
        # Calculate the subtotal in quote currency
        subtotal_Q_olb = filled_value

        # Determine the total spent to receive the rounded amount of the base currency including fees
        total_spent_Q_olb = total_value_after_fees

        # Calculate the fee in quote currency
        fee_Q_olb = total_fees

        # Rename the total B received in the transaction
        total_received_B_olb = filled_size

        # Calculate residual amount of quote ccurrency not spent
        residual_amt_Q_olb = open_size_Q - total_value_after_fees

        # Round the subtotal, fee, and total spent to quote_increment decimal places
        subtotal_Q_olb = round(subtotal_Q_olb, quote_decimals)
        fee_Q_olb = round(fee_Q_olb, quote_decimals)
        total_spent_Q_olb = round(total_spent_Q_olb, quote_decimals)

        # Round the total received base currency and residual quote currency to base_increment and quote_increment decimal places, respectively
        total_received_B_olb = round(total_received_B_olb, base_decimals)  
        residual_amt_Q_olb = round(residual_amt_Q_olb, quote_decimals)

        # Store results in the dictionary
        order_processing_params["total_spent_Q_olb"] = total_spent_Q_olb
        order_processing_params["total_received_B_olb"] = total_received_B_olb
        order_processing_params["residual_amt_Q_olb"] = residual_amt_Q_olb

        info_logger.info("Subtotal for open limit buy order: %s", subtotal_Q_olb)
        info_logger.info("Fee for open limit buy order: %s", fee_Q_olb)
        info_logger.info("Total open cycle amount of quote currency spent: %s", total_spent_Q_olb)
        info_logger.info("Total open cycle amount of base currency received: %s", total_received_B_olb) 
        info_logger.info("Residual open cycle amount of quote currency not spent: %s", residual_amt_Q_olb)
        app_logger.info("Processed order parameters: %s", order_processing_params)

        return order_processing_params
    
    except Exception as e:
            # Log the exception and handle it accordingly
            error_logger.error(f"An error occurred in open_limit_buy_order_processing: {e}")
            return None
    
def close_limit_buy_order_processing(close_size_Q, order_details, order_processing_params = {}):
    # Extract relevant information from order_details
    filled_value = float(order_details["order"]["filled_value"])
    total_fees = float(order_details["order"]["total_fees"])
    total_value_after_fees = float(order_details["order"]["total_value_after_fees"])
    filled_size = float(order_details["order"]["filled_size"])
    
    # Determine decimal places for rounding
    quote_decimals = 7
    base_decimals = 7

    try:
        # Calculate the subtotal in quote currency
        subtotal_Q_clb = filled_value

        # Determine the total spent to receive the rounded amount of the base currency including fees
        total_spent_Q_clb = total_value_after_fees

        # Calculate the fee in quote currency
        fee_Q_clb = total_fees

        # Rename the total B received in the transaction
        total_received_B_clb = filled_size

        # Calculate residual amount of quote ccurrency not spent
        residual_amt_Q_clb = close_size_Q - total_value_after_fees

        # Round the subtotal, fee, and total spent to quote_increment decimal places
        subtotal_Q_clb = round(subtotal_Q_clb, quote_decimals)
        fee_Q_clb = round(fee_Q_clb, quote_decimals)
        total_spent_Q_clb = round(total_spent_Q_clb, quote_decimals)

        # Round the total received base currency and residual quote currency to base_increment and quote_increment decimal places, respectively
        total_received_B_clb = round(total_received_B_clb, base_decimals)  
        residual_amt_Q_clb = round(residual_amt_Q_clb, quote_decimals)

        # Store results in the dictionary
        order_processing_params["total_spent_Q_clb"] = total_spent_Q_clb
        order_processing_params["total_received_B_clb"] = total_received_B_clb
        order_processing_params["residual_amt_Q_clb"] = residual_amt_Q_clb

        info_logger.info("Subtotal for close limit buy order: %s", subtotal_Q_clb)
        info_logger.info("Fee for close limit buy order: %s", fee_Q_clb)
        info_logger.info("Total close cycle amount of quote currency spent: %s", total_spent_Q_clb)
        info_logger.info("Total close cycle amount of base currency received: %s", total_received_B_clb) 
        info_logger.info("Residual close cycle amount of quote currency not spent: %s", residual_amt_Q_clb)
        app_logger.info("Processed order parameters: %s", order_processing_params)

        return order_processing_params

    except Exception as e:
                # Log the exception and handle it accordingly
                error_logger.error(f"An error occurred in close_limit_buy_order_processing: {e}")
                return None
    
def close_limit_sell_order_processing(close_size_B, order_details, order_processing_params = {}):
    # Extract relevant information from order_details
    filled_value = float(order_details["order"]["filled_value"])
    total_fees = float(order_details["order"]["total_fees"])
    total_value_after_fees = float(order_details["order"]["total_value_after_fees"])
    filled_size = float(order_details["order"]["filled_size"])

    # Determine decimal places for rounding
    quote_decimals = 7
    base_decimals = 7
    
    try:
        # Calculate the subtotal in quote currency
        subtotal_Q_cls = filled_value

        # Calculate the fee in quote currency
        fee_Q_cls = total_fees

        # Calculate the total received in quote currency after deducting fees
        total_received_Q_cls = total_value_after_fees

        # Rename the total amount of the base currency spent
        total_spent_B_cls = filled_size

        # Determine any residual base currency not spent
        residual_amt_B_cls = close_size_B - filled_size

        # Round the subtotal, fee, and total received to quote_increment decimal places
        subtotal_Q_cls = round(subtotal_Q_cls, quote_decimals)
        fee_Q_cls = round(fee_Q_cls, quote_decimals)
        total_received_Q_cls = round(total_received_Q_cls, quote_decimals)

        # Round the total spent and residual base currency to base_increment decimal places
        total_spent_B_cls = round(total_spent_B_cls, base_decimals)  
        residual_amt_B_cls = round(residual_amt_B_cls, base_decimals)


        # Store results in the dictionary
        order_processing_params["total_spent_B_cls"] = total_spent_B_cls
        order_processing_params["total_received_Q_cls"] = total_received_Q_cls
        order_processing_params["residual_amt_B_cls"] = residual_amt_B_cls

        info_logger.info("Subtotal for close limit sell order: %s", subtotal_Q_cls)
        info_logger.info("Fee for close limit sell order: %s", fee_Q_cls)
        info_logger.info("Total close cycle amount of base currency spent: %s", total_spent_B_cls)
        info_logger.info("Total close cycle amount of quote currency received: %s", total_received_Q_cls)
        info_logger.info("Residual close cycle amount of base currency not spent: %s", residual_amt_B_cls)
        app_logger.info("Processed order parameters: %s", order_processing_params)

        return order_processing_params

    except Exception as e:
        # Log the exception and handle it accordingly
        error_logger.error(f"An error occurred in close_limit_sell_order_processing: {e}")
        return None
    
def open_market_sell_order_processing(open_size_B, order_details, order_processing_params = {}):
    # Extract relevant information from order_details
    filled_value = float(order_details["order"]["filled_value"])
    total_fees = float(order_details["order"]["total_fees"])
    total_value_after_fees = float(order_details["order"]["total_value_after_fees"])
    filled_size = float(order_details["order"]["filled_size"])
    
    # Determine decimal places for rounding
    quote_decimals = 7
    base_decimals = 7
    
    try:
        # Calculate the subtotal in quote currency
        subtotal_Q_oms = filled_value

        # Calculate the fee in quote currency
        fee_Q_oms = total_fees

        # Calculate the total received in quote currency after deducting fees
        total_received_Q_oms = total_value_after_fees

        # Rename the total amount of the base currency spent
        total_spent_B_oms = filled_size

        # Determine any residual base currency not spent
        residual_amt_B_oms = open_size_B - filled_size

        # Round the subtotal, fee, and total received to quote_increment decimal places
        subtotal_Q_oms = round(subtotal_Q_oms, quote_decimals)
        fee_Q_oms = round(fee_Q_oms, quote_decimals)
        total_received_Q_oms = round(total_received_Q_oms, quote_decimals)

        # Round the total spent and residual base currency to base_increment decimal places
        total_spent_B_oms = round(total_spent_B_oms, base_decimals)  
        residual_amt_B_oms = round(residual_amt_B_oms, base_decimals)


        # Store results in the dictionary
        order_processing_params["total_spent_B_oms"] = total_spent_B_oms
        order_processing_params["total_received_Q_oms"] = total_received_Q_oms
        order_processing_params["residual_amt_B_oms"] = residual_amt_B_oms

        info_logger.info("Subtotal for open market sell order: %s", subtotal_Q_oms)
        info_logger.info("Fee for open market sell order: %s", fee_Q_oms)
        info_logger.info("Total open cycle amount of base currency spent: %s", total_spent_B_oms)
        info_logger.info("Total open cycle amount of quote currency received: %s", total_received_Q_oms)
        info_logger.info("Residual open cycle amount of base currency not spent: %s", residual_amt_B_oms)
        app_logger.info("Processed order parameters: %s", order_processing_params)

        return order_processing_params

    except Exception as e:
        # Log the exception and handle it accordingly
        error_logger.error(f"An error occurred in open_market_sell_order_processing: {e}")
        return None

    
def open_market_buy_order_processing(open_size_Q, order_details, order_processing_params = {}):
    # Extract relevant information from order_details
    filled_value = float(order_details["order"]["filled_value"])
    total_fees = float(order_details["order"]["total_fees"])
    total_value_after_fees = float(order_details["order"]["total_value_after_fees"])
    filled_size = float(order_details["order"]["filled_size"])

    # Determine decimal places for rounding
    quote_decimals = 7
    base_decimals = 7

    try:
        # Calculate the subtotal in quote currency
        subtotal_Q_omb = filled_value

        # Determine the total spent to receive the rounded amount of the base currency including fees
        total_spent_Q_omb = total_value_after_fees

        # Calculate the fee in quote currency
        fee_Q_omb = total_fees

        # Rename the total B received in the transaction
        total_received_B_omb = filled_size

        # Calculate residual amount of quote ccurrency not spent
        residual_amt_Q_omb = open_size_Q - total_value_after_fees

        # Round the subtotal, fee, and total spent to quote_increment decimal places
        subtotal_Q_omb = round(subtotal_Q_omb, quote_decimals)
        fee_Q_omb = round(fee_Q_omb, quote_decimals)
        total_spent_Q_omb = round(total_spent_Q_omb, quote_decimals)

        # Round the total received base currency and residual quote currency to base_increment and quote_increment decimal places, respectively
        total_received_B_omb = round(total_received_B_omb, base_decimals)  
        residual_amt_Q_omb = round(residual_amt_Q_omb, base_decimals)

        # Store results in the dictionary
        order_processing_params["total_spent_Q_omb"] = total_spent_Q_omb
        order_processing_params["total_received_B_omb"] = total_received_B_omb
        order_processing_params["residual_amt_Q_omb"] = residual_amt_Q_omb

        info_logger.info("Subtotal for open market buy order: %s", subtotal_Q_omb)
        info_logger.info("Fee for open market buy order: %s", fee_Q_omb)
        info_logger.info("Total open cycle amount of quote currency spent: %s", total_spent_Q_omb)
        info_logger.info("Total open cycle amount of base currency received: %s", total_received_B_omb) 
        info_logger.info("Residual open cycle amount of quote currency not spent: %s", residual_amt_Q_omb)
        app_logger.info("Processed order parameters: %s", order_processing_params)

        return order_processing_params
    
    except Exception as e:
            # Log the exception and handle it accordingly
            error_logger.error(f"An error occurred in open_market_buy_order_processing: {e}")
            return None
    
def close_market_buy_order_processing(close_size_Q, order_details, order_processing_params = {}):
    # Extract relevant information from order_details
    filled_value = float(order_details["order"]["filled_value"])
    total_fees = float(order_details["order"]["total_fees"])
    total_value_after_fees = float(order_details["order"]["total_value_after_fees"])
    filled_size = float(order_details["order"]["filled_size"])
    
    # Determine decimal places for rounding
    quote_decimals = 7
    base_decimals = 7

    try:
        # Calculate the subtotal in quote currency
        subtotal_Q_cmb = filled_value

        # Determine the total spent to receive the rounded amount of the base currency including fees
        total_spent_Q_cmb = total_value_after_fees

        # Calculate the fee in quote currency
        fee_Q_cmb = total_fees

        # Rename the total B received in the transaction
        total_received_B_cmb = filled_size

        # Calculate residual amount of quote ccurrency not spent
        residual_amt_Q_cmb = close_size_Q - total_value_after_fees

        # Round the subtotal, fee, and total spent to quote_increment decimal places
        subtotal_Q_cmb = round(subtotal_Q_cmb, quote_decimals)
        fee_Q_cmb = round(fee_Q_cmb, quote_decimals)
        total_spent_Q_cmb = round(total_spent_Q_cmb, quote_decimals)

        # Round the total received base currency and residual quote currency to base_increment and quote-increment decimal places, respectively
        total_received_B_cmb = round(total_received_B_cmb, base_decimals)  
        residual_amt_Q_cmb = round(residual_amt_Q_cmb, base_decimals)

        # Store results in the dictionary
        order_processing_params["total_spent_Q_cmb"] = total_spent_Q_cmb
        order_processing_params["total_received_B_cmb"] = total_received_B_cmb
        order_processing_params["residual_amt_Q_cmb"] = residual_amt_Q_cmb

        info_logger.info("Subtotal for close market buy order: %s", subtotal_Q_cmb)
        info_logger.info("Fee for close market buy order: %s", fee_Q_cmb)
        info_logger.info("Total close cycle amount of quote currency spent: %s", total_spent_Q_cmb)
        info_logger.info("Total close cycle amount of base currency received: %s", total_received_B_cmb) 
        info_logger.info("Residual close cycle amount of quote currency not spent: %s", residual_amt_Q_cmb)
        app_logger.info("Processed order parameters: %s", order_processing_params)

        return order_processing_params

    except Exception as e:
                # Log the exception and handle it accordingly
                error_logger.error(f"An error occurred in close_market_buy_order_processing: {e}")
                return None
    
def close_market_sell_order_processing(close_size_B, order_details, order_processing_params = {}):
    # Extract relevant information from order_details
    filled_value = float(order_details["order"]["filled_value"])
    total_fees = float(order_details["order"]["total_fees"])
    total_value_after_fees = float(order_details["order"]["total_value_after_fees"])
    filled_size = float(order_details["order"]["filled_size"])

    # Determine decimal places for rounding
    quote_decimals = 7
    base_decimals = 7
    
    try:
        # Calculate the subtotal in quote currency
        subtotal_Q_cms = filled_value

        # Calculate the fee in quote currency
        fee_Q_cms = total_fees

        # Calculate the total received in quote currency after deducting fees
        total_received_Q_cms = total_value_after_fees

        # Rename the total amount of the base currency spent
        total_spent_B_cms = filled_size

        # Determine any residual base currency not spent
        residual_amt_B_cms = close_size_B - filled_size

        # Round the subtotal, fee, and total received to quote_increment decimal places
        subtotal_Q_cms = round(subtotal_Q_cms, quote_decimals)
        fee_Q_cms = round(fee_Q_cms, quote_decimals)
        total_received_Q_cms = round(total_received_Q_cms, quote_decimals)

        # Round the total spent and residual base currency to base_increment decimal places
        total_spent_B_cms = round(total_spent_B_cms, base_decimals)  
        residual_amt_B_cms = round(residual_amt_B_cms, base_decimals)


        # Store results in the dictionary
        order_processing_params["total_spent_B_cms"] = total_spent_B_cms
        order_processing_params["total_received_Q_cms"] = total_received_Q_cms
        order_processing_params["residual_amt_B_cms"] = residual_amt_B_cms

        info_logger.info("Subtotal for close market sell order: %s", subtotal_Q_cms)
        info_logger.info("Fee for close market sell order: %s", fee_Q_cms)
        info_logger.info("Total close cycle amount of base currency spent: %s", total_spent_B_cms)
        info_logger.info("Total close cycle amount of quote currency received: %s", total_received_Q_cms)
        info_logger.info("Residual close cycle amount of base currency not spent: %s", residual_amt_B_cms)
        app_logger.info("Processed order parameters: %s", order_processing_params)

        return order_processing_params

    except Exception as e:
        # Log the exception and handle it accordingly
        error_logger.error(f"An error occurred in close_market_sell_order_processing: {e}")
        return None
