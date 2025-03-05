# test_function.py

import time
from datetime import datetime, timedelta
import pytz
from cycle_set_init import definitions
from logging_config import info_logger, error_logger


# Function to determine 24 hour mean
def determine_mean24(conn, user_config):
    from stellar_db import fetch_historical_candles_from_db

    current_time = int(time.time())
    twenty_four_hours_ago = current_time - (24 * 60 * 60)
    high_24hr = float('-inf')
    low_24hr = float('inf')

    # Calculate the last 30 days date range
    end_date_str = datetime.now(pytz.utc).strftime("%Y-%m-%d")
    start_date_str = (datetime.now(pytz.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

    historical_data24 = fetch_historical_candles_from_db(conn, user_config, 86400, start_date_str=start_date_str, end_date_str=end_date_str, last_processed_timestamp=None)

    for entry in historical_data24:
        entry_time = entry[0]
        if twenty_four_hours_ago <= entry_time <= current_time:
            try:
                high = float(entry[5])
                low = float(entry[6])

                if high > high_24hr:
                    high_24hr = high
                if low < low_24hr:
                    low_24hr = low
            except ValueError:
                error_logger.error("Error: unable to determine 24 hr high and low")
                pass

    mean24 = (high_24hr + low_24hr) / 2

    info_logger.info("24 hr High: %s, 24 hr Low: %s", high_24hr, low_24hr)
    info_logger.info("24 hr mean: %s", mean24)

    return mean24

conn = definitions["conn"]
user_config = definitions["user_config"]
mean24 = determine_mean24(conn, user_config)