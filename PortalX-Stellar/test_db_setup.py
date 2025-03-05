# test_db_setup.py

from logging_config import app_logger, info_logger, error_logger
from datetime import datetime, timedelta
from stellar_db import fetch_trading_pairs_from_db, fetch_historical_candles_from_db, extract_data_for_historical_data_db
import psycopg2.extras

start_date_str=None
end_date_str=None 
last_processed_timestamp=None

def get_db_connection():
    # Connect to your postgres DB
    try:
        conn = psycopg2.connect("dbname='stellar_trading' user='legacygold' host='localhost' password='$Poofah32567'")
        return conn
    except Exception as e:
        error_logger.error(f"Database connection failed due to {e}") 

def fetch_historical_candles_from_db(conn, user_config, interval, start_date_str=None, end_date_str=None, last_processed_timestamp=None):
    base_asset_code = user_config["base_asset_code"]
    base_asset_issuer = user_config["base_asset_issuer"]
    counter_asset_code = user_config["counter_asset_code"]
    counter_asset_issuer = user_config["counter_asset_issuer"]
    interval_label = f"{interval//60}m" if interval < 3600 else f"{interval//3600}h"

    # Check the last processed timestamp for the given asset pair and interval
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT last_update_time
            FROM candle_update_log
            WHERE base_asset_code = %s AND base_asset_issuer = %s
            AND counter_asset_code = %s AND counter_asset_issuer = %s
            AND interval = %s
            ORDER BY last_update_time DESC LIMIT 1
        """, (base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, interval_label))
        last_update_row = cur.fetchone()
        last_processed_timestamp = last_update_row['last_update_time'] if last_update_row else None

    # Prepare the base query
    query = """
        SELECT timestamp, open, high, low, close 
        FROM candle_data
        WHERE base_asset_code = %s AND base_asset_issuer = %s AND counter_asset_code = %s AND counter_asset_issuer = %s AND interval = %s
    """
    params = [base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, interval_label]

    # Construct date range and last_processed_timestamp filtering
    date_conditions = []
    if start_date_str:
        date_conditions.append("timestamp >= %s")
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        params.append(start_date)
    if end_date_str:
        date_conditions.append("timestamp <= %s")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d") + timedelta(days=1) - timedelta(seconds=1)
        params.append(end_date)
    if last_processed_timestamp:
        date_conditions.append("timestamp > %s")
        params.append(last_processed_timestamp)

    if date_conditions:
        query += " AND " + " AND ".join(date_conditions)

    query += " ORDER BY timestamp ASC"

    # Fetch the candles from the database
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(query, params)
        candles_rows = cur.fetchall()

    # Convert fetched rows into a list of dictionaries
    fetched_candles = [{
        'timestamp': candle['timestamp'],
        'open': candle['open'],
        'high': candle['high'],
        'low': candle['low'],
        'close': candle['close'],
    } for candle in candles_rows]

    app_logger.info("Fetched the most recent candles for all pairings in the database.")

    return fetched_candles

def test_fetch_historical_candles_from_db(conn, user_config, interval, start_date_str=None, end_date_str=None, last_processed_timestamp=None):
    print("Generating candle data for database...")
    candles = fetch_historical_candles_from_db(conn, user_config, interval, start_date_str, end_date_str, last_processed_timestamp)
    
    # Check if historical_data is not None before attempting to iterate
    if candles is not None:
        print("Data extracted successfully.")
    else:
        print("Failed to generate and/or update candle data.")

    return candles

if __name__ == "__main__":
    interval = 60

    conn = get_db_connection()
    trading_pairs = []
    trading_pairs = fetch_trading_pairs_from_db(conn)

    for pair in trading_pairs:
        base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer = pair
        
        # Correctly handle native assets
        base_asset_issuer = "native" if base_asset_issuer is None else base_asset_issuer
        counter_asset_issuer = "native" if counter_asset_issuer is None else counter_asset_issuer
        extract_data_for_historical_data_db(conn, base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer)
    
    info_logger.info(f"Successfully fetched historical trade data for all trading pairs in the database.")

