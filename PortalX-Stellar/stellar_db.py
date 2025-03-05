# stellar_db.py

import time
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
from logging_config import app_logger, info_logger, error_logger
import pytz
from dateutil.parser import parse
from queue import Queue
from  threading import Thread, Semaphore, Lock
from pytz import utc
from stellar_sdk import Server, Asset
from fetch_stellar_dex_data import fetch_historical_data, fetch_historical_data_with_paging_token, fetch_historical_candles
import psycopg2
from psycopg2.extras import DictCursor


# Global variables for rate limit management
BATCH_DURATION = 10  # seconds
BETWEEN_BATCHES_PAUSE = 5 # seconds
STREAM_DURATION = 60 # seconds
RESTART_LOOP_PAUSE = 5 # seconds
MAX_CONCURRENT_REQUESTS = 5  # Initial guess, needs adjustment based on testing
semaphore = Semaphore(MAX_CONCURRENT_REQUESTS)
rate_limit_lock = Lock()
rate_limit_remaining = 3600  # Assuming a default of 3600 requests per hour
rate_limit_reset_time = time.time() + 3600  # Assuming the reset time is in one hour
streaming_status = {}
SOME_LOG_INTERVAL = 300

def get_db_connection():
    # Connect to your postgres DB
    try:
        conn = psycopg2.connect("dbname='stellar_trading' user='legacygold' host='localhost' password='$Poofah32567'")
        return conn
    except Exception as e:
        error_logger.error(f"Database connection failed due to {e}") 

def create_historical_data_db(conn):
    cur = conn.cursor()
    
    # Table for trading pairs
    cur.execute('''
        CREATE TABLE IF NOT EXISTS trading_pairs (
            base_asset_type TEXT NOT NULL,
            base_asset_code TEXT,
            base_asset_issuer TEXT,
            counter_asset_type TEXT NOT NULL,
            counter_asset_code TEXT,
            counter_asset_issuer TEXT,
            UNIQUE(base_asset_type, base_asset_code, base_asset_issuer, counter_asset_type, counter_asset_code, counter_asset_issuer)
        );
    ''')

    # Table for historical trades
    cur.execute('''
        CREATE TABLE IF NOT EXISTS historical_trades (
            id SERIAL PRIMARY KEY,
            stellar_trade_id TEXT UNIQUE NOT NULL,
            base_asset_type TEXT NOT NULL,
            base_asset_code TEXT,
            base_asset_issuer TEXT,
            counter_asset_type TEXT NOT NULL,
            counter_asset_code TEXT,
            counter_asset_issuer TEXT,
            timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            price REAL NOT NULL,
            base_amount REAL NOT NULL,
            counter_amount REAL NOT NULL,
            base_is_seller BOOLEAN NOT NULL
        );
    ''')

    # Table for candle data
    cur.execute('''
        CREATE TABLE IF NOT EXISTS candle_data (
            id SERIAL PRIMARY KEY,
            base_asset_code TEXT NOT NULL,
            base_asset_issuer TEXT NOT NULL,
            counter_asset_code TEXT NOT NULL,
            counter_asset_issuer TEXT NOT NULL,
            interval TEXT NOT NULL,
            timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            open REAL NOT NULL,
            close REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            UNIQUE(base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, interval, timestamp)
        );
    ''')

   # Table for raw trade data update log
    cur.execute('''
        CREATE TABLE IF NOT EXISTS raw_trade_update_log (
            id SERIAL PRIMARY KEY,
            base_asset_code TEXT NOT NULL,
            base_asset_issuer TEXT NOT NULL,
            counter_asset_code TEXT NOT NULL,
            counter_asset_issuer TEXT NOT NULL,
            last_update_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            last_paging_token TEXT,  -- New column for storing the last paging token
            UNIQUE(base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer)
        );
    ''')

    # Table for candles update log
    cur.execute('''
        CREATE TABLE IF NOT EXISTS candle_update_log (
            id SERIAL PRIMARY KEY,
            base_asset_code TEXT NOT NULL,
            base_asset_issuer TEXT NOT NULL,
            counter_asset_code TEXT NOT NULL,
            counter_asset_issuer TEXT NOT NULL,
            interval TEXT NOT NULL,
            last_update_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            last_paging_token TEXT,  -- New column for storing the last paging token
            UNIQUE(base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, interval)
        );
    ''')
    
    conn.commit()
    cur.close()

    print(f"A database for storing trade data has been created")

def extract_data_for_historical_data_db(conn, base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer):
    info_logger.info(f"A database of trades for {base_asset_code}/{counter_asset_code} is being generated or updated. Please wait...")
    cur = conn.cursor()

    server_url = "https://horizon.stellar.org"
    server = Server(horizon_url=server_url)

    cur.execute("""
        SELECT last_update_time FROM raw_trade_update_log 
        WHERE base_asset_code=%s AND base_asset_issuer=%s AND 
        counter_asset_code=%s AND counter_asset_issuer=%s
    """, (base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer))
    
    result = cur.fetchone()
    if result and result[0]:
        # Assuming the fetched last_update_time is a datetime object
        last_update_time = result[0]
    else:
        # Set to 30 days ago if no entry is found
        last_update_time = datetime.now(pytz.UTC) - timedelta(days=30)

    start_time = last_update_time
    info_logger.info("Start time: %s", start_time)
    end_time = datetime.now(pytz.UTC)
    info_logger.info("End time: %s", end_time)

    # Ensure start_time is offset-aware
    if start_time.tzinfo is None or start_time.tzinfo.utcoffset(start_time) is None:
        start_time = pytz.UTC.localize(start_time)

    # end_time should already be offset-aware from how it's generated
    # But, if necessary, you can ensure it's also correctly offset-aware
    if end_time.tzinfo is None or end_time.tzinfo.utcoffset(end_time) is None:
        end_time = pytz.UTC.localize(end_time)

    # Correctly creating Asset objects
    base_asset = Asset.native() if base_asset_code == 'XLM' else Asset(base_asset_code, base_asset_issuer)
    counter_asset = Asset.native() if counter_asset_code == 'XLM' else Asset(counter_asset_code, counter_asset_issuer)

    historical_data = fetch_historical_data(server, base_asset, counter_asset, start_time, end_time, max_records=5000000)

    # Log the number of trades fetched
    info_logger.info(f"Number of records from 'historical_data' fetched: {len(historical_data)}")

    successful_insertions = 0
    for trade in historical_data:
        # Extract correct keys from the trade data
        # Parse the string and assign UTC timezone
        timestamp = parse(trade['ledger_close_time'])
        if timestamp.tzinfo is None or timestamp.tzinfo.utcoffset(timestamp) is None:
            timestamp = timestamp.replace(tzinfo=pytz.UTC)

        price = float(trade['price']['n']) / float(trade['price']['d'])
        base_amount = trade['base_amount']
        counter_amount = trade['counter_amount']
        base_is_seller = trade['base_is_seller']
        stellar_trade_id = trade['id']

        # Determine asset types and handle native assets correctly
        base_asset_type = 'native' if base_asset_code == 'XLM' else trade['base_asset_type']
        counter_asset_type = 'native' if counter_asset_code == 'XLM' else trade['counter_asset_type']      

        try:
            # Insert the trade data into the database
            cur.execute("""
                INSERT INTO historical_trades (
                    stellar_trade_id,
                    base_asset_type, base_asset_code, base_asset_issuer, 
                    counter_asset_type, counter_asset_code, counter_asset_issuer, 
                    timestamp, price, base_amount, counter_amount, base_is_seller
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (stellar_trade_id) 
                DO NOTHING
            """, (
                stellar_trade_id, base_asset_type, base_asset_code, base_asset_issuer, 
                counter_asset_type, counter_asset_code, counter_asset_issuer, 
                timestamp, price, base_amount, counter_amount, base_is_seller
            ))
            if cur.rowcount > 0:
                successful_insertions += 1

            conn.commit()

        except Exception as e:
            error_logger.error(f"Failed to insert trade: {e}")
            conn.rollback()  # Rollback in case of failure

    # Log the number of successful insertions
    info_logger.info(f"Number of trades successfully inserted: {successful_insertions}")

    # Example: Updating the last_paging_token after fetching trades
    last_trade = historical_data[-1]  # Assuming historical_data is not empty
    last_paging_token = last_trade['paging_token']

    cur.execute('''
        INSERT INTO trading_pairs (base_asset_type, base_asset_code, base_asset_issuer, counter_asset_type, counter_asset_code, counter_asset_issuer) 
        VALUES (%s, %s, %s, %s, %s, %s) 
        ON CONFLICT (base_asset_type, base_asset_code, base_asset_issuer, counter_asset_type, counter_asset_code, counter_asset_issuer) 
        DO NOTHING
        ''', (base_asset_type, base_asset_code, base_asset_issuer, counter_asset_type, counter_asset_code, counter_asset_issuer))


    cur.execute('''
        INSERT INTO raw_trade_update_log (base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, last_update_time, last_paging_token) 
        VALUES (%s, %s, %s, %s, %s, %s) 
        ON CONFLICT (base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer) 
        DO UPDATE SET last_update_time = EXCLUDED.last_update_time, last_paging_token = EXCLUDED.last_paging_token
        ''', (base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, datetime.now(pytz.UTC), last_paging_token))

    conn.commit()
    cur.close()

    info_logger.info("Data extraction and historical trade data update complete.")

    return historical_data

def fetch_trades_from_db(conn, base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer):
    cur = conn.cursor()
    # Adjust the query as needed based on your data model
    cur.execute("""
        SELECT timestamp, price, base_amount, counter_amount
        FROM historical_trades
        WHERE base_asset_code = %s AND base_asset_issuer = %s AND counter_asset_code = %s AND counter_asset_issuer = %s
        ORDER BY timestamp ASC
    """, (base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer))
    trades = cur.fetchall()
    cur.close()
    return trades

def fetch_historical_data_from_db(conn, base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, start_date_str, end_date_str, last_processed_timestamp):
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Prepare the base query
    query = """
        SELECT timestamp, price, base_amount, counter_amount
        FROM historical_trades
        WHERE base_asset_code = %s AND base_asset_issuer = %s AND counter_asset_code = %s AND counter_asset_issuer = %s
    """

    # Initialize parameters list for the query
    params = [base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer]

    # Add date range filtering if start_date_str, end_date_str, or last_processed_timestamp is provided
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

    # Append date conditions to the query if any
    if date_conditions:
        query += " AND " + " AND ".join(date_conditions)

    query += " ORDER BY timestamp ASC"

    cur.execute(query, params)
    trades = cur.fetchall()

    historical_data = [{
        'timestamp': trade['timestamp'],
        'price': trade['price'],
        'base_amount': trade['base_amount'],
        'counter_amount': trade['counter_amount'],
    } for trade in trades]

    cur.close()
    return historical_data

def generate_and_update_candle_data_for_all_intervals(conn, base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, last_processed_timestamp):
    
    server = Server("https://horizon.stellar.org")
    
    # Fetch the last update info
    _, last_paging_token = fetch_last_update_info(conn, base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, data_source="raw_trade_update_log")

     # Create Asset objects correctly
    base_asset = Asset.native() if base_asset_code.upper() == "XLM" else Asset(base_asset_code, base_asset_issuer)
    counter_asset = Asset.native() if counter_asset_code.upper() == "XLM" else Asset(counter_asset_code, counter_asset_issuer)

    info_logger.info(f"Fetching historical data for {base_asset_code}/{counter_asset_code} pairing...")

    if last_paging_token is None:
        # This is the first run or no previous data, so fetch historical data without filtering by paging token
        historical_data = fetch_historical_data_from_db(conn, base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, start_date_str="earliest", end_date_str="latest", last_processed_timestamp=None)
    else:
        # Fetch new data starting from the last known paging token
        historical_data = fetch_historical_data_with_paging_token(server, base_asset, counter_asset, last_paging_token, max_records=5000000)

    info_logger.info(f"Generating and updating candle data for {base_asset_code}/{counter_asset_code} pairing...")

    intervals = [60, 300, 900, 1800, 3600, 7200, 21600, 86400]  # in seconds
    cur = conn.cursor()
    try:
        for interval in intervals:
            interval_label = f"{interval//60}m" if interval < 3600 else f"{interval//3600}h"
            historical_data = fetch_historical_data_from_db(conn, base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, start_date_str=None, end_date_str=None, last_processed_timestamp=last_processed_timestamp)
            app_logger.info(f"Processing candle data for the {interval_label} interval for the {base_asset_code}/{counter_asset_code} pairing...")

            # Assume fetch_historical_candles() correctly handles trades from DB and interval
            candles = fetch_historical_candles(historical_data, interval)
            _, last_paging_token = fetch_last_update_info(conn, base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, data_source="candle_update_log")

            for candle in candles:
                try:
                    cur.execute("""
                        INSERT INTO candle_data (
                            base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, interval, timestamp, open, close, high, low
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, interval, timestamp)
                        DO UPDATE SET open = EXCLUDED.open, close = EXCLUDED.close, high = EXCLUDED.high, low = EXCLUDED.low
                    """, (
                        base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, 
                        interval_label,  # Make sure this matches how you're identifying intervals (e.g., '1m', '5m', etc.)
                        candle['interval_end'], candle['opening_price'], candle['closing_price'], 
                        candle['highest_price'], candle['lowest_price']
                    ))
                
                except Exception as e:
                    error_logger.error(f"Failed to insert/update candle data: {e}")
                    conn.rollback()

            info_logger.info(f"Candle data generation and update complete for the {interval_label} interval of the {base_asset_code}/{counter_asset_code} pairing.")

            # This assumes you have a mechanism to identify the interval and pair uniquely in the log
            cur.execute("""
                INSERT INTO candle_update_log (base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, interval, last_update_time, last_paging_token)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, interval)
                DO UPDATE SET last_update_time = EXCLUDED.last_update_time, last_paging_token = EXCLUDED.last_paging_token
            """, (base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, interval, datetime.now(pytz.UTC), last_paging_token))

            info_logger.info(f"Successfully fetched {len(candles)} candles for {base_asset_code}/{counter_asset_code} for all candle intervals.")
        
        info_logger.info("Completed generation and/or update of all candles for all pairings in the database.")
        
        conn.commit()
        cur.close()   
    
        return candles
        
    except Exception as e:
        error_logger.error(f"Failed to generate and/or update candle data due to {e}")
        error_logger.error(f"Failed to generate and/or update candles for {base_asset_code}/{counter_asset_code}.")

    

def fetch_historical_candles_from_db(conn, user_config, interval, start_date_str=None, end_date_str=None, last_processed_timestamp=None):
    candles = []
    base_asset_code = user_config["base_asset_code"]
    base_asset_issuer = user_config["base_asset_issuer"]
    counter_asset_code = user_config["counter_asset_code"]
    counter_asset_issuer = user_config["counter_asset_issuer"]
    interval_label = f"{interval//60}m" if interval < 3600 else f"{interval//3600}h"

    last_processed_timestamp, last_paging_token = fetch_last_update_info(conn, base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, data_source="candle_update_log")

    # Prepare the base query
    query = """
        SELECT timestamp, open, high, low, close 
        FROM candle_data
        WHERE base_asset_code = %s AND base_asset_issuer = %s AND counter_asset_code = %s AND counter_asset_issuer = %s AND interval = %s
    """

    # Initialize parameters list for the query
    params = [base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, interval_label]

    # Add date range filtering if start_date_str or end_date_str is provided
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

    # Append date conditions to the query if any
    if date_conditions:
        query += " AND " + " AND ".join(date_conditions)

    query += " ORDER BY timestamp ASC"

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(query, params)
        candles = cur.fetchall()

    # Convert fetched rows into a list of dictionaries
    fetched_candles = []
    for candle in candles:
        fetched_candles.append({
            'timestamp': candle['timestamp'],
            'open': candle['open'],
            'high': candle['high'],
            'low': candle['low'],
            'close': candle['close'],
        })

    return fetched_candles

def adapt_trades_format(trades):
    # Assume trades is a list of tuples or dictionaries fetched from the database
    # Adapt the format as needed for further processing
    adapted_trades = []
    for trade in trades:
        # Example adaptation, specifics depend on your needs
        adapted_trade = {
            "timestamp": trade['timestamp'],
            "price": float(trade['price']),
            "base_amount": float(trade['base_amount']),
            "counter_amount": float(trade['counter_amount']),
            # Add or transform fields as necessary
        }
        adapted_trades.append(adapted_trade)
    return adapted_trades

def select_historical_data_database(conn, historical_data_db):
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM stellar_trading")
    trades = cur.fetchall()
    conn.commit()
    cur.close()
    return trades, historical_data_db

# Function to setup the scheduler for daily updates at midnight UTC
def setup_scheduled_updates(conn, base_asset_code, counter_asset_code, last_update_time):
    scheduler = BackgroundScheduler(timezone=utc)
    scheduler.add_job(extract_data_for_historical_data_db(conn, base_asset_code, counter_asset_code, last_update_time), 'cron', hour=0, minute=0)
    scheduler.start()

    # This part ensures that the scheduler stops gracefully upon shutdown
    try:
        # This is a blocking call that waits for a signal (like Ctrl+C) to shutdown
        scheduler.print_jobs()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()

def setup_scheduled_updates():
    scheduler = BackgroundScheduler()
    
    def update_wrapper(conn):
        # Define your trading pairs and intervals here, or fetch them from a database
        trading_pairs = [('XLM', 'G...issuer', 'USD', 'G...issuer', '1m')]
        for base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, interval in trading_pairs:
            extract_data_for_historical_data_db(conn, base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, interval)
    
    scheduler.add_job(update_wrapper, 'cron', hour=0, minute=0, timezone=pytz.utc)
    scheduler.start()

    # Consider handling shutdown within your main program logic as discussed

def fetch_trading_pairs_from_db(conn):
    info_logger.info("Fetching existing trading pairs...")
    cur = conn.cursor()
    cur.execute("""
        SELECT base_asset_type, base_asset_code, base_asset_issuer, counter_asset_type, counter_asset_code, counter_asset_issuer
        FROM trading_pairs
    """)
    raw_pairs = cur.fetchall()
    cur.close()

    trading_pairs = []
    for pair in raw_pairs:
        base_asset_type, base_asset_code, base_asset_issuer, counter_asset_type, counter_asset_code, counter_asset_issuer = pair
        # Handle native asset representation
        if base_asset_code == 'XLM'and base_asset_type == 'native':
            base_asset_type, base_asset_code, base_asset_issuer = 'native', 'XLM', None
        if counter_asset_code == 'XLM'and counter_asset_type =='native':
            counter_asset_type, counter_asset_code, counter_asset_issuer = 'native', 'XLM', None
        trading_pairs.append((base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer))
    info_logger.info("Exisitng trading pairs: %s", trading_pairs)
    return trading_pairs

def trade_stream_handler(conn, pair, stream_duration):
    start_time = datetime.now()
    session_end_time = start_time + timedelta(seconds=stream_duration)  # Convert stream_duration to timedelta if it's in seconds

    global rate_limit_remaining, rate_limit_reset_time, streaming_status

    status_time = time.time()
    pair_identifier = f"{pair[1]}/{pair[4]}"  # Customize based on how you identify pairs

    # Mark the start of streaming
    streaming_status[pair_identifier] = f"Streaming started for {pair_identifier}"

    while time.time() - status_time < stream_duration:

        base_asset_type, base_asset_code, base_asset_issuer, counter_asset_type, counter_asset_code, counter_asset_issuer = pair
        base_asset = Asset(base_asset_code, base_asset_issuer) if base_asset_issuer else Asset.native()
        counter_asset = Asset(counter_asset_code, counter_asset_issuer)

        cur = conn.cursor()

        try:
            cur.execute('''SELECT last_paging_token FROM raw_trade_update_log WHERE base_asset_code=%s AND base_asset_issuer=%s AND counter_asset_code=%s AND counter_asset_issuer=%s''', (base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer))
            result = cur.fetchone()
            last_paging_token = result[0] if result else 'now'

            server_url = "https://horizon.stellar.org"
            server = Server(horizon_url=server_url)

            while datetime.now() < session_end_time:
                with semaphore:
                    with rate_limit_lock:
                        if time.time() >= rate_limit_reset_time:
                            rate_limit_remaining = 3600
                            rate_limit_reset_time = time.time() + 3600

                        if rate_limit_remaining <= 5:
                            time_to_sleep = rate_limit_reset_time - time.time()
                            time.sleep(time_to_sleep)
                        else:
                            rate_limit_remaining -= 1

                    # Update the dictionary instead of logging each trade processed
                    streaming_status[pair_identifier] = f"Streaming in progress for {pair_identifier}..."

                    # Retrieve the last paging token for the trading pair
                    cur.execute('''SELECT last_paging_token FROM raw_trade_update_log WHERE base_asset_code=%s AND base_asset_issuer=%s AND counter_asset_code=%s AND counter_asset_issuer=%s''', (pair[1], pair[2], pair[4], pair[5]))
                    result = cur.fetchone() 
                    
                    # Use the last paging token to start the stream
                    for trade in server.trades().for_asset_pair(base_asset, counter_asset).cursor(last_paging_token).stream():

                        # Process and insert trade data into the database
                        cur = conn.cursor()

                        # Extract correct keys from the trade data
                        # Parse the string and assign UTC timezone
                        timestamp = parse(trade['ledger_close_time'])
                        if timestamp.tzinfo is None or timestamp.tzinfo.utcoffset(timestamp) is None:
                            timestamp = timestamp.replace(tzinfo=pytz.UTC)
                        price = float(trade['price']['n']) / float(trade['price']['d'])
                        base_amount = trade['base_amount']
                        counter_amount = trade['counter_amount']
                        base_is_seller = trade['base_is_seller']
                        stellar_trade_id = trade['id']

                        # Determine asset types and handle native assets correctly
                        base_asset_type = 'native' if base_asset_code == 'XLM' else base_asset_type
                        counter_asset_type = 'native' if counter_asset_code == 'XLM' else counter_asset_type

                        # Modify 'base_asset_issuer' and 'counter_asset_issuer' directly if they are None
                        if base_asset_issuer is None:
                            base_asset_issuer = 'native'
                        if counter_asset_issuer is None:
                            counter_asset_issuer = 'native'

                        try:
                            # Insert the trade data into the database
                            cur.execute("""
                                INSERT INTO historical_trades (
                                    stellar_trade_id,
                                    base_asset_type, base_asset_code, base_asset_issuer, 
                                    counter_asset_type, counter_asset_code, counter_asset_issuer, 
                                    timestamp, price, base_amount, counter_amount, base_is_seller
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (stellar_trade_id) 
                                DO NOTHING
                            """, (
                                stellar_trade_id, base_asset_type, base_asset_code, base_asset_issuer, 
                                counter_asset_type, counter_asset_code, counter_asset_issuer, 
                                timestamp, price, base_amount, counter_amount, base_is_seller
                            ))

                        except Exception as e:
                            error_logger.error(f"Failed to insert/update trade {trade['id']}: {e}")
                            conn.rollback()  # Rollback in case of failure

                        # Updating the last_paging_token after fetching trade
                        last_paging_token = trade['paging_token']

                        cur.execute('''
                            INSERT INTO raw_trade_update_log (base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, last_update_time, last_paging_token) 
                            VALUES (%s, %s, %s, %s, %s, %s) 
                            ON CONFLICT (base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer) 
                            DO UPDATE SET last_update_time = EXCLUDED.last_update_time, last_paging_token = EXCLUDED.last_paging_token
                            ''', (base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, datetime.now(pytz.UTC), last_paging_token))

                        conn.commit()

                        if datetime.now() >= session_end_time:
                            # When a session reaches its duration limit
                            streaming_status[pair_identifier] = f"Session for {pair_identifier} has reached its duration limit."

                            break  # Exit the streaming loop

                        # Optionally, if you still want occasional logs:
                        if time.time() % SOME_LOG_INTERVAL == 0:
                            info_logger.info(f"Current streaming status: {streaming_status}")
                            print_streaming_status()

                        time.sleep(10)  # Adjust based on your needs

        except Exception as e:
            error_logger.error(f"Error in trade_stream_handler for {pair}: {e}")
        finally:
            # After completing the streaming session
            streaming_status[pair_identifier] = f"Streaming completed for {pair_identifier}"
            cur.close()

def thread_trade_streaming(conn, pairs):
    threads = []
    for base_asset_type, base_asset_code, base_asset_issuer, counter_asset_type, counter_asset_code, counter_asset_issuer in pairs:
        t = Thread(target=trade_stream_handler, args=(conn, base_asset_type, base_asset_code, base_asset_issuer, counter_asset_type, counter_asset_code, counter_asset_issuer))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

def start_streams(conn):
    info_logger.info("Running 'start_streams'...")
    trading_pairs = fetch_trading_pairs_from_db(conn)
    
    while True:  # This loop will ensure continuous operation
        # Initialize a queue with all trading pairs
        trading_pairs_queue = Queue()
        for pair in trading_pairs:
            trading_pairs_queue.put(pair)

        # Process trading pairs in batches continuously
        while not trading_pairs_queue.empty():
            threads = []  # List to keep track of threads

            for _ in range(min(MAX_CONCURRENT_REQUESTS, trading_pairs_queue.qsize())):
                pair = trading_pairs_queue.get()
                t = Thread(target=trade_stream_handler, args=(conn, pair, STREAM_DURATION))
                t.start()
                threads.append(t)

            # Wait for all threads in the current batch to finish
            for t in threads:
                t.join()

            # Optional: Pause between processing batches
            time.sleep(BETWEEN_BATCHES_PAUSE)

        # Optional: Pause before refilling the queue and restarting the loop
        info_logger.info("Completed processing all trading pairs. Restarting...")
        time.sleep(RESTART_LOOP_PAUSE)

        
# Function to be run in a separate process
def run_start_streams():
    conn = get_db_connection()
    start_streams(conn)
    conn.close()

def archive_old_data(conn):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM historical_trades
            WHERE timestamp < NOW() - INTERVAL '30 days'
        """)
        cur.execute("""
            DELETE FROM candle_data
            WHERE timestamp < NOW() - INTERVAL '30 days'
        """)
    conn.commit()
    cur.close()

def drop_tables(conn):
    cur = conn.cursor()

    try:
        # List of tables you want to drop
        tables = ['trading_pairs', 'historical_trades', 'candle_data', 'raw_trade_update_log', 'candle_update_log']
        for table in tables:
            cur.execute(f"DROP TABLE IF EXISTS {table};")
            print(f"Dropped table {table}")
        conn.commit()
    except Exception as e:
        print(f"Failed to drop tables due to: {e}")
    finally:
        cur.close()

def print_streaming_status():
    global streaming_status
    for pair, status in streaming_status.items():
        print(f"{pair}: {status}")

def fetch_last_update_info(conn, asset_code, asset_issuer, counter_asset_code, counter_asset_issuer, data_source):
    """
    Fetch the last_update_time and last_paging_token from the specified update log.
    
    conn: Database connection object
    asset_code, asset_issuer, counter_asset_code, counter_asset_issuer: Asset pair identifiers
    data_source: A string indicating which log to query ('raw_trade_update_log' or 'candle_update_log')
    
    Returns a tuple: (last_update_time, last_paging_token)
    """
    cur = conn.cursor()
    query = f"""
        SELECT last_update_time, last_paging_token FROM {data_source}
        WHERE base_asset_code = %s AND base_asset_issuer = %s 
        AND counter_asset_code = %s AND counter_asset_issuer = %s
        ORDER BY last_update_time DESC LIMIT 1;
    """
    cur.execute(query, (asset_code, asset_issuer, counter_asset_code, counter_asset_issuer))
    result = cur.fetchone()
    cur.close()
    if result:
        return result[0], result[1]  # last_update_time, last_paging_token
    else:
        return None, None
    
def update_all_pairs_historical_data_and_candles(conn):
    info_logger.info("Starting historical data and candle update for all pairs in database.")

    cur = conn.cursor(cursor_factory=DictCursor)
    cur.execute("SELECT base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer FROM trading_pairs")
    pairs = cur.fetchall()
    
    for pair in pairs:
        update_historical_and_candle_data_for_pair(conn, pair['base_asset_code'], pair['base_asset_issuer'], pair['counter_asset_code'], pair['counter_asset_issuer'])

    cur.close()
    info_logger.info("Historical data update for all pairs complete.")

def update_trade_if_necessary(cur, stellar_trade_id, trade, existing_trade):
    """
    Dynamically check each field in a trade record and update it if necessary.

    :param cur: Database cursor.
    :param trade_id: The unique ID for the trade in the database.
    :param trade_data: Dictionary of trade data fetched from the Stellar network.
    :param existing_trade_data: Dictionary of existing trade data in the database.
    """
    update_fields = []
    update_values = []

    # Define the list of fields to check based on the Stellar trade data structure
    fields_to_check = ['price', 'base_amount', 'counter_amount', 'timestamp', 'base_is_seller', 'stellar_trade_id',
                       'base_asset_type', 'base_asset_code', 'base_asset_issuer',
                       'counter_asset_type', 'counter_asset_code', 'counter_asset_issuer']

    for field in fields_to_check:
        if field in trade and (not existing_trade[field] or existing_trade[field] is None):
            update_fields.append(field)
            update_values.append(trade[field])

    if update_fields:
        # Construct the SQL update statement dynamically
        set_clause = ', '.join([f"{field} = %s" for field in update_fields])
        sql = f"UPDATE historical_trades SET {set_clause} WHERE stellar_trade_id = %s"
        cur.execute(sql, update_values + [stellar_trade_id])
        app_logger.info(f"Updated trade {stellar_trade_id} for field(s): {', '.join(update_fields)}")

def insert_trade_into_db(conn, trade, base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer):
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    try:
        # Parse the timestamp and ensure it is timezone-aware
        timestamp = parse(trade['ledger_close_time'])
        if timestamp.tzinfo is None or timestamp.tzinfo.utcoffset(timestamp) is None:
            timestamp = timestamp.replace(tzinfo=pytz.UTC)

        # Calculate the price from the price components
        price = float(trade['price']['n']) / float(trade['price']['d'])

        # Insert or update trading pair information
        cur.execute("""
            INSERT INTO trading_pairs (base_asset_type, base_asset_code, base_asset_issuer, counter_asset_type, counter_asset_code, counter_asset_issuer)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (base_asset_type, base_asset_code, base_asset_issuer, counter_asset_type, counter_asset_code, counter_asset_issuer)
            DO NOTHING;
        """, (
            'native' if base_asset_code == 'XLM' else 'credit_alphanum4', base_asset_code, base_asset_issuer or 'none',
            'native' if counter_asset_code == 'XLM' else 'credit_alphanum4', counter_asset_code, counter_asset_issuer or 'none'
        ))

        # Insert the new trade into the historical_trades table if not exists
        cur.execute("""
            INSERT INTO historical_trades (
                stellar_trade_id, base_asset_type, base_asset_code, base_asset_issuer, 
                counter_asset_type, counter_asset_code, counter_asset_issuer, 
                timestamp, price, base_amount, counter_amount, base_is_seller
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (stellar_trade_id) DO NOTHING;
        """, (
            trade['id'], 'native' if base_asset_code == 'XLM' else 'credit_alphanum4', base_asset_code, base_asset_issuer or 'none',
            'native' if counter_asset_code == 'XLM' else 'credit_alphanum4', counter_asset_code, counter_asset_issuer or 'none',
            timestamp, price, trade['base_amount'], trade['counter_amount'], trade['base_is_seller']
        ))

        # Update the raw_trade_update_log with the most recent data
        cur.execute("""
            INSERT INTO raw_trade_update_log (base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, last_update_time, last_paging_token)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer)
            DO UPDATE SET last_update_time = EXCLUDED.last_update_time, last_paging_token = EXCLUDED.last_paging_token;
        """, (
            base_asset_code, base_asset_issuer or 'none', counter_asset_code, counter_asset_issuer or 'none', 
            timestamp, trade['paging_token']
        ))

        conn.commit()
    except Exception as e:
        error_logger.error(f"Failed to insert trade or update logs: {e}")
        conn.rollback()
    finally:
        cur.close()

def update_historical_and_candle_data_for_pair(conn, base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer):
    server_url = "https://horizon.stellar.org"
    server = Server(horizon_url=server_url)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    # Determine the start and end times for fetching historical trades
    end_time = datetime.now(pytz.UTC)
    start_time = end_time - timedelta(days=30)

    # Create Asset objects for the Stellar SDK calls
    base_asset = Asset.native() if base_asset_code == 'XLM' else Asset(base_asset_code, base_asset_issuer)
    counter_asset = Asset.native() if counter_asset_code == 'XLM' else Asset(counter_asset_code, counter_asset_issuer)
    
    # Fetch historical trades from the Stellar network
    trades = fetch_historical_data(server, base_asset, counter_asset, start_time, end_time, max_records=5000000)

    for trade in trades:
        stellar_trade_id = trade['id']
        # Check if the trade exists in the database and update or insert accordingly
        cur.execute("""
            SELECT * FROM historical_trades WHERE stellar_trade_id = %s
        """, (stellar_trade_id,))
        existing_trade = cur.fetchone()
        
        if existing_trade:
            # If the trade exists, check for null values and update if necessary
            update_trade_if_necessary(cur, stellar_trade_id, trade, existing_trade)
            insert_trade_into_db(conn, trade, base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer)
        else:
            # If the trade doesn't exist, insert it
            insert_trade_into_db(conn, trade, base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer)

    last_processed_timestamp, _ = fetch_last_update_info(conn, base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, data_source='raw_trade_update_log')
    candles = generate_and_update_candle_data_for_all_intervals(conn, base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, last_processed_timestamp)
    update_candle_data_for_pair_and_log(conn, base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, candles)
    
    # Commit the transaction to save changes to the database
    conn.commit()
    cur.close()

def update_candle_data_for_pair_and_log(conn, base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, candles):
    cur = conn.cursor()
    
    for candle in candles:
        # Derive interval_label from candle data (assuming interval in seconds is part of your candle dict)
        interval_seconds = (candle['interval_end'] - candle['interval_start']).total_seconds()
        interval_label = f"{interval_seconds//60}m" if interval_seconds < 3600 else f"{interval_seconds//3600}h"
        
        try:
            # Insert or update candle data in candle_data table
            cur.execute("""
                INSERT INTO candle_data (
                    base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, interval, timestamp, open, close, high, low
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, interval, timestamp)
                DO UPDATE SET open = EXCLUDED.open, close = EXCLUDED.close, high = EXCLUDED.high, low = EXCLUDED.low
            """, (
                base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, 
                interval_label, candle['interval_end'], candle['opening_price'], candle['closing_price'], 
                candle['highest_price'], candle['lowest_price']
            ))

            # Assume fetch_last_update_info() updates last_paging_token and last_update_time correctly elsewhere
            
            # Update candle_update_log for this interval
            cur.execute("""
                INSERT INTO candle_update_log (
                    base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, interval, last_update_time, last_paging_token
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, interval)
                DO UPDATE SET last_update_time = EXCLUDED.last_update_time, last_paging_token = EXCLUDED.last_paging_token
            """, (
                base_asset_code, base_asset_issuer, counter_asset_code, counter_asset_issuer, 
                interval_label, datetime.now(pytz.UTC), candle['last_paging_token']
            ))

            conn.commit()
        except Exception as e:
            error_logger.error(f"Failed to insert/update candle data for interval {interval_label}: {e}")
            conn.rollback()
    cur.close()

conn = get_db_connection()
update_all_pairs_historical_data_and_candles(conn)
