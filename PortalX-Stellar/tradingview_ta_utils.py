# tradingview_ta_utils.py

from tradingview_ta import TA_Handler, Interval, Exchange
import tradingview_ta
from logging_config import app_logger

# Show TradingVie_TA version
print("Tradingview_TA version:", tradingview_ta.__version__)

def create_ta_handler_instance():
    # Instantiate TA-Handler
    handler = TA_Handler(
        symbol=input("Enter trading pair (e.g. 'XLMUSD'): "),
        exchange=input("Enter exchange (e.g. 'COINBASE'): "),
        screener=input("Enter screener (e.g. 'crypto'): "),
        interval=input("Enter interval (e.g. '1m' for '1 minute'): "),
        timeout=None
    )

    return handler

def get_ta_handler_analysis(handler):
    
    # Get handler analysis
    analysis = handler.get_analysis()

    return analysis

def print_ta_handler_analysis(handler):
    
    # Get handler analysis
    analysis = handler.get_analysis()

    app_logger.info("Open: %s", analysis.indicators["open"])
    app_logger.info("Close: %s", analysis.indicators["close"])
    app_logger.info("24hr High: %s", analysis.indicators["high"])
    app_logger.info("24hr Low: %s", analysis.indicators["low"])
    app_logger.info("24hr Volume: %s", analysis.indicators["volume"])
    app_logger.info("24hr Change: %s", analysis.indicators["change"])
    app_logger.info("Upper Bollinger Band: %s", analysis.indicators["BB.upper"])
    app_logger.info("Lower Bollinger Band: %s", analysis.indicators["BB.lower"])
    app_logger.info("Moving Averge: %s", analysis.indicators["SMA20"])
    app_logger.info("RSI: %s", analysis.indicators["RSI"])
    app_logger.info("MACD: %s", analysis.indicators["MACD.macd"])

# Create a TA-Handler instance
handler = create_ta_handler_instance()
market_handler = print_ta_handler_analysis(handler)
