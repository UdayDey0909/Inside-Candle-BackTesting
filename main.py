from backtester import backtest
import sys

# Force UTF-8 encoding for Windows terminals
sys.stdout.reconfigure(encoding='utf-8')

from backtester import backtest

ticker = "RELIANCE.NS"
backtest(ticker)

# Define the stock ticker symbol
ticker = "RELIANCE.NS"

# Run backtest
backtest(ticker)
