import sys
from backtester import backtest

# Force UTF-8 encoding for Windows terminals
sys.stdout.reconfigure(encoding='utf-8')

# Define the stock ticker symbol and run backtest
ticker = "RELIANCE.NS"
backtest(ticker)
