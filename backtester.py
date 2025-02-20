import pandas as pd
from datetime import datetime, timedelta
from data_fetcher import fetch_data
from pattern_detector import detect_gap_patterns

def backtest(ticker):
    """Runs the backtest on the last 59 days of data."""
    end_date = datetime.today().strftime('%Y-%m-%d')
    start_date = (datetime.today() - timedelta(days=59)).strftime('%Y-%m-%d')
    
    df = fetch_data(ticker, start_date, end_date)
    if df is None:
        return
    
    patterns = detect_gap_patterns(df)
    if not patterns:
        print("⚠️ No patterns detected.")
        return
    
    results_df = pd.DataFrame(patterns)
    results_df.to_csv("backtest_results.csv", index=False)
    print("✅ Backtest results saved to backtest_results.csv")
