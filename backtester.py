import pandas as pd
from datetime import datetime, timedelta
from data_fetcher import fetch_data
from pattern_detector import detect_gap_patterns

def backtest(ticker):
    """Runs the backtest on the last 59 days of data."""
    end_date = datetime.today().date()
    start_date = end_date - timedelta(days=59)
    
    df = fetch_data(ticker, start_date, end_date)
    if df is None or df.empty:
        print(f"❌ Failed to fetch data for {ticker}.")
        return
    
    patterns = detect_gap_patterns(df)
    if not patterns:
        print("⚠️ No patterns detected.")
        return

    # Ensure patterns is a list of dictionaries before creating a DataFrame
    if isinstance(patterns, list) and all(isinstance(p, dict) for p in patterns):
        results_df = pd.DataFrame(patterns)
        results_df.to_csv("backtest_results.csv", index=False)
        print("✅ Backtest results saved to backtest_results.csv")
    else:
        print("❌ Error: Unexpected format for detected patterns.")

