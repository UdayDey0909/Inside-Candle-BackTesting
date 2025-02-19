import yfinance as yf
import pandas as pd
import sys

# Fix encoding error for Windows terminal
sys.stdout.reconfigure(encoding='utf-8')

def fetch_data(ticker, start, end, interval="5m"):
    data = yf.download(ticker, start=start, end=end, interval=interval)
    
    if data.empty:
        print(f"❌ No data found for {ticker}. Check ticker or date range.")
        return None

    # Flatten MultiIndex if it exists
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = ['_'.join(col).strip() for col in data.columns]

    data = data.reset_index()
    data.rename(columns={'Datetime': 'Date'}, inplace=True)
    data['Date'] = pd.to_datetime(data['Date'])
    data['Date'] = data['Date'].dt.tz_localize(None)  # Convert to timezone-naive

    # Fix column names (detect actual column names for the given ticker)
    ticker_suffix = f"_{ticker}"
    column_mapping = {col: col.replace(ticker_suffix, '') for col in data.columns if ticker_suffix in col}
    data.rename(columns=column_mapping, inplace=True)

    # Debugging: Print available columns
    print(f"\n✅ Data fetched for {ticker}. Available columns: {list(data.columns)}")

    return data

def detect_pattern(df, ticker="RELIANCE.NS"):
    required_columns = ['Date', 'Close', 'Open', 'High', 'Low']
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        print(f"❌ Missing expected columns in {ticker} data: {list(df.columns)}")
        return None

    results = []
    unique_dates = df['Date'].dt.date.unique()

    print(f"📊 Detecting patterns for {ticker}... Total Days: {len(unique_dates)}")

    for date in unique_dates:
        daily_data = df[df['Date'].dt.date == date]
        print(f"\n🔍 Processing {date} - Total Rows: {len(daily_data)}")

        if daily_data.empty:
            continue
        
        # Get previous day's last row
        prev_day_data = df[df['Date'] < pd.to_datetime(date)]
        
        if prev_day_data.empty:
            print(f"⚠️ No previous day data for {date}, skipping...")
            continue
        
        prev_close_value = prev_day_data.iloc[-1]['Close']
        first_open_value = daily_data.iloc[0]['Open']

        print(f"📈 Prev Close: {prev_close_value}, Today's Open: {first_open_value}")

        # Check if there's a gap
        if first_open_value != prev_close_value:
            print(f"✅ Gap detected on {date}")

            mother_candle = daily_data.iloc[0]  # First 5-minute candle
            baby_candles = []

            for i in range(1, len(daily_data)):
                candle = daily_data.iloc[i]
                if candle['High'] < mother_candle['High'] and candle['Low'] > mother_candle['Low']:
                    baby_candles.append(candle)
                else:
                    break  # Stop if candle is outside

            print(f"🕯️ Inside candles found: {len(baby_candles)}")
            
            if len(baby_candles) >= 3:
                breakout_index = 1 + len(baby_candles)
                if breakout_index < len(daily_data):
                    breakout_candle = daily_data.iloc[breakout_index]
                    stop_loss = (mother_candle['High'] - mother_candle['Low']) / 2
                    target = max(1.5 * stop_loss, breakout_candle['Close'] + stop_loss)

                    results.append({
                        'Date': date,
                        'Mother High': mother_candle['High'],
                        'Mother Low': mother_candle['Low'],
                        'Breakout Price': breakout_candle['Close'],
                        'Stop Loss': stop_loss,
                        'Target': target
                    })
                else:
                    print(f"❌ No breakout found for {date}")

    print(f"\n✅ Found {len(results)} valid breakout patterns for {ticker}.")
    return results

def backtest(ticker, start_date, end_date):
    df = fetch_data(ticker, start_date, end_date)
    if df is None:
        return
    
    patterns = detect_pattern(df, ticker)
    if not patterns:
        print("⚠️ No patterns detected.")
        return
    
    results_df = pd.DataFrame(patterns)
    results_df.to_csv("backtest_results.csv", index=False)
    print("\n✅ Backtest results saved to backtest_results.csv")
    print("\n📊 Backtest Performance Summary")  # No encoding issues

# Parameters
ticker = "RELIANCE.NS"
start_date = "2025-01-01"
end_date = "2025-02-15"

# Run backtest
backtest(ticker, start_date, end_date)
