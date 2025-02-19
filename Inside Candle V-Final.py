import yfinance as yf
import pandas as pd

def fetch_data(ticker, start, end, interval="5m"):
    data = yf.download(ticker, start=start, end=end, interval=interval)
    if data.empty:
        print(f"No data found for {ticker}. Check the ticker or date range.")
        return None

    # Flatten MultiIndex if it exists
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = ['_'.join(col).strip() if col[1] else col[0] for col in data.columns]

    data = data.reset_index()
    data.rename(columns={'Datetime': 'Date'}, inplace=True)
    data['Date'] = pd.to_datetime(data['Date'])
    data['Date'] = data['Date'].dt.tz_localize(None)  # Convert to timezone-naive
    return data

def detect_pattern(df, ticker="RELIANCE.NS"):
    if 'Date' not in df.columns:
        print("Error: 'Date' column not found in DataFrame.")
        return None
    
    results = []
    unique_dates = df['Date'].dt.date.unique()
    
    for date in unique_dates:
        daily_data = df[df['Date'].dt.date == date]
        if daily_data.empty:
            continue
        
        # Get previous day's data using a 1-day offset
        prev_day_data = df[df['Date'] < pd.to_datetime(date)].iloc[-1:]
        if prev_day_data.empty:
            continue
        
        prev_close_value = prev_day_data.iloc[-1][f'Close_{ticker}']
        first_open_value = daily_data.iloc[0][f'Open_{ticker}']
        
        print(f"Date: {date}, Prev Close: {prev_close_value}, Today's Open: {first_open_value}")
        
        # Check if there's any gap (non-equality implies a gap)
        if first_open_value != prev_close_value:
            print(f"Gap detected on {date}")
            mother_candle = daily_data.iloc[0]  # First 5-minute candle of the day
            
            # Collect consecutive baby candles immediately after the mother candle
            baby_candles = []
            for i in range(1, len(daily_data)):
                candle = daily_data.iloc[i]
                # Candle is inside the mother candle if its high is lower and its low is higher
                if candle[f'High_{ticker}'] < mother_candle[f'High_{ticker}'] and candle[f'Low_{ticker}'] > mother_candle[f'Low_{ticker}']:
                    baby_candles.append(candle)
                else:
                    break  # Stop if a candle is not fully inside
            
            if baby_candles:
                print(f"Inside candles found on {date}, Count: {len(baby_candles)}")
            
            # Check if we have at least 3 consecutive baby candles
            if len(baby_candles) >= 3:
                breakout_index = 1 + len(baby_candles)  # Mother candle is index 0, baby candles follow
                if breakout_index < len(daily_data):
                    breakout_candle = daily_data.iloc[breakout_index]
                    stop_loss = (mother_candle[f'High_{ticker}'] - mother_candle[f'Low_{ticker}']) / 2
                    target = max(1.5 * stop_loss, breakout_candle[f'Close_{ticker}'] + stop_loss)
                    
                    results.append({
                        'Date': date,
                        'Mother High': mother_candle[f'High_{ticker}'],
                        'Mother Low': mother_candle[f'Low_{ticker}'],
                        'Breakout Price': breakout_candle[f'Close_{ticker}'],
                        'Stop Loss': stop_loss,
                        'Target': target
                    })
    
    return results

def backtest(ticker, start_date, end_date):
    df = fetch_data(ticker, start_date, end_date)
    if df is None:
        return
    
    print(df.head())  # Debugging: Check data
    print(df.columns)  # Debugging: Check available columns
    print(df['Date'].dt.date.unique())  # Debugging: Check available dates
    
    patterns = detect_pattern(df, ticker)
    if not patterns:
        print("No patterns detected.")
        return
    
    results_df = pd.DataFrame(patterns)
    results_df.to_csv("backtest_results.csv", index=False)
    print("Backtest results saved to backtest_results.csv")

# Parameters
ticker = "RELIANCE.NS"
start_date = "2025-01-01"
end_date = "2025-02-15"

# Run backtest
backtest(ticker, start_date, end_date)
