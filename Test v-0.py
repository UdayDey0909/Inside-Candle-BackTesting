import yfinance as yf
import pandas as pd

# Fetch historical data
def fetch_data(ticker, start, end, interval="5m"):
    data = yf.download(ticker, start=start, end=end, interval=interval)
    if data.empty:
        print(f"No data found for {ticker}. Check the ticker or date range.")
        return None
    data = data.reset_index()
    data.rename(columns={'Datetime': 'Date'}, inplace=True)
    data['Date'] = pd.to_datetime(data['Date'])
    return data

# Detect gap-up/gap-down and inside candles
def detect_pattern(df):
    if 'Date' not in df.columns:
        print("Error: 'Date' column not found in DataFrame.")
        return None
    
    results = []
    df['Prev_Close'] = df['Close'].shift(1)
    unique_dates = df['Date'].dt.date.unique()
    
    for date in unique_dates:
        daily_data = df[df['Date'].dt.date == date]
        if daily_data.empty:
            continue
        
        prev_day_data = df[df['Date'].dt.date < date]
        if prev_day_data.empty:
            continue
        
        prev_close = prev_day_data.iloc[-1]['Close']
        first_candle = daily_data.iloc[0]
        
        if (first_candle['Open'] > prev_close).any() or (first_candle['Open'] < prev_close).any():
            mother_candle = first_candle  # First 5-min candle is mother candle
            baby_candles = daily_data[(daily_data['High'] < mother_candle['High']) & 
                                      (daily_data['Low'] > mother_candle['Low'])]
            
            if len(baby_candles) >= 3:
                breakout_index = len(baby_candles)
                breakout_candle = daily_data.iloc[breakout_index] if breakout_index < len(daily_data) else None
                
                if breakout_candle is not None:
                    stop_loss = (mother_candle['High'] - mother_candle['Low']) / 2
                    target = max(1.5 * stop_loss, breakout_candle['Close'] + stop_loss)
                    
                    results.append({
                        'Date': date,
                        'Gap Type': 'Gap Up' if first_candle['Open'] > prev_close else 'Gap Down',
                        'Mother High': mother_candle['High'],
                        'Mother Low': mother_candle['Low'],
                        'Breakout Price': breakout_candle['Close'],
                        'Stop Loss': stop_loss,
                        'Target': target
                    })
    
    return results

# Backtest function
def backtest(ticker, start_date, end_date):
    df = fetch_data(ticker, start_date, end_date)
    if df is None:
        return
    
    print(df.head())  # Debugging: Check data
    patterns = detect_pattern(df)
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
