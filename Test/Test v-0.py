import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
import mplfinance as mpf

def fetch_data(ticker, start, end, interval="5m"):
    data = yf.download(ticker, start=start, end=end, interval=interval)
    if data.empty:
        print(f"No data found for {ticker}. Check the ticker or date range.")
        return None

    # Reset index and rename columns
    data = data.reset_index()
    data.rename(columns={'Datetime': 'Date'}, inplace=True)
    data['Date'] = pd.to_datetime(data['Date'])
    return data

def detect_pattern(df, ticker):
    if 'Date' not in df.columns:
        print("Error: 'Date' column not found in DataFrame.")
        return None
    
    results = []
    unique_dates = df['Date'].dt.date.unique()
    
    for date in unique_dates:
        daily_data = df[df['Date'].dt.date == date]
        if daily_data.empty:
            continue
        
        prev_day_data = df[df['Date'].dt.date == (pd.to_datetime(date) - pd.Timedelta(days=1)).date()]
        if prev_day_data.empty:
            continue
        
        prev_close_value = prev_day_data.iloc[-1]['Close']
        first_open_value = daily_data.iloc[0]['Open']

        # Ensure values are scalars
        if isinstance(prev_close_value, pd.Series):
            prev_close_value = prev_close_value.iloc[-1]
        if isinstance(first_open_value, pd.Series):
            first_open_value = first_open_value.iloc[-1]

        if first_open_value != prev_close_value:
            mother_candle = daily_data.iloc[0]
            
            baby_candles = []
            for i in range(1, len(daily_data)):
                candle = daily_data.iloc[i]

                # Ensure High and Low are scalars
                high_value = candle['High'].iloc[0] if isinstance(candle['High'], pd.Series) else candle['High']
                low_value = candle['Low'].iloc[0] if isinstance(candle['Low'], pd.Series) else candle['Low']
                mother_high = mother_candle['High'].iloc[0] if isinstance(mother_candle['High'], pd.Series) else mother_candle['High']
                mother_low = mother_candle['Low'].iloc[0] if isinstance(mother_candle['Low'], pd.Series) else mother_candle['Low']

                if high_value < mother_high and low_value > mother_low:
                    baby_candles.append(candle)
                else:
                    break
            
            if len(baby_candles) >= 3:
                breakout_index = 1 + len(baby_candles)
                if breakout_index < len(daily_data):
                    breakout_candle = daily_data.iloc[breakout_index]
                    stop_loss = (mother_high - mother_low) / 2
                    target = max(1.5 * stop_loss, breakout_candle['Close'] + stop_loss)
                    
                    results.append({
                        'Date': date,
                        'Mother High': mother_high,
                        'Mother Low': mother_low,
                        'Breakout Price': breakout_candle['Close'],
                        'Stop Loss': stop_loss,
                        'Target': target
                    })
    
    return results

def plot_trades(df, ticker, results):
    for result in results:
        date = result['Date']
        daily_data = df[df['Date'].dt.date == date]
        
        fig, ax = plt.subplots(figsize=(10, 5))
        mpf.plot(daily_data.set_index('Date'), type='candle', style='charles', ax=ax)
        ax.axhline(result['Mother High'], color='red', linestyle='--', label='Mother High')
        ax.axhline(result['Mother Low'], color='blue', linestyle='--', label='Mother Low')
        ax.axhline(result['Breakout Price'], color='green', linestyle='-', label='Breakout')
        ax.legend()
        plt.title(f"Trade Setup on {date} for {ticker}")
        plt.show()

def backtest(ticker, start_date, end_date):
    df = fetch_data(ticker, start_date, end_date)
    if df is None:
        return
    
    patterns = detect_pattern(df, ticker)
    if not patterns:
        print("No patterns detected.")
        return
    
    results_df = pd.DataFrame(patterns)
    results_df.to_csv(f"backtest_results_{ticker}.csv", index=False)
    print(f"Backtest results saved to backtest_results_{ticker}.csv")
    
    plot_trades(df, ticker, patterns)

def run_multiple_stocks(tickers, start_date, end_date):
    for ticker in tickers:
        print(f"Running backtest for {ticker}...")
        backtest(ticker, start_date, end_date)

# Parameters
tickers = ["RELIANCE.NS", "TCS.NS", "INFY.NS"]
start_date = "2025-01-01"
end_date = "2025-02-15"

# Run backtest for multiple stocks
run_multiple_stocks(tickers, start_date, end_date)
