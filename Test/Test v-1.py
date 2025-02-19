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

def detect_pattern(df, ticker="RELIANCE.NS", min_gap_pct=0.5):
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

        gap_percent = ((first_open_value - prev_close_value) / prev_close_value) * 100
        
        # Check if there's a gap and if it's above the minimum threshold
        if abs(gap_percent) >= min_gap_pct:
            print(f"Gap detected on {date} ({gap_percent:.2f}%)")
            mother_candle = daily_data.iloc[0]  # First 5-minute candle of the day
            
            baby_candles = []
            for i in range(1, len(daily_data)):
                candle = daily_data.iloc[i]
                if candle[f'High_{ticker}'] < mother_candle[f'High_{ticker}'] and candle[f'Low_{ticker}'] > mother_candle[f'Low_{ticker}']:
                    baby_candles.append(candle)
                else:
                    break  # Stop if a candle is not fully inside
            
            if len(baby_candles) >= 3:
                breakout_index = 1 + len(baby_candles)
                if breakout_index < len(daily_data):
                    breakout_candle = daily_data.iloc[breakout_index]
                    stop_loss = (mother_candle[f'High_{ticker}'] - mother_candle[f'Low_{ticker}']) / 2
                    target = max(1.5 * stop_loss, breakout_candle[f'Close_{ticker}'] + stop_loss)
                    
                    results.append({
                        'Ticker': ticker,
                        'Date': date,
                        'Gap %': gap_percent,
                        'Mother High': mother_candle[f'High_{ticker}'],
                        'Mother Low': mother_candle[f'Low_{ticker}'],
                        'Breakout Price': breakout_candle[f'Close_{ticker}'],
                        'Stop Loss': stop_loss,
                        'Target': target,
                        'Profit/Loss': breakout_candle[f'Close_{ticker}'] - mother_candle[f'Open_{ticker}']
                    })
    
    return results

def backtest(tickers, start_date, end_date, min_gap_pct=0.5):
    all_results = []

    for ticker in tickers:
        print(f"\nRunning backtest for {ticker}...")
        df = fetch_data(ticker, start_date, end_date)
        if df is None:
            continue

        patterns = detect_pattern(df, ticker, min_gap_pct)
        if not patterns:
            print(f"No patterns detected for {ticker}.")
            continue
        
        all_results.extend(patterns)
    
    if all_results:
        results_df = pd.DataFrame(all_results)
        results_df.to_csv("backtest_results.csv", index=False)
        print("\nBacktest results saved to backtest_results.csv")
        
        # Performance Metrics
        total_trades = len(results_df)
        winning_trades = len(results_df[results_df['Profit/Loss'] > 0])
        losing_trades = total_trades - winning_trades
        success_rate = (winning_trades / total_trades) * 100 if total_trades > 0 else 0
        total_profit = results_df['Profit/Loss'].sum()

        print("\n🔹 Backtest Performance Summary 🔹")
        print(f"Total Trades: {total_trades}")
        print(f"Winning Trades: {winning_trades} ({success_rate:.2f}% win rate)")
        print(f"Losing Trades: {losing_trades}")
        print(f"Total Profit/Loss: {total_profit:.2f}")

# Parameters
tickers = ["RELIANCE.NS", "TCS.NS", "INFY.NS"]  # Multiple stocks
start_date = "2025-01-01"
end_date = "2025-02-15"
min_gap_pct = 0.5  # Minimum gap % required

# Run backtest
backtest(tickers, start_date, end_date, min_gap_pct)
