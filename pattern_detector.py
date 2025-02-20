import pandas as pd

def detect_gap_patterns(df):
    """Identify gap-up/gap-down breakouts with inside candles."""
    required_columns = ['Date', 'Close', 'Open', 'High', 'Low']
    if not all(col in df.columns for col in required_columns):
        print("❌ Missing required columns in dataset!")
        return []
    
    results = []
    unique_dates = df['Date'].dt.date.unique()
    
    for date in unique_dates:
        daily_data = df[df['Date'].dt.date == date]
        prev_day_data = df[df['Date'] < pd.to_datetime(date)]
        
        if daily_data.empty or prev_day_data.empty:
            continue
        
        prev_close = prev_day_data.iloc[-1]['Close']
        first_open = daily_data.iloc[0]['Open']
        
        # Calculate Gap Amount and Percentage
        gap_amount = first_open - prev_close
        gap_percentage = (gap_amount / prev_close) * 100

        if first_open == prev_close:
            continue  # No gap detected, skip this day
        
        mother_candle = daily_data.iloc[0]
        
        baby_candles = []
        for i in range(1, len(daily_data)):
            candle = daily_data.iloc[i]
            if candle['High'] < mother_candle['High'] and candle['Low'] > mother_candle['Low']:
                baby_candles.append(candle)
            else:
                break
        
        if len(baby_candles) >= 3:
            breakout_index = 1 + len(baby_candles)
            if breakout_index < len(daily_data) - 1:
                breakout_candle = daily_data.iloc[breakout_index]
                next_candle = daily_data.iloc[[breakout_index + 1]]  # Ensure next_candle is a DataFrame
                
                stop_loss = (mother_candle['High'] - mother_candle['Low']) / 2
                target = breakout_candle['Close'] + stop_loss * 1.5
                
                breakout_side = "Unknown"
                if not next_candle.empty:
                    if breakout_candle['High'] > mother_candle['High'] and next_candle['Open'].iloc[0] > mother_candle['High']:
                        breakout_side = "Buy"
                    elif breakout_candle['Low'] < mother_candle['Low'] and next_candle['Open'].iloc[0] < mother_candle['Low']:
                        breakout_side = "Short"
                
                results.append({
                    'Date': date,
                    'Gap': f"{round(gap_amount, 2)} ({round(gap_percentage, 2)}%)",
                    'High': mother_candle['High'],
                    'Low': mother_candle['Low'],
                    'Breakout Price': breakout_candle['Close'],
                    'Breakout Side': breakout_side,
                    'Stop Loss': stop_loss,
                    'Target': target

                })
    
    print(f"✅ {len(results)} valid breakout patterns found.")
    return results
