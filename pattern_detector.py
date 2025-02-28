import pandas as pd

def detect_gap_patterns(df):
    """Identify gap-up/gap-down breakouts with inside candles and implement a dynamic trailing stop-loss."""
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

        gap_amount = round(first_open - prev_close, 2)
        gap_percentage = round((gap_amount / prev_close) * 100, 2)

        if first_open == prev_close:
            continue  # No gap detected, skip this day

        mother_candle = daily_data.iloc[0]
        
        baby_candles = []
        for i in range(1, len(daily_data)):
            candle = daily_data.iloc[i]
            if candle['High'] < mother_candle['High'] and candle['Low'] > mother_candle['Low']:
                baby_candles.append(candle)
            else:
                break  # Keep original early exit behavior
        
        if len(baby_candles) < 3:
            continue  # Not enough inside candles

        breakout_index = 1 + len(baby_candles)
        if breakout_index >= len(daily_data) - 1:
            continue  # Avoid out-of-range errors

        breakout_candle = daily_data.iloc[breakout_index]
        next_candle = daily_data.iloc[breakout_index + 1] if breakout_index + 1 < len(daily_data) else None

        # Determine trade direction
        breakout_side, stop_loss, target, trailing_stop = calculate_trade_params(mother_candle, breakout_candle, next_candle)
        if not breakout_side:
            continue  # No valid breakout

        trade_result, hit_target_count = simulate_trade(daily_data, breakout_index, breakout_side, stop_loss, target, trailing_stop)

        results.append({
            'Date': date,
            'Gap': f"{gap_amount} ({gap_percentage}%)",
            'High': round(mother_candle['High'], 2),
            'Low': round(mother_candle['Low'], 2),
            'Breakout Price': round(breakout_candle['Close'], 2),
            'Breakout Side': breakout_side,
            'Stop Loss': stop_loss,
            'Target': target,
            'Trailing Stop': trailing_stop,
            'Target Hits': hit_target_count,
            'Result': trade_result
        })

    print(f"✅ {len(results)} valid breakout patterns found.")
    return results


def calculate_trade_params(mother_candle, breakout_candle, next_candle):
    """Calculate trade direction, stop-loss, target, and trailing stop levels."""
    breakout_side, stop_loss, target, trailing_stop = None, None, None, None
    mother_range = round(mother_candle['High'] - mother_candle['Low'], 2)
    risk_amount = round(mother_range / 2, 2)

    if breakout_candle['High'] > mother_candle['High'] and next_candle is not None and next_candle['Open'] > mother_candle['High']:
        breakout_side = "Buy"
        stop_loss = round(breakout_candle['Close'] - risk_amount, 2)
        target = round(breakout_candle['Close'] + (1.5 * risk_amount), 2)
        trailing_stop = stop_loss
    elif breakout_candle['Low'] < mother_candle['Low'] and next_candle is not None and next_candle['Open'] < mother_candle['Low']:
        breakout_side = "Short"
        stop_loss = round(breakout_candle['Close'] + risk_amount, 2)
        target = round(breakout_candle['Close'] - (1.5 * risk_amount), 2)
        trailing_stop = stop_loss

    return breakout_side, stop_loss, target, trailing_stop


def simulate_trade(daily_data, breakout_index, breakout_side, stop_loss, target, trailing_stop):
    """Simulate trade execution and track target hits or stop-loss triggers."""
    trade_result = "Sideways"
    hit_target_count = 0  # Track number of times target is hit

    for i in range(breakout_index + 1, len(daily_data)):
        candle = daily_data.iloc[i]

        if breakout_side == "Buy":
            if candle['Low'] <= stop_loss:
                trade_result = f"Stop-Loss Hit at {stop_loss}"
                break
            if candle['High'] >= target:
                trade_result = "Target Hit"
                hit_target_count += 1
                target = round(target + (1.5 * (target - stop_loss)), 2)  # Move target higher
                trailing_stop = round(trailing_stop + (0.5 * (target - stop_loss)), 2)
            if trailing_stop is not None and candle['Low'] <= trailing_stop:
                trade_result = f"Trailing SL Hit at {trailing_stop}"
                break
        else:  # Short trade
            if candle['High'] >= stop_loss:
                trade_result = f"Stop-Loss Hit at {stop_loss}"
                break
            if candle['Low'] <= target:
                trade_result = "Target Hit"
                hit_target_count += 1
                target = round(target - (1.5 * (stop_loss - target)), 2)  # Move target lower
                trailing_stop = round(trailing_stop - (0.5 * (stop_loss - target)), 2)
            if trailing_stop is not None and candle['High'] >= trailing_stop:
                trade_result = f"Trailing SL Hit at {trailing_stop}"
                break

    return trade_result, hit_target_count
