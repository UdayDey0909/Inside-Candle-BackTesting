import yfinance as yf
import pandas as pd

def fetch_data(ticker, start, end, interval="5m"):
    """Fetch historical 5-minute data from Yahoo Finance."""
    print(f"📅 Fetching data for {ticker} from {start} to {end}...")

    try:
        data = yf.download(ticker, start=start, end=end, interval=interval)
    except Exception as e:
        print(f"❌ Error fetching data for {ticker}: {e}")
        return None
    
    if data is None or data.empty:
        print(f"❌ No data found for {ticker}. Check ticker or date range.")
        return None

    print("📊 Columns received:", data.columns)

    # Flatten MultiIndex if necessary
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = ['_'.join([str(item) for item in col if item]).strip() for col in data.columns]

    # Reset index if it's a DateTimeIndex
    if isinstance(data.index, pd.DatetimeIndex):
        data.reset_index(inplace=True)

    # Ensure 'Date' column exists (sometimes it is named 'Datetime')
    if 'Datetime' in data.columns:
        data.rename(columns={'Datetime': 'Date'}, inplace=True)

    if 'Date' not in data.columns:
        print("❌ Error: 'Date' column missing after processing!")
        return None  # Prevent further errors

    # Convert 'Date' column to datetime and remove timezone
    data['Date'] = pd.to_datetime(data['Date']).dt.tz_localize(None)

    # Remove any unwanted ticker suffix from column names
    suffix = f"_{ticker}"
    data.columns = [col.replace(suffix, "") if isinstance(col, str) and col.endswith(suffix) else col for col in data.columns]

    print(f"✅ Data fetched for {ticker}. {len(data)} rows loaded.")
    
    return data
