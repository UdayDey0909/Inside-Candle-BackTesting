import yfinance as yf
import pandas as pd

def fetch_data(ticker, start, end, interval="5m"):
    """Fetch historical 5-minute data from Yahoo Finance."""
    print(f"📅 Fetching data for {ticker} from {start} to {end}...")
    data = yf.download(ticker, start=start, end=end, interval=interval)
    
    if data.empty:
        print(f"❌ No data found for {ticker}. Check ticker or date range.")
        return None

    # Reset index to move the datetime index into a column.
    data.reset_index(inplace=True)
    
    # Flatten MultiIndex columns if present.
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = ['_'.join([str(item) for item in col if item]).strip() for col in data.columns.values]
    
    # Ensure the first column is named 'Date'
    if 'Date' not in data.columns:
        data.rename(columns={data.columns[0]: 'Date'}, inplace=True)
    
    # Convert 'Date' column to datetime and make it timezone naive.
    data['Date'] = pd.to_datetime(data['Date']).dt.tz_localize(None)
    
    # Remove the ticker suffix from columns if present.
    ticker_suffix = f"_{ticker}"
    new_columns = {}
    for col in data.columns:
        if col.endswith(ticker_suffix):
            new_columns[col] = col.replace(ticker_suffix, "")
    if new_columns:
        data.rename(columns=new_columns, inplace=True)
    
    print(f"✅ Data fetched for {ticker}. {len(data)} rows loaded.")
    print("Columns:", list(data.columns))
    return data
