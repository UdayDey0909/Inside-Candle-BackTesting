import yfinance as yf
import pandas as pd
from database import insert_stock_data, get_stock_data

def fetch_data(ticker, start, end, interval="5m"):
    """Fetch historical 5-minute data from MongoDB or Yahoo Finance."""
    print(f"📅 Checking MongoDB for {ticker} data from {start} to {end}...")

    # Check if data already exists in MongoDB
    existing_data = get_stock_data(ticker, start, end)
    if existing_data is not None and not existing_data.empty:
        print("✅ Data loaded from MongoDB.")
    else:
        print("📡 Data not found in MongoDB. Fetching from Yahoo Finance...")
        try:
            existing_data = yf.download(ticker, start=start, end=end, interval=interval)
        except Exception as e:
            print(f"❌ Error fetching data for {ticker}: {e}")
            return None

        if existing_data is None or existing_data.empty:
            print(f"❌ No data found for {ticker}. Check ticker or date range.")
            return None

        # Convert index to a column
        existing_data.reset_index(inplace=True)

        # Rename Datetime column to Date if present
        if "Datetime" in existing_data.columns:
            existing_data.rename(columns={"Datetime": "Date"}, inplace=True)

        # Flatten MultiIndex columns (Fix for tuple keys issue)
        if isinstance(existing_data.columns, pd.MultiIndex):
            existing_data.columns = ['_'.join([str(c) for c in col if c]) for col in existing_data.columns]

        if "Date" not in existing_data.columns:
            print("❌ Error: 'Date' column missing after processing!")
            return None

        # Convert Date column to datetime and remove timezone
        existing_data["Date"] = pd.to_datetime(existing_data["Date"]).dt.tz_localize(None)

        # ✅ Fix: Extract correct float values if nested dictionaries exist
        for col in ["Open", "High", "Low", "Close", "Volume"]:
            if col in existing_data.columns:
                existing_data[col] = existing_data[col].apply(lambda x: x[list(x.keys())[0]] if isinstance(x, dict) else x)

        # Store new data in MongoDB
        insert_stock_data(ticker, existing_data)

        print(f"✅ Data fetched and stored in MongoDB. {len(existing_data)} rows loaded.")

    # Print column names to debug missing columns
    print("📊 Columns retrieved from MongoDB:", existing_data.columns)

    return existing_data
