from pymongo import MongoClient
import pandas as pd
from datetime import datetime

def connect_to_db():
    """Establish connection to MongoDB."""
    client = MongoClient("mongodb://localhost:27017/")  # Update if using a remote database
    db = client["stock_data"]  # Database name
    return db

def insert_stock_data(ticker, df):
    """Insert stock data into MongoDB, ensuring numeric values are stored correctly."""
    db = connect_to_db()
    collection = db["reliance_data"]

    # Flatten MultiIndex columns (if any)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ['_'.join([str(c) for c in col if c]) for col in df.columns]

    # Remove suffix like "_RELIANCE" if present
    suffix = f"_{ticker.split('.')[0]}"
    df.columns = [col.replace(suffix, "") if col.endswith(suffix) else col for col in df.columns]

    # ✅ Fix: Extract correct float values if stored as {"NS": value}
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: x["NS"] if isinstance(x, dict) and "NS" in x else x)
            df[col] = df[col].astype(float)  # Convert to float

    records = df.to_dict("records")  # Convert DataFrame to list of dictionaries
    for record in records:
        record["ticker"] = ticker  # Add ticker field
        record["date"] = record.get("Date", None) or record.get("Datetime", None) or datetime.now()
        collection.update_one({"ticker": ticker, "date": record["date"]}, {"$set": record}, upsert=True)

    print(f"✅ {len(records)} records inserted/updated in MongoDB.")

def get_stock_data(ticker, start_date, end_date):
    """Retrieve stock data from MongoDB and fix incorrect data types."""
    db = connect_to_db()
    collection = db["reliance_data"]

    # Convert datetime.date to datetime.datetime (with time set to 00:00:00)
    start_date = datetime.combine(start_date, datetime.min.time())
    end_date = datetime.combine(end_date, datetime.min.time())

    query = {"ticker": ticker, "date": {"$gte": start_date, "$lte": end_date}}
    cursor = collection.find(query, {"_id": 0})
    df = pd.DataFrame(list(cursor))

    if df.empty:
        return None

    df["Date"] = pd.to_datetime(df["date"])  # Ensure Date column is datetime
    df.drop(columns=["date"], inplace=True)  # Drop duplicate date column

    # ✅ Fix: Extract float values after loading from MongoDB
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: x["NS"] if isinstance(x, dict) and "NS" in x else x)
            df[col] = df[col].astype(float)  # Convert to float

    # ✅ Fix: Remove '_RELIANCE' suffix after retrieving from MongoDB
    suffix = f"_{ticker.split('.')[0]}"
    df.columns = [col.replace(suffix, "") if col.endswith(suffix) else col for col in df.columns]

    return df
