# validate.py
# It takes the DataFrame read by ingest.py, performs several checks, and returns a cleaned DataFrame:

# ✅ What It Does
# Ensures required columns are present.
# Drops nulls in critical fields.
# Removes duplicate transaction_ids.
# Parses and filters valid timestamps.


import pandas as pd # type: ignore

REQUIRED_COLUMNS = ["transaction_id", "user_id", "amount", "timestamp"]


def validate_data(df):
    intial_count = len(df)

    # check required columns
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            raise ValueError(f"missing required column {col}")

    # Drop rows with null values in required column.
    df = df.dropna(subset=REQUIRED_COLUMNS)

    # Remove duplicate transaction IDs
    df = df.drop_duplicates(subset="transaction_id")

    # Ensure correct data types
    df["transaction_id"] = df["transaction_id"].astype(str)
    df["user_id"] = df["user_id"].astype(int)
    df["amount"] = df["amount"].astype(float)

    # Convert timestamp to datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    # Drop rows with invalid timestamps
    df = df.dropna(subset=["timestamp"])

    finale_count = len(df)

    print(f"validated data {intial_count} --> {finale_count}")
    return df

if __name__== "__main__":
    from ingest import get_latest_file, read_transaaction_file

    file_path=get_latest_file('../data/raw/')
    raw_df= read_transaaction_file(file_path)
    clean_df=validate_data(raw_df)
    print(clean_df.head())

