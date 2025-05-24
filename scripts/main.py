import pandas as pd
from generate_data import generate_transactions, save_to_csv
from ingest import get_latest_file, read_transaaction_file
from validate import validate_data
from transform import transform_data
from load import load_to_sqlite


def run_pipeline():
    print("📥 Ingesting latest transaction file...")
    file_path = get_latest_file("../data/raw/")
    raw_df = read_transaaction_file(file_path)

    print("✅ Validating data...")
    valid_df = validate_data(raw_df)

    print("🧹 Transforming data...")
    transformed_df = transform_data(valid_df)

    print("📦 Loading data into SQLite database...")
    load_to_sqlite(transformed_df)

    print("🚀 Pipeline completed successfully.")


if __name__ == "__main__":
    run_pipeline()


# If you want main.py to always generate a new transaction file automatically, you can add this to the top of main.py:

# print("🧪 Generating new transaction file...")
# df_generated = generate_transactions(100)  # or any number you want
# save_to_csv(df_generated)
