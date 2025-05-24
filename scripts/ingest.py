# This file reads the latest transaction file from the ../data/raw/ folder using pandas.

import os
import pandas as pd # type: ignore
from glob import glob

RAW_DATA_PATH = '../data/raw'

def get_latest_file (path):
    # This uses glob() to find all files in path that match the pattern transactions_*.csv
    # os.path.join(path, "transaction_*.csv") like => data/raw/transaction_*.csv then pass it to glob()
    # file = glob(f"{path}/transactions_*.csv") or 
    files = glob(os.path.join(path, "transactions_*.csv")) 
    if not files:
        raise FileNotFoundError(f"No transaction files found in {path}")
    lates_file= max(files, key=os.path.getmtime)
    return lates_file

def read_transaaction_file(file_path):
    try:
        df=pd.read_csv(file_path)
        print(f"Successfully read file: {file_path}")
        return df
    except Exception as e:
        print(f'failed to read the file at {file_path}: {e}')
        # pd.DataFrame() with no arguments creates an empty DataFrame: Empty DataFrame Columns: [] Index: []
        return pd.DataFrame()
    

# When you run a script directly, Python sets the built-in variable __name__ to "__main__".
# When you import that script as a module into another script, __name__ is set to the script’s filename(not "__main__").

if __name__=="__main__":
    file_path= get_latest_file(RAW_DATA_PATH)
    df= read_transaaction_file(file_path)
    print(df.head())  # Returns the first 5 rows of the DataFrame df.
