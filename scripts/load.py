# load.py script to load the transformed transaction data into a local SQLite database (transactions.db) using sqlite3.

import sqlite3
import pandas as pd # type: ignore
import os

DB_PATH = '../database/transactions.db'
TABLE_NAME = 'Transactions'



def load_to_sqlite(df, db_path=DB_PATH, table_name=TABLE_NAME):
    conn = sqlite3.connect(db_path)

    try:
#df.to_sql(...): Exports the DataFrame df to a SQL database table.
# table_name: A string with the name of the destination table(e.g. "transactions").
# conn: The database connection object(e.g., from sqlite3.connect() or SQLAlchemy).
# if_exists = "append":
# If the table already exists, it adds(appends) the data to the existing table.
# index = False: Tells Pandas not to write the DataFrame's index as a separate column in the SQL table.
        df.to_sql(table_name, conn, if_exists='append', index=False )
        print(f"Loaded {len(df)} records into {table_name} table")
    except Exception as e:
        print(f"Error while loading the sqlite {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    from ingest import get_latest_file, read_transaaction_file
    from validate import validate_data
    from transform import transform_data

    file_path = get_latest_file("../data/raw/")
    raw_df = read_transaaction_file(file_path)
    valid_df = validate_data(raw_df)
    transformed_df = transform_data(valid_df)
    load_to_sqlite(transformed_df)
