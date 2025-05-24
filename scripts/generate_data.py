import os
import pandas as pd # type: ignore
import random
from faker import Faker # type: ignore
from datetime import datetime

# Initialize Faker
fake = Faker()

# Ensure raw data folder exists
os.makedirs('data/raw', exist_ok=True)

# Number of transactions to generate
Numbers = 20

# Generate fake transaction data


def generate_transaction(n):
    transactions = []
    for _ in range(n):  # The line for _ in range(n): is a Python loop that means: "Do something n times", where n is a number. The underscore _ is used as a placeholder for a variable that won't be used inside the loop.
        transaction = {
            "transaction_id": fake.uuid4(),
            "user_id": fake.random_int(min=1000, max=9999),
            # generates a random floating-point number between two specified values
            "amount": round(random.uniform(10.0, 1000.0), 2),
            "timestamp": fake.date_time_this_month().strftime("%Y-%m-%d %H:%M:%S"),
            "product": fake.word(),
            "payment_method": random.choice(["Credit Card", "Debit Card", "UPI", "Wallet"])
        }
        transactions.append(transaction)
    # This creates a DataFrame, which is a two-dimensional, tabular data structure (like a spreadsheet or SQL table).
    return pd.DataFrame(transactions)


print(generate_transaction(10))


# Save to CSV with timestamp in filename
def save_to_csv(df):
    today_str = datetime.now().strftime("%Y-%m-%d")
    file_path = f"../data/raw/transactions_{today_str}.csv"
    df.to_csv(file_path, index=False)
    print(f"Generated {len(df)} transactions and saved to {file_path}")


if __name__ == "__main__":
    df = generate_transaction(15)
    save_to_csv(df)
