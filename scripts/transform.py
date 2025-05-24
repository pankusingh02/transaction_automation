# transform.py
# It enriches the validated transaction data with additional computed columns such as day_of_week and is_weekend.

import pandas as pd # type: ignore


def transform_data(df):
    df["day_of_week"] = df["timestamp"].dt.day_name()

    df["is_weekend"] = df["day_of_week"].isin(["Saturday", "Sunday"])

    df["category"] = pd.cut(
        df["amount"],
        bins=[0, 100, 500, 1000],
        labels=["low", "medium", "high"],
        include_lowest=True,
    )
    # Here's what each part does:
    # pd.cut(...): Splits the continuous numeric "amount" data into bins(intervals).
    # bins = [0, 100, 500, 1000]: Defines the edges of the bins:
    # (0, 100] → "Low"
    # (100, 500] → "Medium"
    # (500, 1000] → "High"
    # labels = ["Low", "Medium", "High"]: Assigns category labels to each bin.
    # include_lowest = True: Ensures that amounts equal to 0 are included in the first bin(Low).

    print("Transformation complete. Added columns: day_of_week, is_weekend, category")
    return df


if __name__ == "__main__":
    from ingest import get_latest_file, read_transaaction_file
    from validate import validate_data

    file_path = get_latest_file("../data/raw/")
    raw_df = read_transaaction_file(file_path)
    valid_df = validate_data(raw_df)
    transformed_df = transform_data(valid_df)

    print(transformed_df.head(15))
