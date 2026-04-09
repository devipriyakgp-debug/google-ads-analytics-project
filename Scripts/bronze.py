# bronze.py

import pandas as pd
import os

# Input & Output Paths
INPUT_FILE = "data/source/google_ads_data.csv"
OUTPUT_PATH = "data/bronze/google_ads_raw.parquet"

def ingest_data():
    print("Reading raw Google Ads data...")
    
    df = pd.read_csv(INPUT_FILE)
    
    print("Data Preview:")
    print(df.head())

    # Create folder if not exists
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    # Save as parquet (data lake format)
    df.to_parquet(OUTPUT_PATH, index=False)

    print(f"Raw data saved to {OUTPUT_PATH}")

if __name__ == "__main__":
    ingest_data()
