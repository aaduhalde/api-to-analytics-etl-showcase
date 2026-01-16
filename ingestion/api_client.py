import requests
import pandas as pd
import json
import os

API_URL = "https://fakestoreapi.com/products"

RAW_PATH = "data/raw"
STAGING_PATH = "data/staging"

def fetch_api_data():
    response = requests.get(API_URL)
    response.raise_for_status()
    return response.json()

def normalize_data(raw_data):
    df = pd.json_normalize(raw_data)
    return df

def save_raw_json(data, filename="fakestore_raw.json"):
    os.makedirs(RAW_PATH, exist_ok=True)
    file_path = os.path.join(RAW_PATH, filename)

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"Raw JSON saved to {file_path}")

def save_csv(df, filename="fakestore_normalized.csv"):
    os.makedirs(STAGING_PATH, exist_ok=True)
    file_path = os.path.join(STAGING_PATH, filename)

    df.to_csv(file_path, index=False)
    print(f"Normalized CSV saved to {file_path}")

if __name__ == "__main__":
    # 1. Extract
    data = fetch_api_data()

    # 2. Save RAW layer
    save_raw_json(data)

    # 3. Normalize
    df = normalize_data(data)

    # 4. Save STAGING layer
    save_csv(df)

    # 5. Quick validation
    print(df.head())
