import pandas as pd
import json
import os

RAW_FILE = "data/raw/fakestore_raw.json"
ANALYTICS_PATH = "data/analytics"
OUTPUT_FILE = "fakestore_kpi_dataset.csv"

def load_raw_json(filepath: str):
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data

def transform_for_kpis(raw_data):
    # Normalizar JSON a DataFrame
    df = pd.json_normalize(raw_data)

    # Renombrar columnas para BI
    df.rename(columns={
        "id": "product_id",
        "title": "product_name",
        "price": "product_price",
        "category": "product_category"
    }, inplace=True)

    # Data quality
    df = df.dropna(subset=["product_price", "product_category"])

    # Tipos correctos
    df["product_price"] = df["product_price"].astype(float)

    # KPI 1: Segmentación de precios
    df["price_bucket"] = pd.cut(
        df["product_price"],
        bins=[0, 20, 50, 100, 500],
        labels=["Low", "Medium", "High", "Premium"]
    )

    # KPI 2: Marca de producto caro
    df["is_expensive"] = df["product_price"] > 50

    # KPI 3: Métrica técnica de calidad
    df["price_not_null"] = df["product_price"].notnull()

    # Dataset final orientado a Looker
    kpi_df = df[[
        "product_id",
        "product_name",
        "product_category",
        "product_price",
        "price_bucket",
        "is_expensive",
        "price_not_null"
    ]]

    return kpi_df

def save_kpi_dataset(df: pd.DataFrame, filename: str):
    os.makedirs(ANALYTICS_PATH, exist_ok=True)
    filepath = os.path.join(ANALYTICS_PATH, filename)
    df.to_csv(filepath, index=False)
    print(f"KPI dataset saved to {filepath}")

if __name__ == "__main__":
    # 1. Load RAW layer
    raw_data = load_raw_json(RAW_FILE)

    # 2. Transform into analytics/KPI dataset
    kpi_df = transform_for_kpis(raw_data)

    # 3. Save analytics layer
    save_kpi_dataset(kpi_df, OUTPUT_FILE)

    # 4. Quick validation
    print(kpi_df.head())
