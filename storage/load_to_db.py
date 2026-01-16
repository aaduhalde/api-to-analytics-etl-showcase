import pandas as pd
from sqlalchemy import create_engine
import configparser
import os

# Ruta al archivo de credenciales
CREDENTIALS_FILE = "/home/aadm/GIT/aaduhalde/aaduhalde_api/credentials.conf"

# Configuración MSSQL
DB_SERVER = "localhost"
DB_NAME = "pachacamac"
TABLE_NAME = "fakestore_kpis"

CSV_FILE = "data/analytics/fakestore_kpi_dataset.csv"

def load_credentials():
    config = configparser.ConfigParser()
    config.read(CREDENTIALS_FILE)

    db_user = config["database"]["DB_USER"]
    db_password = config["database"]["DB_PASSWORD"]

    return db_user, db_password

def get_engine():
    DB_USER, DB_PASSWORD = load_credentials()

    connection_string = (
        f"mssql+pyodbc://{DB_USER}:{DB_PASSWORD}@{DB_SERVER}/{DB_NAME}"
        "?driver=ODBC+Driver+18+for+SQL+Server"
        "&TrustServerCertificate=yes"
    )
    return create_engine(connection_string)

def load_csv_to_mssql():
    # 1. Leer CSV
    df = pd.read_csv(CSV_FILE)

    # 2. Conectarse a SQL Server
    engine = get_engine()

    # 3. Crear / reemplazar tabla e insertar datos
    df.to_sql(
        TABLE_NAME,
        engine,
        if_exists="replace",
        index=False
    )

    print(f"CSV loaded into SQL Server: {DB_NAME}.dbo.{TABLE_NAME}")

if __name__ == "__main__":
    load_csv_to_mssql()
