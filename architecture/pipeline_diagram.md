```text
[ External API ]
       |
       v
[ Ingestion Layer ]
(api_client.py)
       |
       |-- guarda --> data/raw/fakestore_raw.json   (RAW layer)
       |
       v
[ Processing Layer ]
(transform.py)
       |
       |-- genera --> data/analytics/fakestore_kpi_dataset.csv (ANALYTICS layer)
       |
       v
[ Storage Layer ]
(load_to_db.py)
       |
       v
[ SQL Server (MSSQL) ]
       |
       v
[ Looker / BI ]
```