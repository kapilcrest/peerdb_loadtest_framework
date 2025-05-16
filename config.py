CONFIG = {
    "pg_conn_str": "dbname=master_data_fake user=postgres password=password host=postgresql-hl.postgresql.svc.cluster.local port=5432",
    "bq_project": "your-bq-project",
    "bq_dataset": "your_bq_dataset",
    "api_url": "http://flow-api.peerdb.svc.cluster.local/api/v1/mirror",
    "schemas": [f"customer_schema_{i}" for i in range(1, 51)],
    "num_workers": 6,
    "mutation_loops": 60,
    "tables_per_schema": 10,
    "mutation_interval_sec": 5,
    "log_csv": "metrics_log.csv"
}