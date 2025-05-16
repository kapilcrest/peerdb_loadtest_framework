import os

CONFIG = {
    "pg_conn_str": f"dbname=master_data_fake user={os.getenv('PG_USER', 'postgres')} "
                   f"password={os.getenv('PG_PASSWORD', '')} "
                   f"host=postgresql-hl.postgresql.svc.cluster.local port=5432",

    "bq_project": "crest-test004-project",
    "bq_dataset": "mv_master_fake",
    "api_url": "http://flow-api.peerdb.svc.cluster.local/api/v1/mirror",
    "schemas": [f"customer_schema_{i}" for i in range(1, 51)],
    "num_workers": 6,
    "mutation_loops": 60,
    "tables_per_schema": 10,
    "mutation_interval_sec": 5,
    "log_csv": "metrics_log.csv"
}