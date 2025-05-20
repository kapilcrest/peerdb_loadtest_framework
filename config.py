import os

CONFIG = {
    "pg_conn_str": f"dbname=master_data_fake user={os.getenv('PG_USER', 'peerdb_user')} "
                   f"password={os.getenv('PG_PASSWORD', '')} "
                   f"host=postgresql-hl.postgresql.svc.cluster.local port=5432",

    "bq_project": "crest-test004-project",
    "bq_dataset": "master_data_fake",

    "api_url": "http://peerdb-ui.peerdb.svc.cluster.local:3000/api/v1/flows/cdc/create",
    "deployment_uid": "peerdb-test004-private-do",

    # 500 schemas
    "schemas": [f"customer_schema_{i}" for i in range(1, 501)],

    # Increased workers
    "num_workers": 30,

    # Mutation settings
    "mutation_loops": 120,
    "tables_per_schema": 50,
    "mutation_interval_sec": 3,
    "log_csv": "metrics_log.csv",

    "source_peer_name": "postgresloadtesting",
    "destination_peer_name": "bigqueryloadtesting",
    "publication_name_template": "pub_{schema}",
    "peerdbui_password": "OnBlZXJkYg=="  # base64 encoded
}