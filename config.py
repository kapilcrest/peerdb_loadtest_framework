import os

CONFIG = {
    # This is the connection string used for mutation and lag-checking, not for PeerDB itself.
    "pg_conn_str": f"dbname=master_data_fake user={os.getenv('PG_USER', 'peerdb_user')} "
                   f"password={os.getenv('PG_PASSWORD', '')} "
                   f"host=postgresql-hl.postgresql.svc.cluster.local port=5432",

    # Google BigQuery project and dataset for destination
    "bq_project": "crest-test004-project",
    "bq_dataset": "master_data_fake",

    # API endpoint for PeerDB mirror creation (valid for v0.23+)
    "api_url": "http://peerdb-ui.peerdb.svc.cluster.local:3000/api/v1/flows/cdc/create",

    # This must match your PeerDB deployment UID set during Helm installation
    "deployment_uid": "peerdb-test004-private-do",

    # Schemas to iterate over for mirror creation and load testing
    "schemas": [f"customer_schema_{i}" for i in range(1, 2)],

    # Worker/threading config for multiprocessing
    "num_workers": 10,

    # Number of mutation + lag check rounds per schema
    "mutation_loops": 60,

    # Number of tables per schema to test against
    "tables_per_schema": 10,

    # Time to wait between each mutation round
    "mutation_interval_sec": 5,

    # CSV log file to store timing and metrics
    "log_csv": "metrics_log.csv",

    # Source and destination peer names as defined in PeerDB UI or peer creation API
    "source_peer_name": "postgresloadtesting",
    "destination_peer_name": "bigqueryloadtesting",

    # Optional: publication name pattern if you follow "pub_{schema}" convention
    "publication_name_template": "pub_{schema}",
    "peerdbui_password": "OnBlZXJkYg=="
}