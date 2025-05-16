import psycopg2
from google.cloud import bigquery
from datetime import datetime
from peerdb_loadtest_framework.config import CONFIG

def check_lag(pg_conn, schema, table):
    cur = pg_conn.cursor()
    cur.execute(f"SELECT max(updated_at) FROM {schema}.{table}")
    pg_time = cur.fetchone()[0]
    cur.close()

    client = bigquery.Client(project=CONFIG["bq_project"])
    query = f"SELECT MAX(updated_at) as max_time FROM `{CONFIG['bq_project']}.{CONFIG['bq_dataset']}.{schema}_{table}`"
    result = client.query(query).result()
    for row in result:
        bq_time = row.max_time

    if pg_time and bq_time:
        return (pg_time - bq_time).total_seconds()
    return None
