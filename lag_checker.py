import psycopg2
from google.cloud import bigquery
from datetime import datetime
from config import CONFIG

def check_lag(pg_conn, schema, table):
    try:
        cur = pg_conn.cursor()
        cur.execute(f"SELECT max(updated_at) FROM {schema}.{table}")
        pg_time = cur.fetchone()[0]
        print(pg_time)
        cur.close()

        client = bigquery.Client(project=CONFIG["bq_project"])
        query = f"""
            SELECT MAX(updated_at) as max_time 
            FROM `{CONFIG['bq_project']}.{CONFIG['bq_dataset']}.{schema}_{table}`
        """
        result = client.query(query).result()
        print(result)
        bq_time = None
        for row in result:
            bq_time = row.max_time

        if not pg_time:
            print(f"⚠️ No updated_at in PG: {schema}.{table}")
        if not bq_time:
            print(f"⚠️ No updated_at in BQ: {schema}.{table}")

        if pg_time and bq_time:
            lag = (pg_time - bq_time).total_seconds()
            print(f"🕒 Lag for {schema}.{table} = {lag:.2f}s")
            return lag
        return None
    except Exception as e:
        print(f"❌ Error checking lag for {schema}.{table}: {e}")
        return None