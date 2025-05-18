import psycopg2
import time
from config import CONFIG

def create_mirror(schema_name):
    mirror_name = f"mirror_{schema_name}"
    conn = psycopg2.connect(CONFIG["pg_conn_str"])
    cur = conn.cursor()

    start = time.time()
    try:
        tables = [f"'{schema_name}.table_{i}'" for i in range(1, CONFIG["tables_per_schema"] + 1)]
        tables_str = ", ".join(tables)
        sql = f"""
        CREATE MIRROR {mirror_name}
        FROM postgresloadtesting
        TO bigquery
        TABLES {tables_str}
        INTO "{CONFIG['bq_dataset']}"
        OPTIONS (
            initial_copy = 'true',
            cdc = 'true'
        );
        """
        cur.execute(sql)
        conn.commit()
        duration = round(time.time() - start, 2)
        return {
            "schema": schema_name,
            "status": 200,
            "duration_sec": duration,
            "error": None
        }

    except Exception as e:
        conn.rollback()
        return {
            "schema": schema_name,
            "status": 500,
            "duration_sec": 0,
            "error": str(e)
        }
    finally:
        cur.close()
        conn.close()