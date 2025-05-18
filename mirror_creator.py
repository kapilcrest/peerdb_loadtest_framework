import requests
import time
from config import CONFIG

def create_mirror(schema_name):
    payload = {
        "source_peer": "pg_src",
        "destination_peer": "bq_dest",
        "source_schema": schema_name,
        "destination_dataset": CONFIG["bq_dataset"],
        "tables": [],
        "mirror_name": f"mirror_{schema_name}",
        "initial_copy": True,
        "cdc": True
    }
    start = time.time()
    r = requests.post(CONFIG["api_url"], json=payload)
    end = time.time()
    return {
        "schema": schema_name,
        "status": r.status_code,
        "duration_sec": round(end - start, 2),
        "error": r.text if r.status_code != 200 else None
    }