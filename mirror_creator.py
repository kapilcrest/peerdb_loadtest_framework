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
    headers = {
        "X-PeerDB-Deployment-UID": CONFIG.get("deployment_uid", "peerdb-test004-private-do"),
        "Content-Type": "application/json"
    }
    try:
        r = requests.post(CONFIG["api_url"], json=payload,headers=headers, timeout=5)
    except Exception as e:
        print(f"❌ API request to create mirror for {schema_name} failed: {e}", flush=True)
        return {
            "schema": schema_name,
            "status": "timeout",
            "duration_sec": 0,
            "error": str(e)
        }
    end = time.time()
    return {
        "schema": schema_name,
        "status": r.status_code,
        "duration_sec": round(end - start, 2),
        "error": r.text if r.status_code != 200 else None
    }