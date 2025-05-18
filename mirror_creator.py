import requests
import time
from config import CONFIG

def create_mirror(schema_name):
    publication_name = CONFIG["publication_name_template"].format(schema=schema_name)
    mirror_name = f"mirror_{schema_name}"

    table_mappings = [
        {
            "source_table_identifier": f"{schema_name}.table_{i}",
            "destination_table_identifier": f"table_{i}"
        }
        for i in range(1, CONFIG["tables_per_schema"] + 1)
    ]

    payload = {
        "connection_configs": {
            "flow_job_name": mirror_name,
            "source_name": CONFIG["source_peer_name"],
            "destination_name": CONFIG["destination_peer_name"],
            "table_mappings": table_mappings,
            "do_initial_snapshot": True,
            "max_batch_size": 1000,
            "idle_timeout_seconds": 300,
            "publication_name": publication_name,
            "snapshot_num_rows_per_partition": 5000,
            "snapshot_max_parallel_workers": 4,
            "snapshot_num_tables_in_parallel": 2,
            "resync": False,
            "initial_snapshot_only": False,
            "soft_delete_col_name": "_peerdb_is_deleted",
            "synced_at_col_name": "_peerdb_synced_at"
        }
    }

    headers = {
        "X-PeerDB-Deployment-UID": CONFIG["deployment_uid"],
        "Content-Type": "application/json"
    }

    print(f"📦 Sending mirror create request for {schema_name} to {CONFIG['api_url']}", flush=True)
    print(f"Payload: {payload}", flush=True)

    try:
        start = time.time()
        r = requests.post(CONFIG["api_url"], json=payload, headers=headers, timeout=10)
        end = time.time()
        return {
            "schema": schema_name,
            "status": r.status_code,
            "duration_sec": round(end - start, 2),
            "error": r.text if r.status_code != 200 else None
        }
    except Exception as e:
        return {
            "schema": schema_name,
            "status": "timeout",
            "duration_sec": 0,
            "error": str(e)
        }