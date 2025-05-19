import requests
import time
from config import CONFIG

def create_mirror(schema_name):
    publication_name = CONFIG["publication_name_template"].format(schema=schema_name)
    mirror_name = f"mirror_{schema_name}"

    payload = {
        "connectionConfigs": {
            "sourceName": CONFIG["source_peer_name"],
            "destinationName": CONFIG["destination_peer_name"],
            "flowJobName": mirror_name,
            "publicationName": publication_name,
            "tableMappings": [
                {
                    "sourceTableIdentifier": f"{schema_name}.table_{i}",
                    "destinationTableIdentifier": f"{schema_name}_table_{i}",
                    "engine": 0,  # use engine=0 for default
                    "exclude": [],
                    "columns": [],
                    "partitionKey": ""
                }
                for i in range(1, CONFIG["tables_per_schema"] + 1)
            ],
            "doInitialSnapshot": True,
            "maxBatchSize": 250000,
            "idleTimeoutSeconds": 60,
            "snapshotNumRowsPerPartition": 250000,
            "snapshotMaxParallelWorkers": 10,
            "snapshotNumTablesInParallel": 10,
            "resync": False,
            "softDeleteColName": "_PEERDB_IS_DELETED",
            "syncedAtColName": "_PEERDB_SYNCED_AT",
            "initialSnapshotOnly": False,
            "script": "",
            "system": 0,
            "disablePeerDBColumns": False,
            "replicationSlotName": "",
            "cdcStagingPath": "",
            "snapshotStagingPath": "",
            "env": {},
            "envString": ""
        }
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {CONFIG['peerdbui_password']}"
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