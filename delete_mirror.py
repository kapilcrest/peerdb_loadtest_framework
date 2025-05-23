import requests
from config import CONFIG

def delete_mirror(mirror_name):
    url = "http://peerdb-ui.peerdb.svc.cluster.local:3000/api/v1/mirrors/state_change"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {CONFIG['peerdbui_password']}"
    }
    payload = {
        "flowJobName": mirror_name,
        "requestedFlowState": "STATUS_TERMINATED"
    }
    resp = requests.post(url, json=payload, headers=headers)
    print(f"🗑️ Deleting mirror {mirror_name} - Status: {resp.status_code}, Response: {resp.text}")

# Example: loop through your mirror names
for i in range(1, 501):
    delete_mirror(f"mirror_customer_schema_{i}")