import csv
from datetime import datetime

class CSVLogger:
    def __init__(self, filename):
        self.filename = filename
        self.fields = ["timestamp", "schema", "metric", "value"]
        with open(self.filename, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.fields)
            writer.writeheader()

    def log(self, schema, metric, value):
        with open(self.filename, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.fields)
            writer.writerow({
                "timestamp": datetime.utcnow().isoformat(),
                "schema": schema,
                "metric": metric,
                "value": value
            })