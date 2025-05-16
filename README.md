"""
# PeerDB Load Testing Framework

## 📦 Features
- Create mirrors via API
- Mutate Postgres data live
- Check replication lag (CDC)
- Log all metrics to CSV
- Generate visual report

## 🚀 How to Run

1. Customize config in `config.py`
2. Run the framework:
```bash
python3 -m peerdb_loadtest_framework.runner
```
3. Generate a report:
```bash
python3 -m peerdb_loadtest_framework.report_generator
```

## 🔍 Monitored Metrics
- Mirror creation time
- Mutation round
- CDC lag (PG vs BQ)
- Mirror creation errors

"""