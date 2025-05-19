import multiprocessing
import time
import psycopg2
from config import CONFIG
from mirror_creator import create_mirror
from data_mutator import mutate_schema
from lag_checker import check_lag
from logger import CSVLogger

logger = CSVLogger(CONFIG["log_csv"])

def worker(schema_name):
    try:
        # Skip mirror creation since they're already created
        print(f"Connecting to database for schema: {schema_name}")
        conn = psycopg2.connect(CONFIG["pg_conn_str"])
        print(f"Connected to database for schema: {schema_name}")

        for i in range(CONFIG["mutation_loops"]):
            print(f"🔁 Mutation round {i + 1} for {schema_name}")
            
            # Run the mutation and collect stats
            mutation_stats = mutate_schema(conn, schema_name, CONFIG["tables_per_schema"])
            total_changes = 0
            tables_mutated = 0

            for stat in mutation_stats:
                table = stat["table"]
                inserted = stat["inserted"]
                updated = stat["updated"]
                deleted = stat["deleted"]
                total = inserted + updated + deleted

                if total > 0:
                    tables_mutated += 1
                    logger.log(schema_name, f"{table}_inserted", inserted)
                    logger.log(schema_name, f"{table}_updated", updated)
                    logger.log(schema_name, f"{table}_deleted", deleted)

                total_changes += total

            # Log total changes and tables mutated in this round
            logger.log(schema_name, "tables_mutated", tables_mutated)
            logger.log(schema_name, "total_changes", total_changes)
            logger.log(schema_name, "mutation_round", i + 1)

            # Sleep before measuring lag
            time.sleep(CONFIG["mutation_interval_sec"])

            # Check max lag across all tables
            max_lag = 0
            for t in range(1, CONFIG["tables_per_schema"] + 1):
                table = f"table_{t}"
                lag = check_lag(conn, schema_name, table)
                if lag is not None:
                    max_lag = max(max_lag, lag)

            logger.log(schema_name, "max_cdc_lag_sec", max_lag)

        conn.close()
    except Exception as e:
        logger.log(schema_name, "worker_exception", str(e))

def run_all():
    with multiprocessing.Pool(CONFIG["num_workers"]) as pool:
        pool.map(worker, CONFIG["schemas"])

if __name__ == "__main__":
    run_all()