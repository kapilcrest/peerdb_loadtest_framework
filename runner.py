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
        # mirror = create_mirror(schema_name)
        # logger.log(schema_name, "mirror_creation_time", mirror["duration_sec"])
        # if mirror["error"]:
        #     logger.log(schema_name, "mirror_creation_error", mirror["error"])
        #     return
        # Mirrors already created
        conn = psycopg2.connect(CONFIG["pg_conn_str"])
        print(f"✅ Connected to DB for {schema_name}")

        for i in range(CONFIG["mutation_loops"]):
            start = time.time()
            mutation_stats = mutate_schema(conn, schema_name, CONFIG["tables_per_schema"])
            duration = round(time.time() - start, 2)

            logger.log(schema_name, "mutation_round", i + 1)
            logger.log(schema_name, "mutation_duration_sec", duration)
            logger.log(schema_name, "tables_mutated", mutation_stats["tables_mutated"])
            logger.log(schema_name, "total_changes", mutation_stats["total_changes"])
            logger.log(schema_name, "total_inserts", mutation_stats["total_inserts"])
            logger.log(schema_name, "total_updates", mutation_stats["total_updates"])
            logger.log(schema_name, "total_deletes", mutation_stats["total_deletes"])

            # Per-table stats (optional, can comment out if too verbose)
            for table, stats in mutation_stats["table_stats"].items():
                for metric, count in stats.items():
                    logger.log(schema_name, f"{metric}_{table}", count)

            # CDC lag per table
            max_lag = 0
            for t in range(1, CONFIG["tables_per_schema"] + 1):
                table = f"table_{t}"
                lag = check_lag(conn, schema_name, table)
                if lag is not None:
                    logger.log(schema_name, f"lag_{table}", lag)
                    max_lag = max(max_lag, lag)

            logger.log(schema_name, "max_cdc_lag_sec", max_lag)
            time.sleep(CONFIG["mutation_interval_sec"])

        conn.close()
    except Exception as e:
        logger.log(schema_name, "worker_exception", str(e))

def run_all():
    with multiprocessing.Pool(CONFIG["num_workers"]) as pool:
        pool.map(worker, CONFIG["schemas"])

if __name__ == "__main__":
    run_all()