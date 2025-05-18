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
        mirror = create_mirror(schema_name)
        logger.log(schema_name, "mirror_creation_time", mirror["duration_sec"])
        if mirror["error"]:
            logger.log(schema_name, "mirror_creation_error", mirror["error"])
            return

        conn = psycopg2.connect(CONFIG["pg_conn_str"])
        for i in range(CONFIG["mutation_loops"]):
            mutate_schema(conn, schema_name, CONFIG["tables_per_schema"])
            logger.log(schema_name, "mutation_round", i + 1)
            time.sleep(CONFIG["mutation_interval_sec"])
            lag = check_lag(conn, schema_name, "table_1")
            if lag:
                logger.log(schema_name, "cdc_lag_sec", lag)
        conn.close()
    except Exception as e:
        logger.log(schema_name, "worker_exception", str(e))

def run_all():
    with multiprocessing.Pool(CONFIG["num_workers"]) as pool:
        pool.map(worker, CONFIG["schemas"])

if __name__ == "__main__":
    run_all()