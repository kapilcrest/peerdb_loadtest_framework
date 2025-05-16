import psycopg2
import random
from faker import Faker

def mutate_schema(conn, schema, table_count):
    faker = Faker()
    cur = conn.cursor()
    for t in range(1, table_count + 1):
        table = f"table_{t}"
        cur.execute(f"INSERT INTO {schema}.{table} (name, email) VALUES (%s, %s)", (faker.name(), faker.email()))
        cur.execute(f"UPDATE {schema}.{table} SET updated_at = now() WHERE random() < 0.01")
        cur.execute(f"DELETE FROM {schema}.{table} WHERE random() < 0.005")
    conn.commit()
    cur.close()