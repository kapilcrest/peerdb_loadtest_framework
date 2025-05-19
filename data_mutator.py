import psycopg2
import random
from faker import Faker

def mutate_schema(conn, schema, table_count):
    faker = Faker()
    cur = conn.cursor()
    
    mutation_stats = []

    for t in range(1, table_count + 1):
        table = f"table_{t}"
        table_name = f"{schema}.{table}"

        # Generate 10 inserts per table
        insert_count = 10
        for _ in range(insert_count):
            cur.execute(
                f"INSERT INTO {table_name} (name, email) VALUES (%s, %s)",
                (faker.name(), faker.email())
            )

        # Randomly update ~1% of rows
        cur.execute(f"WITH cte AS (SELECT * FROM {table_name} WHERE random() < 0.01) "
                    f"UPDATE {table_name} SET updated_at = now() WHERE row_id IN (SELECT row_id FROM cte)")
        update_count = cur.rowcount

        # Randomly delete ~0.5% of rows
        cur.execute(f"WITH cte AS (SELECT * FROM {table_name} WHERE random() < 0.005) "
                    f"DELETE FROM {table_name} WHERE row_id IN (SELECT row_id FROM cte)")
        delete_count = cur.rowcount

        mutation_stats.append({
            "table": table,
            "inserted": insert_count,
            "updated": update_count,
            "deleted": delete_count
        })

    conn.commit()
    cur.close()
    return mutation_stats