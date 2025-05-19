import psycopg2
import random
from faker import Faker

def mutate_schema(conn, schema, table_count):
    faker = Faker()
    cur = conn.cursor()

    total_inserts = 0
    total_updates = 0
    total_deletes = 0
    tables_mutated = 0
    table_stats = {}

    for t in range(1, table_count + 1):
        table = f"table_{t}"
        inserted = updated = deleted = 0

        try:
            # INSERT
            cur.execute(f"INSERT INTO {schema}.{table} (name, email) VALUES (%s, %s)", (faker.name(), faker.email()))
            inserted = 1

            # UPDATE
            cur.execute(f"UPDATE {schema}.{table} SET updated_at = now() WHERE random() < 0.01 RETURNING *")
            updated = cur.rowcount

            # DELETE
            cur.execute(f"DELETE FROM {schema}.{table} WHERE random() < 0.005 RETURNING *")
            deleted = cur.rowcount

            if inserted + updated + deleted > 0:
                tables_mutated += 1

            total_inserts += inserted
            total_updates += updated
            total_deletes += deleted

            table_stats[table] = {
                "inserted": inserted,
                "updated": updated,
                "deleted": deleted,
            }

        except Exception as e:
            print(f"⚠️ Mutation error on {schema}.{table}: {e}", flush=True)

    conn.commit()
    cur.close()

    return {
        "tables_mutated": tables_mutated,
        "total_inserts": total_inserts,
        "total_updates": total_updates,
        "total_deletes": total_deletes,
        "total_changes": total_inserts + total_updates + total_deletes,
        "table_stats": table_stats
    }