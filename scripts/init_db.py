"""
Database initializer — run during Render build step.

1. Applies schema (CREATE TABLE IF NOT EXISTS — idempotent)
2. Seeds prod data if the parcels table is empty
"""
import os
import sys
from pathlib import Path

import psycopg2

DATABASE_URL = os.environ["DATABASE_URL"]

ROOT = Path(__file__).parent.parent
INIT_DIR = ROOT / "db" / "init"
SEED_FILE = ROOT / "db" / "seed" / "prod_data.sql"


def run_sql_file(cur, path: Path) -> None:
    sql = path.read_text(encoding="utf-8")
    cur.execute(sql)
    print(f"  ✓ {path.name}")


def main():
    print("Connecting to database…")
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    cur = conn.cursor()

    print("Applying schema…")
    for fname in ["01_extensions.sql", "02_schema.sql", "03_indexes.sql"]:
        run_sql_file(cur, INIT_DIR / fname)

    print("Checking if data already present…")
    cur.execute("SELECT COUNT(*) FROM parcels")
    count = cur.fetchone()[0]

    if count == 0:
        print("Seeding production data…")
        run_sql_file(cur, SEED_FILE)
        cur.execute("SELECT COUNT(*) FROM parcels")
        seeded = cur.fetchone()[0]
        print(f"  Seeded {seeded} parcels.")
    else:
        print(f"  Data already present ({count} parcels) — skipping seed.")

    cur.close()
    conn.close()
    print("Database ready.")


if __name__ == "__main__":
    main()
