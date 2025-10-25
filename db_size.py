import sqlite3
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_DIR = os.path.join(BASE_DIR, "db")
os.makedirs(DB_DIR, exist_ok=True)
DB_PATH = os.path.join(DB_DIR, "fm_database.sqlite")


con = sqlite3.connect(DB_PATH)
cur = con.cursor()

def qident(name: str) -> str:
    """SQLite identifier quoting with double quotes."""
    return '"' + name.replace('"', '""') + '"'

# 1) list user tables
tables = [r[0] for r in cur.execute(
    "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
)]

def table_payload_bytes(tbl: str) -> int:
    # columns for this table
    cols = [r[1] for r in cur.execute(f"PRAGMA table_info({qident(tbl)})")]
    if not cols:
        return 0
    # length(col) works for TEXT/BLOB; fall back to length(CAST(col AS TEXT)) for numbers
    parts = [f"COALESCE(length({qident(c)}), length(CAST({qident(c)} AS TEXT)), 0)" for c in cols]
    expr = " + ".join(parts)
    sql = f"SELECT COALESCE(SUM({expr}), 0) FROM {qident(tbl)}"
    return cur.execute(sql).fetchone()[0] or 0

sizes = [(t, table_payload_bytes(t)) for t in tables]
sizes.sort(key=lambda x: x[1], reverse=True)

print("Approx table payload sizes (bytes):")
for t, b in sizes:
    print(f"{t:30s} {b:>12,d}")

# 3) whole DB size and used bytes (excludes free list pages)
page_size  = cur.execute("PRAGMA page_size").fetchone()[0]
page_count = cur.execute("PRAGMA page_count").fetchone()[0]
freelist   = cur.execute("PRAGMA freelist_count").fetchone()[0]
db_bytes   = page_size * page_count
used_bytes = page_size * (page_count - freelist)
print(f"\nDatabase file bytes: {db_bytes:,} (used ~{used_bytes:,})")

con.close()
