import os, sys, psycopg2
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

# Forza UTF-8 su stdout/stderr per evitare errori di encoding su Windows
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

PG = dict(
    host=os.getenv("POSTGRES_HOST","localhost"),
    port=os.getenv("POSTGRES_PORT","5432"),
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD")
)

def exec_sql_file(path):
    with open(path, "r", encoding="utf-8") as f, psycopg2.connect(**PG) as conn:
        with conn.cursor() as cur:
            sql = f.read()
            print(f"\n[SQL] Executing {os.path.basename(path)}")
            cur.execute(sql)
        conn.commit()

if __name__ == "__main__":
    base = os.path.join(os.path.dirname(__file__), "sql_blocks")
    steps = [
        # "00_reset_pipeline.sql",
        "build_places_raw.sql",
        "00b_build_osm_poi.sql",
        "normalize_osm.sql",
        "context_density.sql",
        "context_poi.sql",
        "scoring_popularity.sql",
        "scoring_context.sql",
        "scoring_access.sql",
        "scoring_total.sql"
    ]
    for step in steps:
        exec_sql_file(os.path.join(base, step))
    print("\n[SQL] Pipeline completata con successo!")
