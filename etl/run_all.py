import os, sys, logging, psycopg2
from dotenv import load_dotenv
from time import perf_counter

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

# Forza UTF-8 su stdout/stderr per evitare errori di encoding su Windows
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

logging.basicConfig(
    level=os.getenv("ETL_LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("etl.pipeline")

PG = dict(
    host=os.getenv("POSTGRES_HOST","localhost"),
    port=os.getenv("POSTGRES_PORT","5432"),
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD")
)

def exec_sql_file(path):
    step_name = os.path.basename(path)
    started_at = perf_counter()
    logger.info("Starting step %s", step_name)
    with open(path, "r", encoding="utf-8") as f, psycopg2.connect(**PG) as conn:
        try:
            sql = f.read()
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()
        except Exception:
            conn.rollback()
            logger.exception("Step %s failed", step_name)
            raise
    elapsed = perf_counter() - started_at
    logger.info("Completed step %s in %.2fs", step_name, elapsed)

if __name__ == "__main__":
    base = os.path.join(os.path.dirname(__file__), "sql_blocks")
    steps = [
        # "00_reset_pipeline.sql",
        "00_setup_brello.sql",
        "build_places_raw.sql",
        "00b_build_osm_poi.sql",
        "normalize_osm.sql",
        "context_sector_density.sql",
    ]
    logger.info("Launching ETL pipeline with %d steps", len(steps))
    for step in steps:
        exec_sql_file(os.path.join(base, step))
    logger.info("ETL pipeline completata con successo!")
