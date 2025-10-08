from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import os, sys, psycopg2, subprocess, threading, datetime
from dotenv import load_dotenv

# Carica il .env dalla root del progetto
load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))

PG = dict(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=os.getenv("POSTGRES_PORT", "5432"),
    dbname=os.getenv("POSTGRES_DB", "ctdb"),
    user=os.getenv("POSTGRES_USER", "ctuser"),
    password=os.getenv("POSTGRES_PASSWORD", "ctpass"),
)

def q(sql, params=()):
    with psycopg2.connect(**PG) as c:
        with c.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]

app = FastAPI()

origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
]
app.add_middleware(
    CORSMiddleware,
    # In DEV: accetta localhost e 127.0.0.1 su QUALSIASI porta
    allow_origins=[],  # lasciato vuoto perché usiamo la regex
    allow_origin_regex=r"^http://(localhost|127\.0\.0\.1):\d+$",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return q("SELECT now() AS db_time;")[0]

@app.get("/counts")
def counts():
    sql = """
    SELECT 'places_raw' AS tbl, COUNT(*) FROM places_raw
    UNION ALL SELECT 'places_clean', COUNT(*) FROM places_clean
    UNION ALL SELECT 'place_context', COUNT(*) FROM place_context
    UNION ALL SELECT 'company_scores', COUNT(*) FROM company_scores
    UNION ALL SELECT 'osm_business', COUNT(*) FROM osm_business
    UNION ALL SELECT 'osm_roads', COUNT(*) FROM osm_roads
    """
    return q(sql)

# =====================
# ETL run orchestration
# =====================

ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
ETL_DIR = os.path.join(ROOT_DIR, "etl")

RUNS = {
    "overpass": {
        "status": "idle",  # idle | running | ok | error
        "started_at": None,
        "ended_at": None,
        "last_rc": None,
        "last_lines": [],
    },
    "pipeline": {
        "status": "idle",
        "started_at": None,
        "ended_at": None,
        "last_rc": None,
        "last_lines": [],
    },
}

def _start_job(name: str, args: list[str]) -> bool:
    job = RUNS[name]
    if job["status"] == "running":
        return False
    job.update({
        "status": "running",
        "started_at": datetime.datetime.now().isoformat(timespec="seconds"),
        "ended_at": None,
        "last_rc": None,
        "last_lines": [],
    })

    def worker():
        try:
            env = dict(os.environ)
            env.setdefault("PYTHONIOENCODING", "utf-8")
            env.setdefault("PYTHONUTF8", "1")
            proc = subprocess.Popen(
                args,
                cwd=ROOT_DIR,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                env=env,
            )
            assert proc.stdout is not None
            for line in proc.stdout:
                # mantieni solo le ultime 200 righe
                job["last_lines"].append(line.rstrip("\n"))
                if len(job["last_lines"]) > 200:
                    job["last_lines"] = job["last_lines"][-200:]
            rc = proc.wait()
            job["last_rc"] = rc
            job["status"] = "ok" if rc == 0 else "error"
        except Exception as ex:
            job["status"] = "error"
            job["last_lines"].append(f"[EXC] {ex}")
        finally:
            job["ended_at"] = datetime.datetime.now().isoformat(timespec="seconds")

    threading.Thread(target=worker, daemon=True).start()
    return True


@app.post("/etl/overpass/start")
def etl_overpass_start():
    args = [sys.executable, "-u", os.path.join("etl", "osm_overpass.py")]
    if not _start_job("overpass", args):
        raise HTTPException(status_code=409, detail="overpass already running")
    return {"status": "started"}


@app.post("/etl/pipeline/start")
def etl_pipeline_start():
    args = [sys.executable, "-u", os.path.join("etl", "run_all.py")]
    if not _start_job("pipeline", args):
        raise HTTPException(status_code=409, detail="pipeline already running")
    return {"status": "started"}


@app.get("/etl/status")
def etl_status():
    return RUNS

@app.get("/places")
def places(city: str | None = None, category: str | None = None, min_score: float = 0, limit: int = 50):
    sql = """
    SELECT p.place_id, p.name, p.city, p.category,
           s.total_score, s.popularity_score, s.territory_score, s.accessibility_score
    FROM places_clean p
    JOIN company_scores s USING(place_id)
    WHERE s.total_score >= %s
    """ + (" AND p.city=%s" if city else "") + (" AND p.category=%s" if category else "") + """
    ORDER BY s.total_score DESC
    LIMIT %s
    """
    args = [min_score] + ([city] if city else []) + ([category] if category else []) + [limit]
    return q(sql, tuple(args))
