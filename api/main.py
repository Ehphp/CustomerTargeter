from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import os, sys, psycopg2, subprocess, threading, datetime
from dotenv import load_dotenv
from typing import Any

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
    UNION ALL SELECT 'place_sector_density', COUNT(*) FROM place_sector_density
    UNION ALL SELECT 'business_facts', COUNT(*) FROM business_facts
    UNION ALL SELECT 'business_metrics', COUNT(*) FROM business_metrics
    UNION ALL SELECT 'osm_business', COUNT(*) FROM osm_business
    UNION ALL SELECT 'osm_roads', COUNT(*) FROM osm_roads
    UNION ALL SELECT 'brello_stations', COUNT(*) FROM brello_stations
    UNION ALL SELECT 'geo_zones', COUNT(*) FROM geo_zones
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
def places(
    city: str | None = None,
    category: str | None = None,
    geo_label: str | None = None,
    size_class: str | None = None,
    ad_budget: str | None = None,
    is_chain: bool | None = None,
    min_affinity: float = 0.0,
    min_density: float = 0.0,
    min_digital: float = 0.0,
    limit: int = 50,
):
    sql = """
    SELECT
        p.place_id,
        p.name,
        p.city,
        p.category,
        bm.sector_density_score,
        bm.sector_density_neighbors,
        bm.geo_distribution_label,
        bm.geo_distribution_source,
        bm.size_class,
        bm.is_chain,
        bm.ad_budget_band,
        bm.umbrella_affinity,
        bm.digital_presence,
        bm.digital_presence_confidence,
        bm.marketing_attitude,
        bm.facts_confidence,
        bf.website_url,
        bf.social,
        bf.confidence AS facts_confidence_override,
        bf.source_provider,
        bf.source_model
    FROM places_clean p
    JOIN business_metrics bm ON bm.business_id = p.place_id
    LEFT JOIN business_facts bf ON bf.business_id = p.place_id
    WHERE 1=1
    """
    params: list[Any] = []
    if city:
        sql += " AND p.city = %s"
        params.append(city)
    if category:
        sql += " AND p.category = %s"
        params.append(category)
    if geo_label:
        sql += " AND bm.geo_distribution_label = %s"
        params.append(geo_label)
    if size_class:
        sql += " AND bm.size_class = %s"
        params.append(size_class)
    if ad_budget:
        sql += " AND bm.ad_budget_band = %s"
        params.append(ad_budget)
    if is_chain is not None:
        sql += " AND bm.is_chain = %s"
        params.append(is_chain)
    if min_affinity:
        sql += " AND COALESCE(bm.umbrella_affinity, 0) >= %s"
        params.append(min_affinity)
    if min_density:
        sql += " AND COALESCE(bm.sector_density_score, 0) >= %s"
        params.append(min_density)
    if min_digital:
        sql += " AND COALESCE(bm.digital_presence, 0) >= %s"
        params.append(min_digital)

    sql += """
    ORDER BY
        bm.umbrella_affinity DESC NULLS LAST,
        bm.digital_presence DESC NULLS LAST,
        bm.sector_density_score DESC NULLS LAST
    LIMIT %s
    """
    params.append(limit)
    return q(sql, tuple(params))
