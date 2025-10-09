from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import os, sys, psycopg2, subprocess, threading, datetime
from dotenv import load_dotenv
from typing import Any, List, Optional
from pydantic import BaseModel, Field, root_validator

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

_default_cors_origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost:5174",
    "http://127.0.0.1:5174",
    "http://localhost:5175",
    "http://127.0.0.1:5175",
]
_raw_origins = os.getenv("API_CORS_ORIGINS")
_cors_origins = [origin.strip() for origin in _raw_origins.split(",") if origin.strip()] if _raw_origins else _default_cors_origins

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
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
    UNION ALL SELECT 'brello_stations', COUNT(*) FROM brello_stations
    UNION ALL SELECT 'geo_zones', COUNT(*) FROM geo_zones
    """
    return q(sql)

# =====================
# ETL run orchestration
# =====================

ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
ETL_DIR = os.path.join(ROOT_DIR, "etl")
DEFAULT_QUERIES_FILE = os.getenv(
    "GOOGLE_PLACES_QUERIES_FILE",
    os.path.join(ETL_DIR, "queries", "google_places_queries.txt"),
)

RUNS = {
    "google_import": {
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
    "auto_refresh": {
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

def _load_queries_from_file(path: str) -> list[str]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            queries = []
            for line in fh:
                text = line.strip()
                if text and not text.startswith("#"):
                    queries.append(text)
            return queries
    except FileNotFoundError:
        return []


class GooglePlacesRequest(BaseModel):
    location: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    radius: Optional[int] = Field(None, gt=0)
    limit: Optional[int] = Field(None, gt=0)
    sleep_seconds: Optional[float] = Field(None, ge=0.0)
    queries: List[str] = Field(default_factory=list)
    queries_file: Optional[str] = None

    class Config:
        anystr_strip_whitespace = True

    @root_validator(pre=True)
    def _normalize_inputs(cls, values):
        queries = values.get("queries") or []
        if isinstance(queries, str):
            queries = [chunk.strip() for chunk in queries.split(",")]
        cleaned = []
        for item in queries:
            if item is None:
                continue
            text = str(item).strip()
            if text:
                cleaned.append(text)
        selected_file = None
        raw_file = values.get("queries_file")
        if raw_file:
            selected_file = str(raw_file).strip() or None
            if selected_file:
                cleaned = _load_queries_from_file(selected_file)
        if not cleaned:
            if selected_file is None and DEFAULT_QUERIES_FILE:
                cleaned = _load_queries_from_file(DEFAULT_QUERIES_FILE)
                if cleaned:
                    selected_file = DEFAULT_QUERIES_FILE
        lat = values.get("lat")
        lng = values.get("lng")
        location = values.get("location")
        if lat is not None and lng is None or lng is not None and lat is None:
            raise ValueError("Specify both lat and lng or neither")
        if (location is None or str(location).strip() == "") and lat is None:
            raise ValueError("Provide a location or both lat/lng")
        if not cleaned:
            raise ValueError("Provide at least one query term")
        values["queries"] = cleaned
        values["queries_file"] = selected_file
        return values

    def to_args(self) -> list[str]:
        args: list[str] = [sys.executable, "-u", os.path.join("etl", "google_places.py")]
        if self.location:
            args.extend(["--location", self.location])
        if self.lat is not None and self.lng is not None:
            args.extend(["--lat", f"{self.lat}", "--lng", f"{self.lng}"])
        if self.radius is not None:
            args.extend(["--radius", str(self.radius)])
        if self.limit is not None:
            args.extend(["--limit", str(self.limit)])
        if self.sleep_seconds is not None:
            args.extend(["--sleep-seconds", f"{self.sleep_seconds}"])
        if self.queries_file:
            args.extend(["--queries-file", self.queries_file])
        else:
            args.append("--queries")
            args.extend(self.queries)
        return args


@app.post("/etl/google_places/start")
def etl_google_places_start(payload: GooglePlacesRequest):
    args = payload.to_args()
    if not _start_job("google_import", args):
        raise HTTPException(status_code=409, detail="google_import already running")
    return {"status": "started"}


@app.post("/etl/pipeline/start")
def etl_pipeline_start():
    args = [sys.executable, "-u", os.path.join("etl", "run_all.py")]
    if not _start_job("pipeline", args):
        raise HTTPException(status_code=409, detail="pipeline already running")
    return {"status": "started"}


@app.post("/automation/auto_refresh/start")
def automation_auto_refresh_start():
    args = [sys.executable, "-u", os.path.join("automation", "auto_refresh.py")]
    if not _start_job("auto_refresh", args):
        raise HTTPException(status_code=409, detail="auto_refresh already running")
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
        bm.updated_at AS metrics_updated_at,
        bf.website_url,
        bf.social,
        bf.confidence AS facts_confidence_override,
        bf.marketing_attitude AS facts_marketing_attitude,
        bf.umbrella_affinity AS facts_umbrella_affinity,
        bf.budget_source,
        bf.provenance,
        bf.updated_at AS facts_updated_at,
        bf.source_provider,
        bf.source_model,
        er.raw_response AS llm_raw_response
    FROM places_clean p
    JOIN business_metrics bm ON bm.business_id = p.place_id
    LEFT JOIN business_facts bf ON bf.business_id = p.place_id
    LEFT JOIN LATERAL (
        SELECT resp.raw_response
        FROM enrichment_request req
        JOIN enrichment_response resp ON resp.request_id = req.request_id
        WHERE req.business_id = p.place_id
        ORDER BY resp.created_at DESC
        LIMIT 1
    ) er ON TRUE
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
