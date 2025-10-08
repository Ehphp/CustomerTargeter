import requests, psycopg2, os, json, time, sys
from dotenv import load_dotenv
from psycopg2.extras import execute_values
load_dotenv()

# Forza stdout/stderr UTF-8 (evita UnicodeEncodeError su Windows)
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

PG = dict(host="localhost", port=os.getenv("POSTGRES_PORT","5432"),
          dbname=os.getenv("POSTGRES_DB"), user=os.getenv("POSTGRES_USER"),
          password=os.getenv("POSTGRES_PASSWORD"))

OVERPASS = "https://overpass-api.de/api/interpreter"
SESSION = requests.Session()
UA = {"User-Agent": "CustumerTarget/1.0 (+contact@example.com)"}

MIRRORS = [
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.openstreetmap.fr/api/interpreter",
    "https://overpass-api.de/api/interpreter",  # metti DE per ultimo
]
SESSION = requests.Session()
UA = {"User-Agent": "CustumerTarget/1.0 (+contact@example.com)"}

RETRYABLE = {429, 502, 503, 504}

def overpass_query(q, per_mirror_retries=2, sleep_base=3):
    last_err = None
    for mirror in MIRRORS:
        for attempt in range(per_mirror_retries):
            try:
                resp = SESSION.post(mirror, data={"data": q}, headers=UA, timeout=180)
                if resp.status_code in RETRYABLE:
                    if attempt < per_mirror_retries - 1:
                        print(f"[WARN] Overpass busy ({resp.status_code}) on {mirror}, retrying...")
                        time.sleep(sleep_base * (attempt + 1))
                        continue
                resp.raise_for_status()
                return resp.json().get("elements", [])
            except (requests.exceptions.RequestException, requests.exceptions.HTTPError) as ex:
                last_err = ex
                if attempt < per_mirror_retries - 1:
                    time.sleep(sleep_base * (attempt + 1))
        # prova il mirror successivo
        print(f"[INFO] Switching Overpass mirror: {mirror} -> next")
    # se tutti falliscono, esplodi
    raise last_err if last_err else RuntimeError("All Overpass mirrors failed")



def upsert_business(conn, e):
    if "lat" not in e or "lon" not in e:
        return
    lon, lat = e["lon"], e["lat"]
    tags = e.get("tags", {}) or {}

    category, subtype = None, None
    for k in ("shop","amenity","craft"):
        if k in tags:
            category = k
            subtype = tags.get(k)
            break
    
    # ⛔ se non ho categoria (o è vuota) non inserisco
    if not category:
        # opzionale: loggare per debug
        # print(f"[SKIP] n{e.get('id')} senza category")
        return
            
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO osm_business(osm_id,name,category,subtype,tags,phone,website,opening_hours,location)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s, ST_SetSRID(ST_MakePoint(%s,%s),4326)::geography)
        ON CONFLICT(osm_id) DO UPDATE SET
          name=EXCLUDED.name, category=EXCLUDED.category, subtype=EXCLUDED.subtype,
          tags=EXCLUDED.tags, phone=EXCLUDED.phone, website=EXCLUDED.website,
          opening_hours=EXCLUDED.opening_hours, location=EXCLUDED.location;
        """, (
            f"n{e['id']}",
            tags.get("name"),
            category,
            subtype,
            json.dumps(tags, ensure_ascii=False),
            tags.get("contact:phone") or tags.get("phone"),
            tags.get("contact:website") or tags.get("website"),
            tags.get("opening_hours"),
            lon, lat
        ))

def upsert_road(conn, w):
    tags = w.get("tags", {}) or {}
    highway = tags.get("highway")
    if not highway or "geometry" not in w:
        return
    name = tags.get("name")
    coords = [(pt["lon"], pt["lat"]) for pt in w["geometry"]]
    if len(coords) < 2:
        return
    wkt = "LINESTRING(" + ",".join([f"{x} {y}" for x,y in coords]) + ")"
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO osm_roads(osm_id, highway, name, geom)
        VALUES (%s,%s,%s, ST_SetSRID(ST_GeomFromText(%s),4326)::geography)
        ON CONFLICT(osm_id) DO UPDATE SET
          highway=EXCLUDED.highway, name=EXCLUDED.name, geom=EXCLUDED.geom;
        """, (f"w{w['id']}", highway, name, wkt))


def split_bbox(bbox_str, nx=2, ny=2):
    # bbox_str = "south,west,north,east"
    s, w, n, e = map(float, bbox_str.split(","))
    dx = (e - w) / nx
    dy = (n - s) / ny
    for iy in range(ny):
        for ix in range(nx):
            ts = s + iy * dy
            tw = w + ix * dx
            tn = ts + dy
            te = tw + dx
            yield f"{ts:.6f},{tw:.6f},{tn:.6f},{te:.6f}"

EXCLUDE_AMENITY = "^(watering_place|place_of_worship|tobacco|drinking_water|toilets|fuel|atm|parking|place_of_worship|post_box|townhall)$"

def fetch_businesses(bbox):
    q = f"""
    [out:json][timeout:60];
    (
      node[shop][name]({bbox});
      node[amenity!~"{EXCLUDE_AMENITY}"][name]({bbox});
      node[craft][name]({bbox});
    );
    out body;
    """
    return overpass_query(q)

def fetch_roads(bbox):
    # prendi solo categorie utili per “visibilità su strada”
    q = f"""
    [out:json][timeout:60];
    way[highway~"^(primary|secondary|tertiary|residential|service)$"]({bbox});
    out geom;
    """
    return overpass_query(q)

def run():
    base_bbox = "41.69,13.28,41.77,13.40"  # (S,W,N,E)
    tiles = list(split_bbox(base_bbox, nx=2, ny=2))  # al bisogno aumenta 3x3

    conn = psycopg2.connect(**PG)
    conn.autocommit = True
    try:
        all_businesses = []
        for tb in tiles:
            els = fetch_businesses(tb)
            print(f"[BUS] tile {tb} -> {len(els)} businesses")
            all_businesses.extend(els)
            time.sleep(1)  # rate-limit gentile

        # de-dup per id OSM
        seen = set()
        dedup_businesses = []
        for e in all_businesses:
            key = ("n", e.get("id"))
            if key not in seen:
                seen.add(key)
                dedup_businesses.append(e)

        print(f"[BUS] TOTAL businesses after dedup: {len(dedup_businesses)}")

        for el in dedup_businesses:
            try:
                upsert_business(conn, el)
            except Exception as ex:
                print(f"[WARN] business n{el.get('id')} skipped: {ex}")

        all_roads = []
        for tb in tiles:
            wys = fetch_roads(tb)
            print(f"[ROAD] tile {tb} -> {len(wys)} roads")
            all_roads.extend(wys)
            time.sleep(1)

        seenw = set()
        dedup_roads = []
        for w in all_roads:
            key = ("w", w.get("id"))
            if key not in seenw:
                seenw.add(key)
                dedup_roads.append(w)

        print(f"[ROAD] TOTAL roads after dedup: {len(dedup_roads)}")

        for wy in dedup_roads:
            try:
                upsert_road(conn, wy)
            except Exception as ex:
                print(f"[WARN] road w{wy.get('id')} skipped: {ex}")

    finally:
        conn.close()



if __name__ == "__main__":
    run()
