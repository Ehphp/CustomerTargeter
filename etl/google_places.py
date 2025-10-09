from __future__ import annotations

import argparse
import json
import logging
import math
import os
import time
from typing import Iterable, List, Mapping, Optional, Set, Tuple

import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

logger = logging.getLogger("etl.google_places")

API = os.getenv("GOOGLE_PLACES_API_KEY")
if not API:
    raise SystemExit("GOOGLE_PLACES_API_KEY non configurata nel .env")

PG = dict(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=os.getenv("POSTGRES_PORT", "5432"),
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
)

FIELDS = (
    "place_id,name,formatted_address,formatted_phone_number,website,"
    "types,rating,user_ratings_total,opening_hours,geometry/location"
)
TEXT_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"
GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"


def text_search(
    session: requests.Session,
    query: str,
    lat: float,
    lng: float,
    radius: int,
    sleep_seconds: float,
) -> Iterable[str]:
    params = {"key": API, "query": query, "location": f"{lat},{lng}", "radius": radius}
    logger.info("Text search '%s' (r=%sm)", query, radius)
    page = 0
    while True:
        response = session.get(TEXT_URL, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
        status = payload.get("status", "UNKNOWN")
        logger.debug(
            "Text search page %s status=%s, results=%s",
            page,
            status,
            len(payload.get("results", [])),
        )
        if status == "ZERO_RESULTS":
            logger.info("Nessun risultato per '%s' (status ZERO_RESULTS)", query)
            break
        if status != "OK":
            logger.error(
                "Google Places Text Search status %s per '%s': %s",
                status,
                query,
                payload.get("error_message"),
            )
            break
        for res in payload.get("results", []):
            pid = res.get("place_id")
            if pid:
                yield pid
        token = payload.get("next_page_token")
        if not token:
            break
        time.sleep(sleep_seconds)
        page += 1
        params = {"key": API, "pagetoken": token}


def fetch_details(session: requests.Session, place_id: str) -> Optional[dict]:
    resp = session.get(
        DETAILS_URL,
        params={"key": API, "place_id": place_id, "fields": FIELDS},
        timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()
    status = payload.get("status")
    logger.debug("Details status=%s per place_id=%s", status, place_id)
    if status and status != "OK":
        logger.warning("Place details status %s per %s: %s", status, place_id, payload.get("error_message"))
        return None
    return payload.get("result")


def upsert_place(conn: psycopg2.extensions.connection, record: Mapping[str, object]) -> None:
    geometry = record.get("geometry") or {}
    location = geometry.get("location") if isinstance(geometry, Mapping) else {}
    lat = location.get("lat") if isinstance(location, Mapping) else None
    lng = location.get("lng") if isinstance(location, Mapping) else None
    if lat is None or lng is None:
        logger.debug("Skipping %s: missing coordinates", record.get("place_id"))
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO places_raw (
              place_id, name, formatted_address, phone, website, types,
              rating, user_ratings_total, opening_hours_json, location, source_ts
            )
            VALUES (
              %(place_id)s, %(name)s, %(formatted_address)s, %(phone)s, %(website)s, %(types)s,
              %(rating)s, %(user_ratings_total)s, %(opening_hours_json)s,
              ST_SetSRID(ST_MakePoint(%(lng)s, %(lat)s), 4326)::geography,
              now()
            )
            ON CONFLICT (place_id) DO UPDATE SET
              name = EXCLUDED.name,
              formatted_address = EXCLUDED.formatted_address,
              phone = EXCLUDED.phone,
              website = EXCLUDED.website,
              types = EXCLUDED.types,
              rating = EXCLUDED.rating,
              user_ratings_total = EXCLUDED.user_ratings_total,
              opening_hours_json = EXCLUDED.opening_hours_json,
              location = EXCLUDED.location,
              source_ts = now()
            """,
            {
                "place_id": record["place_id"],
                "name": record.get("name"),
                "formatted_address": record.get("formatted_address"),
                "phone": record.get("formatted_phone_number"),
                "website": record.get("website"),
                "types": record.get("types"),
                "rating": record.get("rating"),
                "user_ratings_total": record.get("user_ratings_total"),
                "opening_hours_json": json.dumps(record.get("opening_hours")) if record.get("opening_hours") else None,
                "lat": lat,
                "lng": lng,
            },
        )


def parse_queries(args: argparse.Namespace) -> List[str]:
    queries: List[str] = []
    if args.queries:
        queries.extend(args.queries)
    if args.queries_file:
        with open(args.queries_file, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line and not line.startswith("#"):
                    queries.append(line)
    queries = [q.strip() for q in queries if q.strip()]
    if not queries:
        raise SystemExit("Nessuna query specificata (usa --queries o --queries-file).")
    return queries


def geocode_location(
    session: requests.Session,
    location: str,
    fallback_radius: int,
) -> Tuple[float, float, int]:
    logger.info("Geocoding '%s'...", location)
    resp = session.get(GEOCODE_URL, params={"address": location, "key": API}, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    status = payload.get("status", "UNKNOWN")
    if status != "OK":
        raise SystemExit(f"Geocoding status {status} per '{location}': {payload.get('error_message')}")
    result = payload["results"][0]
    geometry = result["geometry"]
    loc = geometry["location"]
    lat = float(loc["lat"])
    lng = float(loc["lng"])
    viewport = geometry.get("viewport")
    radius = fallback_radius
    if viewport:
        ne = viewport["northeast"]
        sw = viewport["southwest"]
        lat_radius = abs(ne["lat"] - sw["lat"]) * 111_320 / 2
        lon_factor = max(math.cos(math.radians(lat)), 0.05) * 111_320
        lon_radius = abs(ne["lng"] - sw["lng"]) * lon_factor / 2
        computed = int(max(lat_radius, lon_radius))
        if computed > 0:
            radius = max(radius, computed)
    logger.info(
        "Geocoding completato: lat=%.6f, lon=%.6f, radius=%sm",
        lat,
        lng,
        radius,
    )
    return lat, lng, radius


def run(args: argparse.Namespace) -> None:
    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s [%(levelname)s] %(message)s")
    queries = parse_queries(args)
    seen: Set[str] = set()

    session = requests.Session()

    radius = args.radius
    lat = args.lat
    lng = args.lng

    if args.location:
        loc_lat, loc_lng, loc_radius = geocode_location(session, args.location, radius or 0)
        if lat is None:
            lat = loc_lat
        if lng is None:
            lng = loc_lng
        radius = loc_radius if args.radius is None else max(args.radius, loc_radius)

    if lat is None or lng is None:
        raise SystemExit("Specificare una coppia lat/lng oppure usare --location.")

    if radius is None:
        radius = 3000

    radius = int(radius)
    logger.info("Coordinate finali: lat=%.6f, lon=%.6f, radius=%sm", lat, lng, radius)

    with psycopg2.connect(**PG) as conn:
        conn.autocommit = True
        for query in queries:
            for place_id in text_search(session, query, lat, lng, radius, args.sleep_seconds):
                if args.limit and len(seen) >= args.limit:
                    logger.info("Limit %s raggiunto", args.limit)
                    return
                if place_id in seen:
                    continue
                seen.add(place_id)
                detail = fetch_details(session, place_id)
                if not detail:
                    continue
                upsert_place(conn, detail)
        logger.info("Upsert completato (%d place_id)", len(seen))


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Importa POI da Google Places in places_raw.")
    parser.add_argument("--lat", type=float, help="Latitudine di riferimento (gradi decimali).")
    parser.add_argument("--lng", type=float, help="Longitudine di riferimento (gradi decimali).")
    parser.add_argument("--location", help="Indirizzo o città (usa geocoding per ricavare lat/lon).")
    parser.add_argument("--radius", type=int, default=None, help="Raggio di ricerca in metri (default 3000, o bounding box geocoding).")
    parser.add_argument("--queries", nargs="*", help="Lista di query text-search (es. 'ristorante', 'bar').")
    parser.add_argument("--queries-file", help="File testo con una query per riga (commenti con #).")
    parser.add_argument("--limit", type=int, help="Massimo numero di place_id da importare.")
    parser.add_argument("--sleep-seconds", type=float, default=2.0, help="Delay tra pagine successive (default 2s).")
    parser.add_argument("--log-level", default="INFO", help="Livello di logging (INFO/DEBUG/...).")
    return parser


if __name__ == "__main__":
    run(build_arg_parser().parse_args())
