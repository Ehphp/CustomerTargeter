from __future__ import annotations

import logging
import math
import os
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor, execute_values

logger = logging.getLogger("feature_builder")


AFFINITY_RULES: Dict[str, float] = {
    "bar": 0.9,
    "cafe": 0.85,
    "coffee": 0.85,
    "pub": 0.85,
    "restaurant": 0.9,
    "pizzeria": 0.9,
    "gelateria": 0.9,
    "ice_cream": 0.9,
    "bakery": 0.8,
    "takeaway": 0.85,
    "fast_food": 0.8,
    "clothes": 0.7,
    "fashion": 0.7,
    "beauty": 0.7,
    "hairdresser": 0.6,
    "gym": 0.75,
    "fitness": 0.75,
    "pharmacy": 0.6,
    "optician": 0.6,
    "supermarket": 0.7,
    "convenience": 0.7,
    "boutique": 0.6,
    "professional": 0.4,
    "lawyer": 0.3,
    "notary": 0.3,
    "mechanic": 0.2,
    "car_repair": 0.2,
}

HIGH_TRAFFIC_HIGHWAYS = {"motorway", "trunk", "primary", "secondary", "tertiary"}


@dataclass
class MetricsRow:
    business_id: str
    sector_density_neighbors: int
    sector_density_score: float
    geo_distribution_label: str
    geo_distribution_source: str
    size_class: Optional[str]
    is_chain: Optional[bool]
    ad_budget_band: Optional[str]
    umbrella_affinity: Optional[float]
    digital_presence: Optional[float]
    digital_presence_confidence: Optional[float]
    marketing_attitude: Optional[float]
    facts_confidence: Optional[float]


def build_pg_config() -> Dict[str, str]:
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }


def fetch_base_rows(conn: psycopg2.extensions.connection) -> List[Mapping[str, Any]]:
    query = """
        SELECT
          p.place_id,
          p.name,
          p.category,
          p.city,
          p.has_website,
          p.hours_weekly,
          psd.neighbor_count,
          psd.density_score,
          bf.size_class,
          bf.is_chain,
          bf.website_url,
          bf.social,
          bf.marketing_attitude,
          bf.umbrella_affinity,
          bf.ad_budget_band,
          bf.confidence,
          z.label     AS zone_label,
          z.kind      AS zone_kind,
          st.dist     AS station_distance,
          road.near_highway
        FROM places_clean p
        LEFT JOIN place_sector_density psd ON psd.place_id = p.place_id
        LEFT JOIN business_facts bf ON bf.business_id = p.place_id
        LEFT JOIN LATERAL (
          SELECT gz.label, gz.kind
          FROM geo_zones gz
          WHERE ST_Intersects(gz.geom, p.location::geometry)
          ORDER BY gz.priority
          LIMIT 1
        ) z ON TRUE
        LEFT JOIN LATERAL (
          SELECT MIN(ST_Distance(p.location, s.geom)) AS dist
          FROM brello_stations s
        ) st ON TRUE
        LEFT JOIN LATERAL (
          SELECT EXISTS (
            SELECT 1
            FROM osm_roads r
            WHERE r.highway = ANY(%s)
              AND ST_DWithin(p.location, r.geom, 50)
          ) AS near_highway
        ) road ON TRUE
    """
    params = (list(HIGH_TRAFFIC_HIGHWAYS),)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, params)
        return cur.fetchall()


def _category_token(category: Optional[str]) -> Optional[str]:
    if not category:
        return None
    token = category.lower().strip()
    return token or None


def resolve_size_class(facts_size: Optional[str], category: Optional[str], is_chain: Optional[bool]) -> Optional[str]:
    if facts_size:
        return facts_size
    token = _category_token(category)
    if token is None:
        return "micro"
    if is_chain:
        if token in {"supermarket", "shopping_centre", "department_store"}:
            return "grande"
        return "media"
    if token in {"supermarket", "hypermarket", "shopping_centre"}:
        return "grande"
    if token in {"gym", "fitness_centre", "car_dealer"}:
        return "media"
    if token in {"restaurant", "pizzeria", "fast_food", "pub", "bar", "cafe"}:
        return "piccola"
    if token in {"pharmacy", "hairdresser", "beauty_salon", "optician"}:
        return "piccola"
    if token in {"lawyer", "notary", "accountant"}:
        return "micro"
    return "micro"


def infer_budget_band(size_class: Optional[str], category: Optional[str]) -> Optional[str]:
    size_map = {
        "micro": "basso",
        "piccola": "medio",
        "media": "medio",
        "grande": "alto",
    }
    base = size_map.get(size_class or "")
    token = _category_token(category)
    if token in {"lawyer", "notary", "accountant", "dentist"}:
        return "medio" if base == "alto" else "basso"
    if token in {"supermarket", "shopping_centre"}:
        return "alto"
    if token in {"bar", "cafe", "pizzeria", "gelateria", "restaurant"} and base:
        return "medio"
    return base


def default_affinity(category: Optional[str]) -> float:
    token = _category_token(category)
    if not token:
        return 0.5
    for key, value in AFFINITY_RULES.items():
        if key in token:
            return value
    return 0.5


def compute_geo_distribution(row: Mapping[str, Any]) -> Tuple[str, str]:
    dist = row.get("station_distance")
    near_highway = row.get("near_highway")
    zone_label = row.get("zone_label")
    zone_kind = (row.get("zone_kind") or "").lower() if row.get("zone_kind") else ""

    if dist is not None and not math.isnan(dist) and dist <= 100:
        return "vicino_brello", "brello_station"
    if near_highway:
        return "passaggio", "road_high_traffic"
    if zone_label:
        if zone_kind in {"centro", "center", "historic"}:
            return "centro", f"geo_zone:{zone_label}"
        return zone_label, f"geo_zone:{zone_label}"
    return "altro", "fallback"


def compute_digital_presence(row: Mapping[str, Any], has_website: bool) -> Tuple[float, float]:
    website_url = row.get("website_url")
    social = row.get("social") or {}
    if isinstance(social, str):
        # fallback if stored as JSON string
        try:
            import json

            social = json.loads(social)
        except Exception:  # noqa: BLE001
            social = {}

    social_count = len([k for k in social.keys() if social[k]])
    marketing_attitude = row.get("marketing_attitude") or 0.0
    confidence = row.get("confidence")

    score = 0.0
    if has_website or website_url:
        score += 0.4
    if social_count:
        score += min(0.4, 0.4 * min(social_count, 3) / 3.0)
    if marketing_attitude:
        score += min(0.2, float(marketing_attitude) * 0.2)
    score = min(1.0, score)

    conf = confidence if isinstance(confidence, (int, float)) else 0.4
    if has_website or website_url:
        conf = min(1.0, conf + 0.1)
    if social_count:
        conf = min(1.0, conf + 0.1)
    return score, conf


def compute_metrics_rows(rows: Sequence[Mapping[str, Any]]) -> List[MetricsRow]:
    metrics: List[MetricsRow] = []
    for row in rows:
        business_id = row["place_id"]
        has_website = bool(row.get("has_website"))
        sector_neighbors = int(row.get("neighbor_count") or 0)
        sector_score = float(row.get("density_score") or 0.0)

        size_class = resolve_size_class(row.get("size_class"), row.get("category"), row.get("is_chain"))
        is_chain = row.get("is_chain")
        if is_chain is None and size_class in {"media", "grande"}:
            is_chain = True if (row.get("category") and "cooperative" in row["category"]) else None

        budget_band = row.get("ad_budget_band") or infer_budget_band(size_class, row.get("category"))
        affinity = row.get("umbrella_affinity")
        if affinity is None:
            affinity = default_affinity(row.get("category"))
        digital_presence, digital_confidence = compute_digital_presence(row, has_website)
        marketing_attitude = row.get("marketing_attitude")
        facts_confidence = row.get("confidence")

        geo_label, geo_source = compute_geo_distribution(row)

        metrics.append(
            MetricsRow(
                business_id=business_id,
                sector_density_neighbors=sector_neighbors,
                sector_density_score=sector_score,
                geo_distribution_label=geo_label,
                geo_distribution_source=geo_source,
                size_class=size_class,
                is_chain=is_chain,
                ad_budget_band=budget_band,
                umbrella_affinity=affinity,
                digital_presence=digital_presence,
                digital_presence_confidence=digital_confidence,
                marketing_attitude=marketing_attitude,
                facts_confidence=facts_confidence,
            )
        )
    return metrics


def upsert_metrics(conn: psycopg2.extensions.connection, rows: Sequence[MetricsRow]) -> None:
    if not rows:
        logger.info("No metrics to upsert")
        return

    records = [
        (
            r.business_id,
            r.sector_density_neighbors,
            r.sector_density_score,
            r.geo_distribution_label,
            r.geo_distribution_source,
            r.size_class,
            r.is_chain,
            r.ad_budget_band,
            r.umbrella_affinity,
            r.digital_presence,
            r.digital_presence_confidence,
            r.marketing_attitude,
            r.facts_confidence,
        )
        for r in rows
    ]

    stmt = """
        INSERT INTO business_metrics (
          business_id,
          sector_density_neighbors,
          sector_density_score,
          geo_distribution_label,
          geo_distribution_source,
          size_class,
          is_chain,
          ad_budget_band,
          umbrella_affinity,
          digital_presence,
          digital_presence_confidence,
          marketing_attitude,
          facts_confidence
        )
        VALUES %s
        ON CONFLICT (business_id) DO UPDATE SET
          sector_density_neighbors = EXCLUDED.sector_density_neighbors,
          sector_density_score = EXCLUDED.sector_density_score,
          geo_distribution_label = EXCLUDED.geo_distribution_label,
          geo_distribution_source = EXCLUDED.geo_distribution_source,
          size_class = EXCLUDED.size_class,
          is_chain = EXCLUDED.is_chain,
          ad_budget_band = EXCLUDED.ad_budget_band,
          umbrella_affinity = EXCLUDED.umbrella_affinity,
          digital_presence = EXCLUDED.digital_presence,
          digital_presence_confidence = EXCLUDED.digital_presence_confidence,
          marketing_attitude = EXCLUDED.marketing_attitude,
          facts_confidence = EXCLUDED.facts_confidence,
          updated_at = now()
    """

    with conn.cursor() as cur:
        execute_values(cur, stmt, records)


def main() -> int:
    root_dir = os.path.dirname(__file__)
    load_dotenv(os.path.join(root_dir, "..", ".env"))
    logging.basicConfig(
        level=os.getenv("FEATURE_BUILDER_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    pg = build_pg_config()
    with psycopg2.connect(**pg) as conn:
        conn.autocommit = False
        rows = fetch_base_rows(conn)
        logger.info("Fetched %d base rows for metric computation", len(rows))
        metrics = compute_metrics_rows(rows)
        upsert_metrics(conn, metrics)
        conn.commit()
        logger.info("business_metrics updated")
    return 0


if __name__ == "__main__":
    sys.exit(main())
