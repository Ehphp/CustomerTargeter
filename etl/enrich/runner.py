from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Optional

import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json, RealDictCursor

from .client import CompletionResult, LLMClient, LLMError
from .prompts import build_prompt
from .schema import EnrichedFacts, parse_enriched_facts

logger = logging.getLogger(__name__)

SEARCH_RADIUS_METERS = int(os.getenv("ENRICHMENT_SEARCH_RADIUS_M", "200"))


POSTCODE_RE = re.compile(r"\b\d{4,5}\b")
PROVINCE_CODE_RE = re.compile(r"\b[A-Z]{2}\b$")
WHITESPACE_RE = re.compile(r"\s+")

REGION_KEYWORDS = {
    "abruzzo",
    "basilicata",
    "calabria",
    "campania",
    "emilia-romagna",
    "friuli-venezia giulia",
    "lazio",
    "liguria",
    "lombardia",
    "marche",
    "molise",
    "piemonte",
    "puglia",
    "sardegna",
    "sicilia",
    "toscana",
    "trentino-alto adige",
    "umbria",
    "valle d'aosta",
    "valle d aosta",
    "veneto",
    "provincia autonoma di bolzano",
    "provincia autonoma di trento",
}
COUNTRY_KEYWORDS = {"italia", "italy", "repubblica italiana", "europe", "ue"}


def _normalize(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = WHITESPACE_RE.sub(" ", value).strip()
    return cleaned or None


def _should_skip_part(part: str) -> bool:
    norm = _normalize(part)
    if not norm:
        return True
    lower = norm.lower()
    if lower in COUNTRY_KEYWORDS:
        return True
    if lower in REGION_KEYWORDS:
        return True
    return False


def _sanitize_city_candidate(part: str) -> Optional[str]:
    cleaned = POSTCODE_RE.sub("", part)
    cleaned = PROVINCE_CODE_RE.sub("", cleaned).strip(" ,;/\\-")
    cleaned = WHITESPACE_RE.sub(" ", cleaned).strip()
    if not cleaned:
        return None
    if cleaned.lower() in COUNTRY_KEYWORDS or cleaned.lower() in REGION_KEYWORDS:
        return None
    return cleaned


def _resolve_city(
    city: Optional[str],
    tags: Optional[Mapping[str, Any]],
    formatted_address: Optional[str],
) -> Optional[str]:
    current = _normalize(city)
    if current:
        return current

    if isinstance(tags, Mapping):
        for key in ("addr:city", "addr:town", "addr:village", "addr:hamlet", "addr:municipality", "addr:suburb"):
            value = _normalize(tags.get(key)) if tags else None
            if value:
                return value

    if formatted_address:
        parts = [p.strip() for p in formatted_address.split(",") if p.strip()]
        for part in reversed(parts):
            if _should_skip_part(part):
                continue
            candidate = _sanitize_city_candidate(part)
            if not candidate:
                continue
            if any(ch.isdigit() for ch in candidate):
                candidate = "".join(ch for ch in candidate if not ch.isdigit()).strip()
            candidate = _normalize(candidate)
            if candidate and not candidate.isdigit():
                return candidate
    return None


def _resolve_address(
    address: Optional[str],
    formatted_address: Optional[str],
    tags: Optional[Mapping[str, Any]],
    fallback_city: Optional[str],
) -> Optional[str]:
    primary = _normalize(address)
    if primary:
        return primary

    formatted = _normalize(formatted_address)
    if formatted:
        return formatted

    if not isinstance(tags, Mapping):
        return None

    street = _normalize(tags.get("addr:street") or tags.get("addr:road") or tags.get("addr:place"))
    housenumber = _normalize(tags.get("addr:housenumber") or tags.get("addr:number"))
    locality = (
        _normalize(tags.get("addr:city") or tags.get("addr:town") or tags.get("addr:village") or tags.get("addr:hamlet"))
        or fallback_city
    )
    postcode = _normalize(tags.get("addr:postcode"))

    components: list[str] = []
    if street and housenumber:
        components.append(f"{street} {housenumber}")
    elif street:
        components.append(street)
    elif housenumber:
        components.append(housenumber)

    if locality:
        components.append(locality)
    if postcode:
        components.append(postcode)

    result = ", ".join(dict.fromkeys([c for c in components if c]))
    return result or None


@dataclass
class BusinessRow:
    place_id: str
    name: Optional[str]
    address: Optional[str]
    city: Optional[str]
    category: Optional[str]
    formatted_address: Optional[str]
    has_phone: bool
    has_website: bool
    hours_weekly: Optional[int]
    types: Optional[list[str]]
    tags: Optional[dict[str, Any]]
    osm_category: Optional[str]
    osm_subtype: Optional[str]
    facts_updated_at: Optional[str]
    facts_confidence: Optional[float]
    raw_phone: Optional[str]
    raw_website: Optional[str]
    opening_hours_json: Optional[dict[str, Any]]
    latitude: Optional[float]
    longitude: Optional[float]
    _resolved_city: Optional[str] = field(init=False, repr=False, default=None)
    _resolved_address: Optional[str] = field(init=False, repr=False, default=None)

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "BusinessRow":
        return cls(
            place_id=row["place_id"],
            name=row.get("name"),
            address=row.get("address") or row.get("formatted_address"),
            city=row.get("city"),
            category=row.get("category"),
            formatted_address=row.get("formatted_address"),
            has_phone=bool(row.get("has_phone") or row.get("raw_phone")),
            has_website=bool(row.get("has_website") or row.get("raw_website")),
            hours_weekly=row.get("hours_weekly"),
            types=list(row.get("types") or []),
            tags=row.get("tags"),
            osm_category=row.get("osm_category"),
            osm_subtype=row.get("osm_subtype"),
            facts_updated_at=row.get("facts_updated_at"),
            facts_confidence=row.get("facts_confidence"),
            raw_phone=row.get("raw_phone"),
            raw_website=row.get("raw_website"),
            opening_hours_json=row.get("opening_hours_json"),
            latitude=row.get("latitude"),
            longitude=row.get("longitude"),
        )

    def __post_init__(self) -> None:
        self._resolved_city = _resolve_city(self.city, self.tags, self.formatted_address)
        self._resolved_address = _resolve_address(self.address, self.formatted_address, self.tags, self._resolved_city)

    def resolved_city(self) -> Optional[str]:
        return self._resolved_city or None

    def resolved_address(self) -> Optional[str]:
        return self._resolved_address or None

    def to_prompt_dict(self) -> dict[str, Any]:
        return {
            "place_id": self.place_id,
            "name": self.name,
            "category": self.category,
            "address": self.resolved_address(),
            "city": self.resolved_city(),
            "formatted_address": self.formatted_address,
            "types": self.types,
            "tags": self.tags,
            "osm_category": self.osm_category,
            "osm_subtype": self.osm_subtype,
            "has_phone": self.has_phone,
            "has_website": self.has_website,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "search_radius_m": SEARCH_RADIUS_METERS,
            "notes": (
                f"Orario settimanale dichiarato: {self.hours_weekly} minuti"
                if self.hours_weekly
                else None
            ),
        }

    def hash_input(self, version: int = 1) -> str:
        raw = [
            self.place_id,
            self.name or "",
            self.resolved_address() or "",
            self.resolved_city() or "",
            self.formatted_address or "",
            self.category or "",
            json.dumps(self.types or [], ensure_ascii=False, sort_keys=True),
            json.dumps(self.tags or {}, ensure_ascii=False, sort_keys=True),
            str(self.latitude or ""),
            str(self.longitude or ""),
            str(SEARCH_RADIUS_METERS),
            str(version),
        ]
        digest = hashlib.sha256("|".join(raw).encode("utf-8")).hexdigest()
        return digest

    def to_payload(self, version: int = 1) -> dict[str, Any]:
        return {
            "version": version,
            "place_id": self.place_id,
            "name": self.name,
            "address": self.resolved_address(),
            "city": self.resolved_city(),
            "category": self.category,
            "types": self.types,
            "tags": self.tags,
            "has_phone": self.has_phone,
            "has_website": self.has_website,
            "hours_weekly": self.hours_weekly,
            "formatted_address": self.formatted_address,
            "raw_phone": self.raw_phone,
            "raw_website": self.raw_website,
            "opening_hours": self.opening_hours_json,
            "osm_category": self.osm_category,
            "osm_subtype": self.osm_subtype,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "search_radius_m": SEARCH_RADIUS_METERS,
        }


class EnrichmentRunner:
    """Coordinates DB access and calls to the LLM provider."""

    def __init__(
        self,
        pg: Mapping[str, Any],
        client: Optional[LLMClient],
        provider_name: Optional[str] = None,
        logger_: Optional[logging.Logger] = None,
    ) -> None:
        self.pg = dict(pg)
        self.client = client
        self.provider_name = provider_name or os.getenv("LLM_PROVIDER", "unknown")
        self.log = logger_ or logger
        self.prompt_version = int(os.getenv("ENRICHMENT_PROMPT_VERSION", "2"))
        self.rate_sleep = float(os.getenv("ENRICHMENT_REQUEST_DELAY", "1.0"))

    def run(self, *, limit: int, dry_run: bool = False, force: bool = False, ttl_days: int = 30) -> None:
        with psycopg2.connect(**self.pg) as conn:
            conn.autocommit = False
            candidates = self._fetch_candidates(conn, limit=limit, force=force, ttl_days=ttl_days)
            if not candidates:
                self.log.info("No businesses require enrichment")
                return

            total = len(candidates)
            self.log.info("Processing %d businesses (dry_run=%s)", total, dry_run)
            for idx, row in enumerate(candidates, start=1):
                business = BusinessRow.from_row(row)
                try:
                    self.log.info(
                        "Enrichment progress %d/%d: %s (%s)",
                        idx,
                        total,
                        business.place_id,
                        business.category or "unknown",
                    )
                    if dry_run or self.client is None:
                        self._log_dry_run(business, dry_run)
                        conn.rollback()
                        continue
                    self._process_business(conn, business)
                    conn.commit()
                    time.sleep(self.rate_sleep)
                except Exception as exc:  # noqa: BLE001
                    conn.rollback()
                    self.log.exception("Enrichment failed for %s: %s", business.place_id, exc)

    def _fetch_candidates(
        self,
        conn: psycopg2.extensions.connection,
        *,
        limit: int,
        force: bool,
        ttl_days: int,
    ) -> list[Mapping[str, Any]]:
        clause: str
        params: list[Any]
        if force:
            clause = "TRUE"
            params = [limit]
        else:
            clause = "bf.business_id IS NULL OR bf.updated_at < now() - (%s || ' days')::interval"
            params = [str(ttl_days), limit]

        query = sql.SQL(
            """
            SELECT
              p.place_id,
              p.name,
              COALESCE(NULLIF(p.address, ''), pr.formatted_address) AS address,
              COALESCE(NULLIF(p.city, ''), city_guess.comune) AS city,
              p.category,
              p.has_phone,
              p.has_website,
              p.hours_weekly,
              pr.formatted_address,
              pr.phone AS raw_phone,
              pr.website AS raw_website,
              pr.types,
              pr.opening_hours_json,
              ob.tags,
              ob.category AS osm_category,
              ob.subtype AS osm_subtype,
              bf.updated_at AS facts_updated_at,
              bf.confidence AS facts_confidence,
              ST_Y(p.location::geometry) AS latitude,
              ST_X(p.location::geometry) AS longitude
            FROM places_clean p
            JOIN places_raw pr ON pr.place_id = p.place_id
            LEFT JOIN osm_business ob ON ob.osm_id = SUBSTRING(p.place_id FROM 5)
            LEFT JOIN business_facts bf ON bf.business_id = p.place_id
            LEFT JOIN LATERAL (
              SELECT i.comune
              FROM istat_comuni i
              WHERE i.geom && p.location::geometry
                AND ST_Intersects(i.geom, p.location::geometry)
              ORDER BY i.popolazione DESC NULLS LAST, i.comune
              LIMIT 1
            ) city_guess ON TRUE
            WHERE {where_clause}
            ORDER BY bf.updated_at NULLS FIRST, p.place_id
            LIMIT %s
            """
        ).format(where_clause=sql.SQL(clause))

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
            return rows

    def _log_dry_run(self, business: BusinessRow, dry_run: bool) -> None:
        prompt = build_prompt(business.to_prompt_dict())
        mode = "dry-run" if dry_run else "no-client"
        self.log.info(
            "[%s] Would enrich %s (%s) – prompt snippet:\n%s",
            mode,
            business.place_id,
            business.category,
            prompt[:400],
        )

    def _process_business(self, conn: psycopg2.extensions.connection, business: BusinessRow) -> None:
        assert self.client is not None
        payload = business.to_payload(version=self.prompt_version)
        input_hash = business.hash_input(version=self.prompt_version)
        provider = self.provider_name or self.client.__class__.__name__

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            request_id = self._upsert_request(cur, business.place_id, provider, input_hash, payload)

        prompt = build_prompt(business.to_prompt_dict())
        try:
            result = self.client.complete(prompt=prompt)
        except LLMError as exc:
            self._mark_request_error(conn, request_id, str(exc))
            raise
        try:
            facts = parse_enriched_facts(result.text)
        except ValueError as exc:
            snippet = result.text.strip()
            snippet = snippet[:500] + ("…" if len(snippet) > 500 else "")
            self.log.error(
                "Failed to parse LLM response for %s: %s | response snippet: %s",
                business.place_id,
                exc,
                snippet,
            )
            self._mark_request_error(conn, request_id, str(exc))
            raise

        with conn.cursor() as cur:
            self._store_response(cur, request_id, result, facts)
            self._upsert_business_facts(cur, business.place_id, provider, result, facts)
            self._mark_request_complete(cur, request_id)

    def _upsert_request(
        self,
        cur: psycopg2.extensions.cursor,
        business_id: str,
        provider: str,
        input_hash: str,
        payload: dict[str, Any],
    ) -> str:
        request_id = str(uuid.uuid4())
        cur.execute(
            """
            INSERT INTO enrichment_request (
              request_id, business_id, provider, input_hash, input_payload, status, created_at, started_at
            )
            VALUES (%s, %s, %s, %s, %s, 'running', now(), now())
            ON CONFLICT (business_id, input_hash) DO UPDATE
            SET provider = EXCLUDED.provider,
                input_payload = EXCLUDED.input_payload,
                status = 'running',
                error = NULL,
                started_at = now(),
                finished_at = NULL
            RETURNING request_id
            """,
            (request_id, business_id, provider, input_hash, Json(payload)),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("Failed to persist enrichment_request row")
        if isinstance(row, dict):
            return row["request_id"]
        return row[0]

    def _store_response(
        self,
        cur: psycopg2.extensions.cursor,
        request_id: str,
        result: CompletionResult,
        facts: EnrichedFacts,
    ) -> None:
        response_id = str(uuid.uuid4())
        cur.execute(
            """
            INSERT INTO enrichment_response (
              response_id, request_id, model, raw_response, parsed_response,
              prompt_tokens, completion_tokens, cost_cents, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
            """,
            (
                response_id,
                request_id,
                result.model,
                Json(result.raw),
                Json(facts.model_dump(mode="json")),
                result.prompt_tokens,
                result.completion_tokens,
                result.cost_cents,
            ),
        )

    def _upsert_business_facts(
        self,
        cur: psycopg2.extensions.cursor,
        business_id: str,
        provider: str,
        result: CompletionResult,
        facts: EnrichedFacts,
    ) -> None:
        facts_json = facts.model_dump(mode="json")
        social = facts_json.get("social")
        cur.execute(
            """
            INSERT INTO business_facts (
              business_id, size_class, is_chain, website_url, social,
              marketing_attitude, umbrella_affinity, ad_budget_band,
              budget_source, confidence, provenance, updated_at,
              source_provider, source_model
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now(), %s, %s)
            ON CONFLICT (business_id) DO UPDATE SET
              size_class = EXCLUDED.size_class,
              is_chain = EXCLUDED.is_chain,
              website_url = EXCLUDED.website_url,
              social = EXCLUDED.social,
              marketing_attitude = EXCLUDED.marketing_attitude,
              umbrella_affinity = EXCLUDED.umbrella_affinity,
              ad_budget_band = EXCLUDED.ad_budget_band,
              budget_source = EXCLUDED.budget_source,
              confidence = EXCLUDED.confidence,
              provenance = EXCLUDED.provenance,
              updated_at = EXCLUDED.updated_at,
              source_provider = EXCLUDED.source_provider,
              source_model = EXCLUDED.source_model
            """,
            (
                business_id,
                facts_json.get("size_class"),
                facts_json.get("is_chain"),
                facts_json.get("website_url"),
                Json(social) if social else None,
                facts_json.get("marketing_attitude"),
                facts_json.get("umbrella_affinity"),
                facts_json.get("ad_budget_band"),
                "LLM_infer" if facts_json.get("ad_budget_band") else None,
                facts_json.get("confidence"),
                Json(facts_json.get("provenance")) if facts_json.get("provenance") else None,
                provider,
                result.model,
            ),
        )

    def _mark_request_complete(self, cur: psycopg2.extensions.cursor, request_id: str) -> None:
        cur.execute(
            """
            UPDATE enrichment_request
               SET status = 'completed',
                   error = NULL,
                   finished_at = now()
             WHERE request_id = %s
            """,
            (request_id,),
        )

    def _mark_request_error(self, conn: psycopg2.extensions.connection, request_id: str, message: str) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE enrichment_request
                   SET status = 'error',
                       error = %s,
                       finished_at = now()
                 WHERE request_id = %s
                """,
                (message[:500], request_id),
            )
