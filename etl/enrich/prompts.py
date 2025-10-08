from __future__ import annotations

import json
from textwrap import dedent
from typing import Any, Mapping, Optional

SCHEMA_EXAMPLE = {
    "size_class": "piccola",
    "is_chain": False,
    "website_url": "https://www.esempio.it",
    "social": {
        "instagram": "https://www.instagram.com/esempio",
        "facebook": "https://www.facebook.com/esempio",
    },
    "marketing_attitude": 0.7,
    "umbrella_affinity": 0.9,
    "ad_budget_band": "medio",
    "confidence": 0.72,
    "provenance": {
        "reasoning": "Locale indipendente con forte presenza turistica."
    },
}


def _format_optional_details(business: Mapping[str, Any]) -> str:
    details: list[str] = []
    formatted_address = business.get("formatted_address")
    if formatted_address and formatted_address != business.get("address"):
        details.append(f"- Indirizzo formattato nel dataset: {formatted_address}")

    lat = business.get("latitude")
    lon = business.get("longitude")
    if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
        details.append(f"- Coordinate approssimative (WGS84): lat {lat:.5f}, lon {lon:.5f}")

    osm_category = business.get("osm_category")
    osm_subtype = business.get("osm_subtype")
    if osm_subtype and osm_subtype != osm_category:
        details.append(f"- Tipologia OSM: {osm_category} → {osm_subtype}")
    elif osm_category:
        details.append(f"- Tipologia OSM: {osm_category}")

    types = business.get("types") or []
    if types:
        joined = ", ".join(str(t) for t in types if t)
        if joined:
            details.append(f"- Tags categoria: {joined}")

    if business.get("has_website"):
        details.append("- Il dataset riporta un sito web attivo.")
    if business.get("has_phone"):
        details.append("- Il dataset riporta un recapito telefonico.")

    tags = business.get("tags")
    if isinstance(tags, dict):
        postcode = tags.get("addr:postcode")
        province = tags.get("addr:province") or tags.get("addr:state")
        region = tags.get("addr:region")
        if postcode:
            details.append(f"- CAP indicato: {postcode}")
        if province:
            details.append(f"- Provincia indicata: {province}")
        if region:
            details.append(f"- Regione indicata: {region}")
        cuisine = tags.get("cuisine")
        if cuisine:
            details.append(f"- Cucina dichiarata: {cuisine}")
        brand = tags.get("brand")
        if brand:
            details.append(f"- Brand associato: {brand}")

    notes = business.get("notes")
    if notes:
        details.append(f"- Note aggiuntive: {notes}")

    if not details:
        return ""

    return "Dettagli aggiuntivi utili:\n" + "\n".join(details)


def build_prompt(business: Mapping[str, Any], include_schema: bool = True) -> str:
    """Craft the user prompt sent to the LLM."""
    name = business.get("name") or "Attività sconosciuta"
    category = business.get("category") or "attività generica"
    address = business.get("address") or ""
    city = business.get("city") or ""
    lat = business.get("latitude")
    lon = business.get("longitude")
    coords_line = ""
    if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
        coords_line = f"- Coordinate: lat {lat:.5f}, lon {lon:.5f}"
    details = _format_optional_details(business)
    details_block = f"{details}\n" if details else ""

    schema_block = json.dumps(SCHEMA_EXAMPLE, ensure_ascii=False, indent=2) if include_schema else "{}"

    base = dedent(
        f"""
        Sei un analista marketing locale specializzato in attività italiane di prossimità.
        Devi arricchire le informazioni dell'attività descritta qui sotto compilando *solo* i campi dello schema dati.

        Attività:
        - Nome: {name}
        - Categoria: {category}
        - Indirizzo: {address}
        - Città: {city}
        {coords_line}
        {details_block}

        Regole:
        - Output: un unico JSON valido (nessun testo prima o dopo).
        - Se non sei certo di un dato, imposta null e riduci "confidence".
        - Usa le coordinate WGS84 e gli indizi (CAP, provincia, regione) per validare il comune: se resti in dubbio lascia "city" a null e spiega la scelta in "provenance".
        - "size_class": micro, piccola, media o grande considerando il contesto italiano.
        - "umbrella_affinity": punteggio 0..1 su quanto un ombrello brandizzato Brellò è coerente con il target.
        - "ad_budget_band": stima prudente (basso, medio, alto) basata su categoria e dimensione.
        - "social": mappa piattaforma->URL solo se plausibile.
        - "provenance": motivazione sintetica o fonti sicure.

        Schema esempio:
        {schema_block}
        """
    ).strip()

    return base
