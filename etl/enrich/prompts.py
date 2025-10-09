from __future__ import annotations

import json
import math
from textwrap import dedent
from typing import Any, Mapping, Optional

SCHEMA_EXAMPLE = {
    "size_class": None,
    "is_chain": None,
    "website_url": "https://www.esempio.it",
    "social": {
        "instagram": "https://www.instagram.com/esempio",
        "facebook": "https://www.facebook.com/esempio",
    },
    "marketing_attitude": None,
    "umbrella_affinity": None,
    "ad_budget_band": None,
    "confidence": None,
    "provenance": {
        "reasoning": "Locale indipendente con forte presenza turistica.",
        "citations": [
            "https://www.esempio.it"
        ],
    },
}


def _format_optional_details(business: Mapping[str, Any]) -> str:
    details: list[str] = []
    place_id = business.get("place_id")
    if place_id:
        details.append(f"- Identificativo interno: {place_id}")

    formatted_address = business.get("formatted_address")
    if formatted_address and formatted_address != business.get("address"):
        details.append(f"- Indirizzo formattato nel dataset: {formatted_address}")

    lat = business.get("latitude")
    lon = business.get("longitude")
    if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
        details.append(f"- Coordinate approssimative (WGS84): lat {lat:.5f}, lon {lon:.5f}")

    radius = business.get("search_radius_m")
    if isinstance(radius, (int, float)) and radius > 0:
        details.append(f"- Raggio di ricerca previsto: ~{int(radius)} metri dalle coordinate.")

    osm_category = business.get("osm_category")
    osm_subtype = business.get("osm_subtype")
    if osm_subtype and osm_subtype != osm_category:
        details.append(f"- Tipologia OSM: {osm_category} -> {osm_subtype}")
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
        street = tags.get("addr:street")
        housenumber = tags.get("addr:housenumber")
        if street or housenumber:
            full = f"{street or ''} {housenumber or ''}".strip()
            if full:
                details.append(f"- Indirizzo OSM dettagliato: {full}")

        locality_parts = [
            tags.get("addr:neighbourhood"),
            tags.get("addr:suburb"),
            tags.get("addr:city"),
        ]
        locality = ", ".join(part for part in locality_parts if part)
        if locality:
            details.append(f"- Localita dai tag OSM: {locality}")

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
    name = business.get("name") or "Attivita sconosciuta"
    category = business.get("category") or "attivita generica"
    address = business.get("address") or ""
    city = business.get("city") or ""
    lat = business.get("latitude")
    lon = business.get("longitude")
    radius_val = business.get("search_radius_m")
    radius = int(radius_val) if isinstance(radius_val, (int, float)) and radius_val > 0 else None
    coords_line = ""
    bbox_line = ""
    if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
        radius_hint = f" (raggio di ricerca ~{radius} m)" if radius else ""
        coords_line = f"- Coordinate: lat {lat:.5f}, lon {lon:.5f}{radius_hint}"
        if radius:
            lat_delta = radius / 111_320
            lon_factor = max(math.cos(math.radians(lat)), 0.05) * 111_320
            lon_delta = radius / lon_factor if lon_factor else 0.0
            bbox_line = (
                f"- Bounding box approssimativo (+/-{radius} m): "
                f"lat {lat - lat_delta:.5f} .. {lat + lat_delta:.5f}, "
                f"lon {lon - lon_delta:.5f} .. {lon + lon_delta:.5f}"
            )
    details = _format_optional_details(business)
    details_block = "\n".join(filter(None, [details, bbox_line]))
    details_block = f"{details_block}\n" if details_block else ""

    schema_block = json.dumps(SCHEMA_EXAMPLE, ensure_ascii=False, indent=2) if include_schema else "{}"

    base = dedent(
        f"""
        Sei un analista marketing locale specializzato in attivita italiane di prossimita.
        Devi arricchire le informazioni dell'attivita descritta qui sotto compilando *solo* i campi dello schema dati.

        Attivita:
        - Nome: {name}
        - Categoria: {category}
        - Indirizzo: {address}
        - Citta: {city}
        {coords_line}
        {details_block}

        Regole:
        - Output: un unico JSON valido (nessun testo prima o dopo).
        - Se non sei certo di un dato, imposta null e riduci "confidence".
        - Lavora SOLO su risultati entro il raggio indicato dalle coordinate: se le fonti portano fuori area o in un comune diverso, lascia i campi stimati a null, imposta "confidence" <= 0.25 e descrivi il problema in "provenance.reasoning".
        - Confronta CAP, provincia e regione nei dettagli con le fonti trovate: eventuali discrepanze vanno motivate in "provenance.reasoning".
        - Riporta le fonti principali (URL) in "provenance.citations" quando disponibili, privilegiando siti istituzionali o elenchi ufficiali italiani.
        - Non calcolare size_class, is_chain, marketing_attitude, umbrella_affinity, ad_budget_band o confidence: restituisci sempre null (saranno calcolati downstream).
        - "social": mappa piattaforma->URL solo se plausibile.
        - "provenance": motivazione sintetica o fonti sicure.

        Schema esempio:
        {schema_block}
        """
    ).strip()

    return base

