from __future__ import annotations

from typing import Any, Dict, Mapping, Optional

# Heuristic maps for affinity scoring by category.
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

CHAIN_KEYWORDS = {
    "coop",
    "conad",
    "esselunga",
    "iper",
    "ipercoop",
    "md ",
    "lidl",
    "carrefour",
    "pam",
    "penny",
    "mcdonald",
    "kfc",
    "burger king",
    "subway",
    "decathlon",
    "ikea",
    "h&m",
    "ovs",
    "oviesse",
    "upim",
    "ovs",
    "foot locker",
    "unicredit",
    "intesa sanpaolo",
    "poste italiane",
}

SOCIAL_TAG_KEYS = {
    "contact:facebook": "facebook",
    "contact:instagram": "instagram",
    "contact:twitter": "twitter",
    "facebook": "facebook",
    "instagram": "instagram",
    "twitter": "twitter",
    "contact:linkedin": "linkedin",
    "linkedin": "linkedin",
}


def _normalize_category(*values: Optional[str]) -> Optional[str]:
    for value in values:
        if value:
            token = value.strip().lower().replace("-", "_").replace(" ", "_")
            if token:
                return token
    return None


def estimate_size_class(
    category: Optional[str],
    osm_subtype: Optional[str],
    osm_category: Optional[str],
    is_chain_hint: Optional[bool] = None,
) -> Optional[str]:
    token = _normalize_category(category, osm_subtype, osm_category)
    if token is None:
        return "micro"
    if is_chain_hint:
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
    base = size_map.get(size_class or "", None)
    token = _normalize_category(category)
    if token in {"lawyer", "notary", "accountant", "dentist"}:
        return "medio" if base == "alto" else "basso"
    if token in {"supermarket", "shopping_centre"}:
        return "alto"
    if token in {"bar", "cafe", "pizzeria", "gelateria", "restaurant"} and base:
        return "medio"
    return base


def default_affinity(category: Optional[str], osm_subtype: Optional[str] = None) -> float:
    token = _normalize_category(category, osm_subtype)
    if not token:
        return 0.5
    for key, value in AFFINITY_RULES.items():
        if key in token:
            return value
    return 0.5


def _detect_brand(tags: Mapping[str, Any] | None, name: Optional[str]) -> bool:
    if tags:
        for key in ("brand", "operator", "network"):
            value = tags.get(key)
            if isinstance(value, str) and value.strip():
                return True
    if name:
        lowered = name.lower()
        return any(keyword in lowered for keyword in CHAIN_KEYWORDS)
    return False


def extract_social_from_tags(tags: Mapping[str, Any] | None) -> Dict[str, str]:
    if not isinstance(tags, Mapping):
        return {}
    social: Dict[str, str] = {}
    for key, value in tags.items():
        if not isinstance(value, str):
            continue
        platform = SOCIAL_TAG_KEYS.get(key.lower())
        if platform and value.strip():
            social[platform] = value.strip()
    return social


def estimate_is_chain(
    category: Optional[str],
    tags: Mapping[str, Any] | None,
    name: Optional[str],
    size_class: Optional[str],
    existing_hint: Optional[bool] = None,
) -> Optional[bool]:
    if existing_hint is not None:
        return existing_hint
    brand = _detect_brand(tags, name)
    if brand:
        return True
    token = _normalize_category(category)
    if token in {"supermarket", "hypermarket", "shopping_centre"}:
        return True
    if size_class in {"media", "grande"} and token in {"gym", "fitness_centre", "department_store"}:
        return True
    return False


def estimate_marketing_attitude(
    has_website: bool,
    social: Mapping[str, str] | None,
    tags: Mapping[str, Any] | None,
    hours_weekly: Optional[int],
    brand_present: bool,
) -> Optional[float]:
    score = 0.25
    if has_website:
        score += 0.3
    social_count = len(social or {})
    if social_count:
        score += min(0.25, 0.12 * social_count)
    if tags:
        # contact emails or booking links indicate marketing attitude
        for key in ("contact:email", "contact:website", "booking", "contact:whatsapp"):
            if isinstance(tags.get(key), str) and tags[key].strip():
                score += 0.1
                break
    if brand_present:
        score += 0.05
    if isinstance(hours_weekly, int) and hours_weekly > 40 * 60:
        score += 0.05
    return min(1.0, round(score, 2))


def estimate_confidence(
    has_website: bool,
    has_phone: bool,
    social: Mapping[str, str] | None,
    brand_present: bool,
    marketing_attitude: Optional[float],
    size_class: Optional[str],
) -> Optional[float]:
    score = 0.35
    if has_website:
        score += 0.2
    if has_phone:
        score += 0.1
    if social:
        score += 0.15
    if brand_present or size_class in {"media", "grande"}:
        score += 0.1
    if marketing_attitude:
        score += 0.1 * marketing_attitude
    return min(0.95, round(score, 2))


def compute_business_facts(
    *,
    name: Optional[str],
    category: Optional[str],
    osm_category: Optional[str],
    osm_subtype: Optional[str],
    tags: Mapping[str, Any] | None,
    has_website: bool,
    has_phone: bool,
    hours_weekly: Optional[int],
    existing: Mapping[str, Any],
) -> Dict[str, Any]:
    result: Dict[str, Any] = {}

    brand_present = _detect_brand(tags, name)
    existing_social = existing.get("social")
    social = existing_social if isinstance(existing_social, Mapping) else None
    if not social:
        derived_social = extract_social_from_tags(tags)
        if derived_social:
            social = derived_social
            result["social"] = derived_social

    size_class = existing.get("size_class")
    is_chain_hint = existing.get("is_chain")
    size_class = size_class or estimate_size_class(category, osm_subtype, osm_category, is_chain_hint)
    result["size_class"] = size_class

    is_chain = estimate_is_chain(category, tags, name, size_class, is_chain_hint)
    result["is_chain"] = is_chain

    budget = existing.get("ad_budget_band") or infer_budget_band(size_class, category)
    result["ad_budget_band"] = budget

    affinity = existing.get("umbrella_affinity")
    if affinity is None:
        affinity = default_affinity(category, osm_subtype)
    result["umbrella_affinity"] = affinity

    marketing = existing.get("marketing_attitude")
    if marketing is None:
        marketing = estimate_marketing_attitude(
            has_website=has_website,
            social=social,
            tags=tags,
            hours_weekly=hours_weekly,
            brand_present=brand_present,
        )
    result["marketing_attitude"] = marketing

    confidence = existing.get("confidence")
    if confidence is None:
        confidence = estimate_confidence(
            has_website=has_website,
            has_phone=has_phone,
            social=social,
            brand_present=brand_present,
            marketing_attitude=marketing,
            size_class=size_class,
        )
    result["confidence"] = confidence

    return result

