from __future__ import annotations

from typing import Any, Dict, Mapping, Optional, Sequence

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
    "upim",
    "foot locker",
    "unicredit",
    "intesa sanpaolo",
    "poste italiane",
}


def _normalize_token(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    token = value.strip().lower().replace("-", "_").replace(" ", "_")
    return token or None


def _normalize_category(*values: Optional[str]) -> Optional[str]:
    for value in values:
        token = _normalize_token(value)
        if token:
            return token
    return None


def estimate_size_class(
    category: Optional[str],
    types: Optional[Sequence[str]] = None,
    is_chain_hint: Optional[bool] = None,
) -> Optional[str]:
    type_tokens = list(types or [])
    token = _normalize_category(category, *type_tokens)
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
    base = size_map.get(size_class or "")
    token = _normalize_category(category)
    if token in {"lawyer", "notary", "accountant", "dentist"}:
        return "medio" if base == "alto" else "basso"
    if token in {"supermarket", "shopping_centre"}:
        return "alto"
    if token in {"bar", "cafe", "pizzeria", "gelateria", "restaurant"} and base:
        return "medio"
    return base


def default_affinity(category: Optional[str], types: Optional[Sequence[str]] = None) -> float:
    type_tokens = list(types or [])
    token = _normalize_category(category, *type_tokens)
    if not token:
        return 0.5
    for key, value in AFFINITY_RULES.items():
        if key in token:
            return value
    return 0.5


def _detect_brand(name: Optional[str]) -> bool:
    if not name:
        return False
    lowered = name.lower()
    return any(keyword in lowered for keyword in CHAIN_KEYWORDS)


def estimate_is_chain(
    category: Optional[str],
    types: Optional[Sequence[str]],
    name: Optional[str],
    size_class: Optional[str],
    existing_hint: Optional[bool] = None,
) -> Optional[bool]:
    if existing_hint is not None:
        return existing_hint
    if _detect_brand(name):
        return True
    token = _normalize_category(category, *(types or []))
    if token in {"supermarket", "hypermarket", "shopping_centre"}:
        return True
    if size_class in {"media", "grande"} and token in {"gym", "fitness_centre", "department_store"}:
        return True
    return False


def estimate_marketing_attitude(
    has_website: bool,
    social: Mapping[str, str] | None,
    hours_weekly: Optional[int],
    brand_present: bool,
) -> Optional[float]:
    score = 0.25
    if has_website:
        score += 0.3
    social_count = len(social or {})
    if social_count:
        score += min(0.25, 0.12 * social_count)
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
    types: Optional[Sequence[str]],
    has_website: bool,
    has_phone: bool,
    hours_weekly: Optional[int],
    existing: Mapping[str, Any],
) -> Dict[str, Any]:
    result: Dict[str, Any] = {}

    existing_social = existing.get("social")
    social = existing_social if isinstance(existing_social, Mapping) else {}
    brand_present = _detect_brand(name)

    size_class = existing.get("size_class")
    size_class = size_class or estimate_size_class(category, types, existing.get("is_chain"))
    result["size_class"] = size_class

    is_chain = estimate_is_chain(category, types, name, size_class, existing.get("is_chain"))
    result["is_chain"] = is_chain

    budget = existing.get("ad_budget_band") or infer_budget_band(size_class, category)
    result["ad_budget_band"] = budget

    affinity = existing.get("umbrella_affinity")
    if affinity is None:
        affinity = default_affinity(category, types)
    result["umbrella_affinity"] = affinity

    marketing = existing.get("marketing_attitude")
    if marketing is None:
        marketing = estimate_marketing_attitude(
            has_website=has_website,
            social=social if social else None,
            hours_weekly=hours_weekly,
            brand_present=brand_present,
        )
    result["marketing_attitude"] = marketing

    confidence = existing.get("confidence")
    if confidence is None:
        confidence = estimate_confidence(
            has_website=has_website,
            has_phone=has_phone,
            social=social if social else None,
            brand_present=brand_present,
            marketing_attitude=marketing,
            size_class=size_class,
        )
    result["confidence"] = confidence

    if social:
        result["social"] = social

    return result
