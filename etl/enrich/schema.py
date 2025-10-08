import json
import re
from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, Field, HttpUrl, ValidationError


SizeClass = Literal["micro", "piccola", "media", "grande"]
BudgetBand = Literal["basso", "medio", "alto"]


class EnrichedFacts(BaseModel):
    size_class: Optional[SizeClass]
    is_chain: Optional[bool]
    website_url: Optional[HttpUrl]
    social: Optional[Dict[str, HttpUrl]]
    marketing_attitude: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    umbrella_affinity: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    ad_budget_band: Optional[BudgetBand]
    confidence: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    provenance: Optional[Dict[str, Any]]
    notes: Optional[str]

    class Config:
        extra = "ignore"


FENCE_RE = re.compile(r"^```(?:json)?\s*(.+?)\s*```$", re.DOTALL)


def _strip_code_fence(text: str) -> str:
    match = FENCE_RE.match(text.strip())
    return match.group(1) if match else text


def _maybe_fix_url(url: Any) -> Any:
    if not isinstance(url, str):
        return url
    value = url.strip()
    if not value:
        return None
    if not value.lower().startswith(("http://", "https://")):
        value = "https://" + value
    return value


def parse_enriched_facts(raw_text: str) -> EnrichedFacts:
    """Parse a JSON string returned by the LLM into EnrichedFacts."""
    candidate = _strip_code_fence(raw_text.strip())
    try:
        payload = json.loads(candidate)
    except json.JSONDecodeError as exc:
        raise ValueError(f"LLM response is not valid JSON: {exc}: {raw_text}") from exc

    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object, got {type(payload)}")

    if "website_url" in payload:
        payload["website_url"] = _maybe_fix_url(payload.get("website_url"))

    social = payload.get("social")
    if isinstance(social, dict):
        payload["social"] = {
            str(k): _maybe_fix_url(v) for k, v in social.items() if v is not None
        }

    try:
        facts = EnrichedFacts.model_validate(payload)
    except ValidationError as exc:
        raise ValueError(f"Response does not match schema: {exc}") from exc

    return facts
