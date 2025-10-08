import abc
import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Optional

import requests

logger = logging.getLogger(__name__)


class LLMError(RuntimeError):
    """Raised when an LLM provider call fails."""


@dataclass
class CompletionResult:
    text: str
    model: Optional[str]
    prompt_tokens: Optional[int]
    completion_tokens: Optional[int]
    cost_cents: Optional[float]
    raw: dict[str, Any]


class LLMClient(abc.ABC):
    """Abstract base class for chat-based LLM clients."""

    @abc.abstractmethod
    def complete(self, *, prompt: str, temperature: float = 0.2, max_tokens: int = 600) -> CompletionResult:
        """Execute a completion request and return the assistant message content."""


class OpenAIChatClient(LLMClient):
    """Thin wrapper around the OpenAI chat completions API."""

    def __init__(
        self,
        api_key: str,
        model: str = "gpt-4o-mini",
        endpoint: str = "https://api.openai.com/v1/chat/completions",
        system_prompt: str | None = None,
        timeout: int = 60,
    ) -> None:
        if not api_key:
            raise ValueError("OpenAI API key is required")
        self.api_key = api_key
        self.model = model
        self.endpoint = endpoint
        self.system_prompt = system_prompt or (
            "Sei un analista marketing locale. Rispondi SEMPRE e SOLO in JSON valido."
        )
        self.timeout = timeout

    def complete(self, *, prompt: str, temperature: float = 0.2, max_tokens: int = 600) -> CompletionResult:
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": self.system_prompt},
                {"role": "user", "content": prompt},
            ],
            "temperature": max(0.0, min(2.0, temperature)),
            "max_tokens": max_tokens,
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        try:
            resp = requests.post(
                self.endpoint,
                headers=headers,
                data=json.dumps(payload),
                timeout=self.timeout,
            )
        except requests.RequestException as exc:
            raise LLMError(f"OpenAI request failed: {exc}") from exc

        if resp.status_code >= 400:
            raise LLMError(f"OpenAI API error {resp.status_code}: {resp.text}")

        body = resp.json()
        try:
            message = body["choices"][0]["message"]["content"]
        except (KeyError, IndexError) as exc:
            raise LLMError(f"Malformed OpenAI response: {body}") from exc

        usage = body.get("usage") or {}
        return CompletionResult(
            text=message,
            model=body.get("model"),
            prompt_tokens=usage.get("prompt_tokens"),
            completion_tokens=usage.get("completion_tokens"),
            cost_cents=None,
            raw=body,
        )


class PerplexityClient(LLMClient):
    """Wrapper around the Perplexity chat completions endpoint."""

    def __init__(
        self,
        api_key: str,
        model: str = "sonar",
        endpoint: str = "https://api.perplexity.ai/chat/completions",
        system_prompt: str | None = None,
        timeout: int = 60,
    ) -> None:
        if not api_key:
            raise ValueError("Perplexity API key is required")
        self.api_key = api_key
        self.model = model
        self.endpoint = endpoint
        self.system_prompt = system_prompt or (
            "Sei un analista marketing locale. Rispondi SEMPRE e SOLO in JSON valido."
        )
        self.timeout = timeout

    def complete(self, *, prompt: str, temperature: float = 0.1, max_tokens: int = 800) -> CompletionResult:
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": self.system_prompt},
                {"role": "user", "content": prompt},
            ],
            "temperature": max(0.0, min(2.0, temperature)),
            "max_tokens": max_tokens,
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        try:
            resp = requests.post(
                self.endpoint,
                headers=headers,
                data=json.dumps(payload),
                timeout=self.timeout,
            )
        except requests.RequestException as exc:
            raise LLMError(f"Perplexity request failed: {exc}") from exc

        if resp.status_code >= 400:
            raise LLMError(f"Perplexity API error {resp.status_code}: {resp.text}")

        body = resp.json()
        try:
            message = body["choices"][0]["message"]["content"]
        except (KeyError, IndexError) as exc:
            raise LLMError(f"Malformed Perplexity response: {body}") from exc

        usage = body.get("usage") or {}
        return CompletionResult(
            text=message,
            model=body.get("model"),
            prompt_tokens=usage.get("prompt_tokens"),
            completion_tokens=usage.get("completion_tokens"),
            cost_cents=None,
            raw=body,
        )


def load_client_from_env(logger_: logging.Logger | None = None) -> LLMClient | None:
    """Instantiate a client based on environment variables.

    Expected variables:
        - LLM_PROVIDER: 'openai' | 'perplexity'
        - OPENAI_API_KEY / PERPLEXITY_API_KEY
        - LLM_MODEL (optional override)
    """
    log = logger_ or logger
    provider = (os.getenv("LLM_PROVIDER") or "").strip().lower()
    if not provider:
        log.warning("LLM_PROVIDER not set; enrichment will run in dry-run mode")
        return None

    if provider == "openai":
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OPENAI_API_KEY is required when LLM_PROVIDER=openai")
        model = os.getenv("LLM_MODEL") or "gpt-4o-mini"
        return OpenAIChatClient(api_key=api_key, model=model)

    if provider in {"perplexity", "px"}:
        api_key = os.getenv("PERPLEXITY_API_KEY")
        if not api_key:
            raise ValueError("PERPLEXITY_API_KEY is required when LLM_PROVIDER=perplexity")
        model = os.getenv("LLM_MODEL") or "sonar"
        return PerplexityClient(api_key=api_key, model=model)

    raise ValueError(f"Unsupported LLM_PROVIDER '{provider}'")
