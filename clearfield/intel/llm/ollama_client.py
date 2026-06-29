import json
import logging
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any

from django.conf import settings


logger = logging.getLogger("intel.llm")


class OllamaError(Exception):
    pass


@dataclass
class OllamaResult:
    text: str
    model: str
    elapsed_ms: int
    raw: dict[str, Any]


def _get_setting(name: str, default=None):
    return getattr(settings, name, default)


def generate_with_ollama(
    prompt: str,
    system: str = "",
    json_mode: bool = False,
    model: str | None = None,
) -> OllamaResult:
    """
    Синхронный клиент Ollama через стандартную библиотеку Python.

    Не требует requests.
    Работает с /api/generate.
    """

    if not _get_setting("LLM_ENABLED", False):
        raise OllamaError("LLM is disabled. Set LLM_ENABLED=True in .env")

    base_url = _get_setting("OLLAMA_BASE_URL", "http://127.0.0.1:11434").rstrip("/")
    selected_model = model or _get_setting("OLLAMA_MODEL", "qwen2.5:7b")

    url = f"{base_url}/api/generate"

    payload: dict[str, Any] = {
        "model": selected_model,
        "prompt": prompt,
        "system": system,
        "stream": False,
        "options": {
            "temperature": _get_setting("OLLAMA_TEMPERATURE", 0.3),
            "top_p": _get_setting("OLLAMA_TOP_P", 0.9),
            "num_predict": _get_setting("OLLAMA_NUM_PREDICT", 2500),
        },
    }

    if json_mode:
        payload["format"] = "json"

    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    request = urllib.request.Request(
        url=url,
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json",
        },
    )

    started = time.perf_counter()

    try:
        with urllib.request.urlopen(
            request,
            timeout=_get_setting("OLLAMA_TIMEOUT", 240),
        ) as response:
            response_body = response.read().decode("utf-8", errors="replace")
            status_code = response.status

    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        logger.exception("Ollama HTTP error: %s", error_body)
        raise OllamaError(f"Ollama HTTP error {exc.code}: {error_body}") from exc

    except urllib.error.URLError as exc:
        logger.exception("Ollama connection error")
        raise OllamaError(f"Ollama connection error: {exc}") from exc

    except TimeoutError as exc:
        logger.exception("Ollama timeout")
        raise OllamaError("Ollama request timeout") from exc

    elapsed_ms = int((time.perf_counter() - started) * 1000)

    if status_code < 200 or status_code >= 300:
        raise OllamaError(f"Ollama returned HTTP {status_code}: {response_body}")

    try:
        raw = json.loads(response_body)
    except json.JSONDecodeError as exc:
        raise OllamaError(f"Ollama returned invalid JSON: {response_body[:500]}") from exc

    text = (raw.get("response") or "").strip()

    if not text:
        raise OllamaError("Ollama returned empty response")

    return OllamaResult(
        text=text,
        model=selected_model,
        elapsed_ms=elapsed_ms,
        raw=raw,
    )


def parse_json_response(text: str) -> dict[str, Any]:
    """
    Достаёт JSON даже если модель обернула ответ в ```json ... ```.
    """

    cleaned = text.strip()

    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`").strip()
        if cleaned.lower().startswith("json"):
            cleaned = cleaned[4:].strip()

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    start = cleaned.find("{")
    end = cleaned.rfind("}")

    if start == -1 or end == -1 or end <= start:
        raise OllamaError("Could not find JSON object in LLM response")

    fragment = cleaned[start:end + 1]

    try:
        return json.loads(fragment)
    except json.JSONDecodeError as exc:
        raise OllamaError(f"Could not parse JSON from LLM response: {exc}") from exc
