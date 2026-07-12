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
    Синхронный клиент OpenAI-compatible API llama.cpp.

    Имя функции и тип результата сохранены для совместимости
    с существующим генератором медицинских новостей.
    """

    if not _get_setting("LLM_ENABLED", False):
        raise OllamaError("LLM is disabled. Set LLM_ENABLED=True in .env")

    base_url = _get_setting(
        "OLLAMA_BASE_URL",
        "http://127.0.0.1:18081",
    ).rstrip("/")

    selected_model = model or _get_setting("OLLAMA_MODEL", "")

    if not selected_model:
        raise OllamaError("OLLAMA_MODEL is empty")

    url = f"{base_url}/v1/chat/completions"

    messages: list[dict[str, str]] = []

    if system:
        messages.append({
            "role": "system",
            "content": system,
        })

    messages.append({
        "role": "user",
        "content": prompt,
    })

    payload: dict[str, Any] = {
        "model": selected_model,
        "messages": messages,
        "stream": False,
        "temperature": float(
            _get_setting("OLLAMA_TEMPERATURE", 0.3)
        ),
        "top_p": float(
            _get_setting("OLLAMA_TOP_P", 0.9)
        ),
        "max_tokens": int(
            _get_setting("OLLAMA_NUM_PREDICT", 2500)
        ),
    }

    if json_mode:
        payload["response_format"] = {
            "type": "json_object",
        }

    data = json.dumps(
        payload,
        ensure_ascii=False,
    ).encode("utf-8")

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
            timeout=int(_get_setting("OLLAMA_TIMEOUT", 600)),
        ) as response:
            response_body = response.read().decode(
                "utf-8",
                errors="replace",
            )
            status_code = response.status

    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode(
            "utf-8",
            errors="replace",
        )
        logger.exception(
            "LLM HTTP error: %s",
            error_body[:2000],
        )
        raise OllamaError(
            f"LLM HTTP error {exc.code}: {error_body[:2000]}"
        ) from exc

    except urllib.error.URLError as exc:
        logger.exception("LLM connection error")
        raise OllamaError(
            f"LLM connection error: {exc}"
        ) from exc

    except TimeoutError as exc:
        logger.exception("LLM request timeout")
        raise OllamaError(
            "LLM request timeout"
        ) from exc

    elapsed_ms = int(
        (time.perf_counter() - started) * 1000
    )

    if status_code < 200 or status_code >= 300:
        raise OllamaError(
            f"LLM returned HTTP {status_code}: "
            f"{response_body[:2000]}"
        )

    try:
        raw = json.loads(response_body)
    except json.JSONDecodeError as exc:
        raise OllamaError(
            "LLM returned invalid JSON: "
            f"{response_body[:1000]}"
        ) from exc

    choices = raw.get("choices") or []

    if not choices:
        raise OllamaError(
            f"LLM returned no choices: {response_body[:1000]}"
        )

    message = choices[0].get("message") or {}
    content = message.get("content")

    if isinstance(content, list):
        text_parts = []

        for part in content:
            if isinstance(part, dict):
                value = part.get("text")
                if value:
                    text_parts.append(str(value))
            elif part:
                text_parts.append(str(part))

        generated_text = "".join(text_parts).strip()
    else:
        generated_text = str(content or "").strip()

    if not generated_text:
        raise OllamaError(
            f"LLM returned empty content: {response_body[:1000]}"
        )

    returned_model = str(
        raw.get("model") or selected_model
    )

    return OllamaResult(
        text=generated_text,
        model=returned_model,
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
