from __future__ import annotations

import hashlib
import ipaddress
import json
import socket
from urllib.parse import urljoin, urlparse

import requests
from trafilatura.core import bare_extraction

from intel.llm.ollama_client import parse_json_response


USER_AGENT = "ClearfieldResearch/1.0 (+https://workhub-rso.ru/)"
MAX_DOWNLOAD_BYTES = 2_000_000
MAX_REDIRECTS = 4


def normalized_text(value: object) -> str:
    return " ".join(str(value or "").split())


def validate_public_source_url(url: str, allowed_domains: list[str]) -> str:
    parsed = urlparse(str(url or "").strip())
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("source URL must use http or https")
    if not parsed.hostname:
        raise ValueError("source URL has no hostname")
    if parsed.port not in (None, 80, 443):
        raise ValueError("source URL uses a forbidden port")
    if parsed.username or parsed.password:
        raise ValueError("source URL must not contain credentials")

    hostname = parsed.hostname.casefold().rstrip(".")
    allowed = [value.casefold().lstrip(".") for value in allowed_domains]
    if allowed and not any(
        hostname == domain or hostname.endswith("." + domain)
        for domain in allowed
    ):
        raise ValueError(f"source domain is not allowed: {hostname}")

    addresses = {
        item[4][0]
        for item in socket.getaddrinfo(
            hostname,
            parsed.port or (443 if parsed.scheme == "https" else 80),
            type=socket.SOCK_STREAM,
        )
    }
    if not addresses:
        raise ValueError("source hostname did not resolve")
    for address in addresses:
        if not ipaddress.ip_address(address).is_global:
            raise ValueError(f"source resolves to non-public address: {address}")

    return parsed.geturl()


def fetch_public_html(url: str, allowed_domains: list[str]) -> tuple[str, str]:
    current = validate_public_source_url(url, allowed_domains)
    session = requests.Session()

    for _ in range(MAX_REDIRECTS + 1):
        response = session.get(
            current,
            allow_redirects=False,
            timeout=(10, 30),
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "ru,en;q=0.7",
            },
            stream=True,
        )
        if response.status_code in {301, 302, 303, 307, 308}:
            location = response.headers.get("Location")
            if not location:
                raise ValueError("redirect has no Location")
            current = validate_public_source_url(
                urljoin(current, location),
                allowed_domains,
            )
            continue
        response.raise_for_status()

        content_type = response.headers.get("Content-Type", "").casefold()
        if "html" not in content_type:
            raise ValueError(f"unsupported Content-Type: {content_type}")

        chunks = []
        total = 0
        for chunk in response.iter_content(65536):
            total += len(chunk)
            if total > MAX_DOWNLOAD_BYTES:
                raise ValueError("source document is too large")
            chunks.append(chunk)

        response.encoding = response.encoding or "utf-8"
        html = b"".join(chunks).decode(response.encoding, errors="replace")
        return current, html

    raise ValueError("too many redirects")


def extract_research_document(url: str, html: str) -> dict:
    data = bare_extraction(html, url=url, favor_precision=True)
    if not data:
        raise ValueError("article extraction returned no content")

    def field(name: str) -> str:
        value = data.get(name) if isinstance(data, dict) else getattr(data, name, "")
        return normalized_text(value)

    text = field("text")
    if len(text) < 300:
        raise ValueError("extracted source text is too short")
    return {
        "url": url,
        "title": field("title")[:500],
        "text": text,
        "sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
    }


def build_claim_extraction_prompt(brief, source, document: dict) -> str:
    return f"""
Извлеки из документа факты, которые непосредственно помогают раскрыть тему.
Тема: {brief.title}
Главный запрос: {brief.primary_keyword}
Поисковое намерение: {brief.search_intent}

Правила:
- Не используй знания вне документа.
- Для каждого тезиса верни короткую дословную цитату из документа.
- Цитата должна встречаться в документе без изменений.
- Не извлекай рекламные обещания, навигацию и сведения вне темы.
- Максимум 6 самостоятельных тезисов.

Документ «{source.name}»:
{document["text"][:16000]}

Верни строго JSON:
{{
  "claims": [
    {{
      "claim": "аккуратный пересказ факта для редактора",
      "source_quote": "дословная цитата из документа"
    }}
  ]
}}
""".strip()


def verified_claims_from_response(text: str, document_text: str) -> list[dict]:
    data = parse_json_response(text)
    claims = data.get("claims")
    if not isinstance(claims, list):
        return []

    normalized_document = normalized_text(document_text).casefold()
    accepted = []
    for item in claims[:10]:
        if not isinstance(item, dict):
            continue
        claim = normalized_text(item.get("claim"))
        quote = normalized_text(item.get("source_quote"))
        if len(claim) < 40 or len(quote) < 40:
            continue
        if quote.casefold() not in normalized_document:
            continue
        accepted.append({"claim": claim, "source_quote": quote})
    return accepted
