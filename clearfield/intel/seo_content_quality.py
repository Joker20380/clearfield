"""Deterministic SEO/editorial checks for generated long-form content."""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class SeoQualityResult:
    score: int
    issues: tuple[str, ...]


def _words(value: object) -> list[str]:
    return re.findall(
        r"[0-9a-zа-яё]+",
        str(value or "").casefold(),
    )


def assess_seo_content(
    *,
    title: object,
    meta_description: object,
    body: object,
    target_keyword: object,
    source_urls: object,
    evergreen: bool,
) -> SeoQualityResult:
    """Score only measurable properties; never pretend to verify expertise."""
    title_text = " ".join(str(title or "").split())
    meta_text = " ".join(str(meta_description or "").split())
    body_text = str(body or "").strip()
    keyword = " ".join(str(target_keyword or "").casefold().split())
    source_text = str(source_urls or "").strip()
    issues: list[str] = []
    score = 100

    if not 30 <= len(title_text) <= 110:
        score -= 10
        issues.append("title-length")

    if not 100 <= len(meta_text) <= 240:
        score -= 10
        issues.append("meta-length")

    minimum_chars = 1800 if evergreen else 700
    if len(body_text) < minimum_chars:
        score -= 20
        issues.append("body-too-short")

    headings = re.findall(r"(?m)^##\s+\S.+$", body_text)
    minimum_headings = 3 if evergreen else 2
    if len(headings) < minimum_headings:
        score -= 12
        issues.append("too-few-sections")

    if re.search(r"(?m)^#\s+", body_text):
        score -= 8
        issues.append("h1-in-body")

    if keyword:
        if keyword not in title_text.casefold():
            score -= 8
            issues.append("keyword-not-in-title")
        if keyword not in body_text.casefold():
            score -= 8
            issues.append("keyword-not-in-body")

        keyword_words = _words(keyword)
        body_words = _words(body_text)
        if keyword_words and body_words:
            phrase_hits = body_text.casefold().count(keyword)
            density = phrase_hits * len(keyword_words) / len(body_words)
            if density > 0.04:
                score -= 15
                issues.append("keyword-stuffing")

    if not source_text:
        score -= 12
        issues.append("no-sources")

    if re.search(
        r"(?i)\b(гарантированно|сто процентов|лучший|самый деш[её]вый)\b",
        f"{title_text}\n{meta_text}\n{body_text}",
    ):
        score -= 15
        issues.append("promotional-overclaim")

    return SeoQualityResult(
        score=max(0, min(score, 100)),
        issues=tuple(issues),
    )
