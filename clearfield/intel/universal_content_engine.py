from __future__ import annotations

import json
import re
from urllib.parse import urlparse

from django.utils.text import slugify

from intel.llm.ollama_client import parse_json_response
from intel.seo_content_quality import assess_seo_content


EVIDENCE_ID_RE = re.compile(r"^[A-Z][A-Z0-9_-]{0,15}$")


def validate_evidence_pack(brief) -> list[str]:
    errors: list[str] = []
    pack = brief.evidence_pack if isinstance(brief.evidence_pack, list) else []
    policy = brief.project.policy if isinstance(brief.project.policy, dict) else {}
    min_claims = max(int(policy.get("min_evidence_claims", 2)), 1)
    min_sources = max(int(policy.get("min_source_domains", 1)), 1)
    min_evidence_chars = max(
        int(
            policy.get(
                "min_evidence_chars",
                max(250, int(brief.template.min_chars * 0.3)),
            )
        ),
        100,
    )
    seen_ids: set[str] = set()
    domains: set[str] = set()
    total_evidence_chars = 0

    if len(pack) < min_claims:
        errors.append(f"not-enough-evidence:{len(pack)}<{min_claims}")

    for position, item in enumerate(pack, start=1):
        if not isinstance(item, dict):
            errors.append(f"evidence-{position}-not-object")
            continue

        evidence_id = str(item.get("id") or "").strip().upper()
        claim = " ".join(str(item.get("claim") or "").split())
        source_url = str(item.get("source_url") or "").strip()
        parsed = urlparse(source_url)

        if not EVIDENCE_ID_RE.fullmatch(evidence_id):
            errors.append(f"evidence-{position}-bad-id")
        elif evidence_id in seen_ids:
            errors.append(f"duplicate-evidence-id:{evidence_id}")
        else:
            seen_ids.add(evidence_id)

        if len(claim) < 40:
            errors.append(f"evidence-{evidence_id or position}-short-claim")
        total_evidence_chars += len(claim)

        if policy.get("require_source_quotes", False):
            source_quote = " ".join(
                str(item.get("source_quote") or "").split()
            )
            total_evidence_chars += len(source_quote)
            source_sha256 = str(item.get("source_sha256") or "")
            if len(source_quote) < 40:
                errors.append(
                    f"evidence-{evidence_id or position}-missing-source-quote"
                )
            if not re.fullmatch(r"[0-9a-f]{64}", source_sha256):
                errors.append(
                    f"evidence-{evidence_id or position}-missing-source-sha256"
                )

        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            errors.append(f"evidence-{evidence_id or position}-bad-url")
        else:
            domains.add(parsed.netloc.casefold())

    if len(domains) < min_sources:
        errors.append(f"not-enough-source-domains:{len(domains)}<{min_sources}")
    if total_evidence_chars < min_evidence_chars:
        errors.append(
            "evidence-pack-too-thin:"
            f"{total_evidence_chars}<{min_evidence_chars}"
        )

    return list(dict.fromkeys(errors))


def build_universal_prompt(brief) -> tuple[str, str]:
    project = brief.project
    template = brief.template
    policy = project.policy if isinstance(project.policy, dict) else {}
    forbidden = policy.get("forbidden_claims") or []
    evidence_lines = []

    for item in brief.evidence_pack:
        evidence_lines.append(
            f"[{str(item['id']).upper()}] {item['claim']}\n"
            f"Источник: {item['source_url']}"
        )

    links = brief.internal_links if isinstance(brief.internal_links, list) else []
    link_lines = [
        f"- {item.get('anchor', '')}: {item.get('url', '')}"
        for item in links
        if isinstance(item, dict)
    ]
    secondary = brief.secondary_keywords
    if not isinstance(secondary, list):
        secondary = []

    system = f"""
Ты редактор проекта «{project.name}» в тематике «{project.niche}».
Пиши для аудитории: {project.audience}.
Создавай полезный материал для человека, а не текст для манипуляции поиском.
Используй только факты из пронумерованного evidence pack.
Не создавай цитаты, экспертов, опыт использования, исследования и статистику.
Каждый фактический абзац заканчивай маркерами использованных доказательств:
[E1] или [E1][E2]. Не используй несуществующие маркеры.
Не вставляй URL: список источников добавит система.
Не заявляй об экспертной проверке.
{project.system_prompt}
""".strip()

    prompt = f"""
Тип материала: {template.content_type}
Рабочий заголовок: {brief.title}
Главный поисковый запрос: {brief.primary_keyword}
Дополнительные запросы: {", ".join(map(str, secondary)) or "нет"}
Поисковое намерение: {brief.search_intent}
Бренд: {project.brand_name or "не задан"}
Сайт: {project.site_url or "не задан"}

Evidence pack:
{chr(10).join(evidence_lines)}

Разрешённые внутренние ссылки:
{chr(10).join(link_lines) or "нет"}

Требования шаблона:
{template.instructions}

Дополнительные инструкции:
{brief.instructions or "нет"}

Ограничения:
- Объём от {template.min_chars} до {template.max_chars} символов.
- Не менее {template.min_sections} разделов с заголовками ##.
- Сразу ответь на основной запрос в первом абзаце.
- Используй ключевые фразы естественно, без повторов ради SEO.
- Не используй H1 внутри body_markdown.
- Не добавляй неподтверждённые причины, последствия и рекомендации.
- Запрещённые утверждения профиля: {json.dumps(forbidden, ensure_ascii=False)}.

Верни строго JSON:
{{
  "title": "полезный заголовок",
  "slug": "latin-slug",
  "meta_description": "информативное описание",
  "body_markdown": "текст с evidence-маркерами",
  "used_evidence_ids": ["E1", "E2"]
}}
""".strip()

    return system, prompt


def parse_and_audit_generated_content(brief, text: str) -> dict:
    data = parse_json_response(text)
    title = " ".join(str(data.get("title") or "").split())
    meta = " ".join(str(data.get("meta_description") or "").split())
    body = str(data.get("body_markdown") or "").strip()
    used_ids = [
        str(value).strip().upper()
        for value in (data.get("used_evidence_ids") or [])
        if str(value).strip()
    ]
    used_ids = list(dict.fromkeys(used_ids))
    available = {
        str(item.get("id") or "").strip().upper(): item
        for item in brief.evidence_pack
        if isinstance(item, dict)
    }
    issues: list[str] = []

    if not title:
        issues.append("missing-title")
    if not meta:
        issues.append("missing-meta")
    if not body:
        issues.append("missing-body")
    if not used_ids:
        issues.append("no-used-evidence")

    unknown = sorted(set(used_ids) - set(available))
    if unknown:
        issues.append("unknown-evidence:" + ",".join(unknown))

    missing_markers = [
        evidence_id
        for evidence_id in used_ids
        if f"[{evidence_id}]" not in body
    ]
    if missing_markers:
        issues.append("missing-evidence-markers:" + ",".join(missing_markers))

    source_urls = [
        available[evidence_id]["source_url"]
        for evidence_id in used_ids
        if evidence_id in available
    ]
    qa = assess_seo_content(
        title=title,
        meta_description=meta,
        body=body,
        target_keyword=brief.primary_keyword,
        source_urls="\n".join(source_urls),
        evergreen=brief.template.content_type == "evergreen_article",
    )
    issues.extend(qa.issues)

    appendix = ["## Источники и основания", ""]
    for evidence_id in used_ids:
        if evidence_id not in available:
            continue
        item = available[evidence_id]
        label = " ".join(
            str(item.get("source_title") or f"Источник {evidence_id}").split()
        )
        appendix.append(f"- [{evidence_id}] [{label}]({item['source_url']})")

    if len(appendix) > 2:
        body = f"{body}\n\n" + "\n".join(appendix)

    internal_links = (
        brief.internal_links
        if isinstance(brief.internal_links, list)
        else []
    )
    link_lines = []
    for item in internal_links:
        if not isinstance(item, dict):
            continue
        url = str(item.get("url") or "").strip()
        anchor = " ".join(str(item.get("anchor") or "").split())
        parsed = urlparse(url)
        if (
            anchor
            and parsed.scheme in {"http", "https"}
            and parsed.netloc
        ):
            link_lines.append(f"- [{anchor}]({url})")

    if link_lines:
        body = (
            f"{body}\n\n## Полезные ссылки\n\n"
            + "\n".join(link_lines)
        )

    raw_slug = str(data.get("slug") or "")
    safe_slug = slugify(raw_slug)[:255] or f"content-{brief.pk}"
    hard_issues = [
        issue
        for issue in issues
        if issue.startswith(
            (
                "missing-",
                "unknown-evidence",
                "no-used-evidence",
            )
        )
    ]

    return {
        "title": title,
        "slug": safe_slug,
        "meta_description": meta[:320],
        "body": body,
        "used_evidence_ids": used_ids,
        "source_urls": list(dict.fromkeys(source_urls)),
        "quality_score": max(0, qa.score - 10 * len(hard_issues)),
        "qa_report": {
            "issues": list(dict.fromkeys(issues)),
            "hard_issues": hard_issues,
            "expert_review_required": brief.template.expert_review_required,
            "evidence_count": len(used_ids),
        },
    }


def build_factual_verification_prompt(brief, generated_body: str) -> str:
    evidence = "\n".join(
        (
            f"[{str(item.get('id') or '').upper()}] "
            f"{item.get('claim')}"
        )
        for item in brief.evidence_pack
        if isinstance(item, dict)
    )
    return f"""
Ты строгий факт-чекер. Сравни текст только с evidence pack.
Считай неподтверждённым любое конкретное техническое, медицинское,
юридическое, финансовое или причинно-следственное утверждение,
которое не следует непосредственно из evidence pack.
Не считай evidence-маркеры доказательством сами по себе.
Общие связующие и редакционные фразы допустимы, если они не добавляют факт.
При сомнении считай утверждение неподтверждённым.

Evidence pack:
{evidence}

Проверяемый текст:
{generated_body}

Верни строго JSON:
{{
  "supported": true,
  "unsupported_claims": [
    {{"quote": "точный короткий фрагмент", "reason": "почему не подтверждён"}}
  ]
}}
Если найден хотя бы один неподтверждённый тезис, supported должно быть false.
""".strip()


def parse_factual_verification(text: str) -> dict:
    data = parse_json_response(text)
    unsupported = data.get("unsupported_claims")
    if not isinstance(unsupported, list):
        unsupported = []

    cleaned = []
    for item in unsupported[:20]:
        if not isinstance(item, dict):
            continue
        quote = " ".join(str(item.get("quote") or "").split())[:500]
        reason = " ".join(str(item.get("reason") or "").split())[:500]
        if quote:
            cleaned.append({"quote": quote, "reason": reason})

    supported = data.get("supported") is True and not cleaned
    return {
        "supported": supported,
        "unsupported_claims": cleaned,
    }
