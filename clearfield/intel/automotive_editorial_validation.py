from __future__ import annotations

import re
from typing import Iterable
from urllib.parse import urlsplit

from django.utils.text import slugify

from intel.models import AutomotiveBrief


IMAGE_TOPICS = (
    "diagnostics",
    "engine",
    "auto_electrics",
    "suspension",
    "brakes",
    "tires",
    "transmission",
    "maintenance",
    "safety",
    "legislation",
    "automotive_market",
    "general_automotive_news",
)

DEFAULT_IMAGE_TOPIC = "general_automotive_news"

DEFAULT_DISCLAIMER = (
    "Материал носит информационный характер. "
    "Точную причину неисправности и необходимость "
    "ремонта следует определять после диагностики "
    "автомобиля специалистом."
)

LOCAL_GEOGRAPHY_TERMS = (
    "владикавказ",
    "северная осетия",
    "рсо-алания",
    "рсо алания",
)

BAD_PATTERNS = (
    r"\bas\s+an\s+ai\b",
    r"я\s+являюсь\s+искусственным\s+интеллектом",
    r"я\s+не\s+могу\s+дать",
    r"ollama",
    r"```",
    r"<\s*script",
    r"<\s*iframe",
    r"можно\s+безопасно\s+продолжать\s+движение",
    r"безопасно\s+продолжать\s+движение",
    r"диагностика\s+не\s+нужна",
    r"ремонт\s+точно\s+не\s+требуется",
    r"точно\s+требуется\s+замена",
    r"однозначно\s+необходимо\s+заменить",
    r"код\s+ошибки\s+точно\s+означает",
    r"неисправность\s+точно\s+связана",
    r"гарантированно\s+устран",
)


def normalize_text(value: object) -> str:
    text = str(value or "").lower()
    text = text.replace("ё", "е")

    text = re.sub(
        r"[‐-‒–—−]",
        "-",
        text,
    )

    text = re.sub(
        r"\s+",
        " ",
        text,
    )

    return text.strip()


def split_source_urls(value: object) -> list[str]:
    if isinstance(value, (list, tuple, set)):
        raw_values: Iterable[object] = value
    else:
        raw_values = str(value or "").splitlines()

    result: list[str] = []
    seen: set[str] = set()

    for raw in raw_values:
        url = str(raw or "").strip()

        if not url:
            continue

        key = url.rstrip("/")

        if key in seen:
            continue

        seen.add(key)
        result.append(url)

    return result


def is_valid_http_url(value: object) -> bool:
    try:
        parsed = urlsplit(
            str(value or "").strip()
        )
    except ValueError:
        return False

    return (
        parsed.scheme in {
            "http",
            "https",
        }
        and bool(parsed.netloc)
    )


def normalize_source_urls(value: object) -> list[str]:
    return [
        url
        for url in split_source_urls(value)
        if is_valid_http_url(url)
    ]


def normalize_image_topic(value: object) -> str:
    topic = normalize_text(value).replace(
        "-",
        "_",
    ).replace(
        " ",
        "_",
    )

    if topic not in IMAGE_TOPICS:
        return DEFAULT_IMAGE_TOPIC

    return topic


def normalize_quality_score(value: object) -> int:
    try:
        score = int(value)
    except (TypeError, ValueError):
        return 0

    return min(
        max(score, 0),
        100,
    )


def make_automotive_slug(
    value: object,
    fallback_id: int,
) -> str:
    slug = slugify(
        str(value or ""),
        allow_unicode=False,
    )

    if not slug:
        slug = (
            f"automotive-news-{fallback_id}"
        )

    return slug[:255]


def get_automotive_disclaimer() -> str:
    return DEFAULT_DISCLAIMER


def apply_automotive_disclaimer(
    body: object,
) -> str:
    cleaned = str(body or "").strip()

    cleaned = re.sub(
        re.escape(DEFAULT_DISCLAIMER),
        "",
        cleaned,
        flags=re.I,
    )

    cleaned = re.sub(
        r"[ \t]+\n",
        "\n",
        cleaned,
    )

    cleaned = re.sub(
        r"\n{3,}",
        "\n\n",
        cleaned,
    ).strip()

    if not cleaned:
        return DEFAULT_DISCLAIMER

    return (
        f"{cleaned}\n\n"
        f"{DEFAULT_DISCLAIMER}"
    )


# =============================================================================
# AUTOMOTIVE CONTENT MODES
# =============================================================================


def strip_automotive_disclaimer(
    body: object,
) -> str:
    cleaned = str(body or "")

    cleaned = re.sub(
        re.escape(DEFAULT_DISCLAIMER),
        "",
        cleaned,
        flags=re.I,
    )

    cleaned = re.sub(
        r"[ \t]+\n",
        "\n",
        cleaned,
    )

    cleaned = re.sub(
        r"\n{3,}",
        "\n\n",
        cleaned,
    )

    return cleaned.strip()


UNSUPPORTED_MARKET_MARKERS = {
    "unconfirmed-cause": (
        "может быть связано",
        "может быть обусловлено",
        "связано с рядом факторов",
        "обусловлено рядом факторов",
        "включая изменения в экономической",
        "колебания в ценах",
        "пересмотр бизнес-планов",
    ),
    "unconfirmed-consequence": (
        "может повлиять на доступность",
        "может повлиять на сроки",
        "может повлиять на стоимость",
        "в перспективе стоимость",
        "приведет к росту цен",
        "приведёт к росту цен",
    ),
    "generic-market-advice": (
        "важно отслеживать изменения на рынке",
        "принимать обоснованные решения",
        "особенно актуально для тех",
    ),
    "unconfirmed-data-scope": (
        "не включают в себя",
        "не включает в себя",
        "не учитывает",
        "не учитывают",
        "исключены из статистики",
        "не вошли в статистику",
    ),
    "unconfirmed-product-positioning": (
        "первый шаг бренда",
        "для массового рынка",
        "принадлежит к классу",
        "часть стратегии развития бренда",
        "позиционируется как",
        "ориентирована на",
        "ориентирован на",
    ),
    "unconfirmed-presentation-details": (
        "официальной презентации",
        "официальная презентация",
        "организованной пресс-службой",
        "в рамках презентации",
    ),
    "unconfirmed-corporate-intent": (
        "подтверждает намерение компании",
        "подтверждает планы компании",
        "развивать отечественное автопроизводство",
        "использовать локальные производственные мощности",
        "укрепить позиции бренда",
    ),
    "unconfirmed-future-disclosure": (
        "ожидается, что полная информация",
        "будет представлена в ближайшее время",
        "станет известна в ближайшее время",
        "подробности появятся в ближайшее время",
    ),
    "unconfirmed-absence-claim": (
        "нет данных о планах по экспорту",
        "нет информации о планах по экспорту",
        "нет данных о доступности модели",
        "не раскрываются сроки начала серийного выпуска",
        "планы по экспорту не раскрываются",
    ),
}


UNSUPPORTED_MARKET_MARKERS.update(
    {
        "unconfirmed-event-framing": (
            "впервые официально представил",
            "официально представил",
            "событие прошло",
            "в рамках анонса",
            "в рамках официального анонса",
        ),
        "unconfirmed-localization-inference": (
            "подтверждает реализацию планов",
            "подтверждает планы по локализации",
            "реализация планов по локализации",
            "подтверждает локализацию сборки",
        ),
        "unconfirmed-deadline-framing": (
            "в установленные сроки",
            "сроки пока не уточнены",
            "которые пока не уточнены",
            "точные сроки пока не называются",
        ),
        "unconfirmed-missing-details-list": (
            "информация о модели не содержит сведений",
            "экспортных планах",
            "доступности на других рынках",
            "доступности модели на других рынках",
            "сроках начала продаж",
        ),
    }
)


NON_SERVICE_DRIFT_TERMS = (
    "автосервис",
    "диагностик",
    "неисправност",
    "ремонт транспортных средств",
)


def unsupported_market_claim_errors(
    brief: AutomotiveBrief,
    text: object,
) -> list[str]:
    generated = normalize_text(
        strip_automotive_disclaimer(
            text
        )
    )

    confirmed = confirmed_source_text(
        brief
    )

    errors: list[str] = []

    for code, markers in (
        UNSUPPORTED_MARKET_MARKERS.items()
    ):
        hits = [
            marker
            for marker in markers
            if (
                normalize_text(marker)
                in generated
                and normalize_text(marker)
                not in confirmed
            )
        ]

        if hits:
            errors.append(
                f"{code}:"
                + ",".join(hits[:3])
            )

    if not bool(
        getattr(
            brief,
            "disclaimer_required",
            True,
        )
    ):
        drift_hits = [
            term
            for term in NON_SERVICE_DRIFT_TERMS
            if (
                normalize_text(term)
                in generated
                and normalize_text(term)
                not in confirmed
            )
        ]

        if drift_hits:
            errors.append(
                "non-service-topic-drift:"
                + ",".join(
                    drift_hits[:3]
                )
            )

    return errors


def confirmed_source_text(
    brief: AutomotiveBrief,
) -> str:
    event = getattr(
        brief,
        "event",
        None,
    )

    return normalize_text(
        " ".join(
            [
                brief.title or "",
                brief.facts or "",
                (
                    event.title
                    if event
                    else ""
                ),
                (
                    event.summary
                    if event
                    else ""
                ),
            ]
        )
    )


def contains_unconfirmed_local_geography(
    brief: AutomotiveBrief,
    text: object,
) -> bool:
    generated = normalize_text(text)
    confirmed = confirmed_source_text(brief)

    for term in LOCAL_GEOGRAPHY_TERMS:
        normalized_term = normalize_text(term)

        if (
            normalized_term in generated
            and normalized_term not in confirmed
        ):
            return True

    return False


def unsafe_pattern_hits(
    text: object,
) -> list[str]:
    normalized = normalize_text(text)
    hits: list[str] = []

    for pattern in BAD_PATTERNS:
        if re.search(
            pattern,
            normalized,
            flags=re.I | re.M,
        ):
            hits.append(pattern)

    return hits


def minimum_body_chars_for_brief(
    brief: AutomotiveBrief,
) -> int:
    """
    Service-oriented materials need more explanatory
    context. Ordinary automotive and market news may
    remain concise when the source contains few facts.
    """

    if bool(
        getattr(
            brief,
            "disclaimer_required",
            True,
        )
    ):
        return 1000

    return 700


def validate_automotive_news(
    *,
    brief: AutomotiveBrief,
    title: object,
    body: object,
    meta_description: object,
    image_topic: object,
    source_urls: object,
    min_body_chars: int = 1000,
) -> list[str]:
    errors: list[str] = []

    clean_title = str(
        title or ""
    ).strip()

    clean_body = str(
        body or ""
    ).strip()

    clean_meta = str(
        meta_description or ""
    ).strip()

    urls = split_source_urls(
        source_urls
    )

    if len(clean_title) < 20:
        errors.append(
            f"short-title:{len(clean_title)}"
        )

    if len(clean_title) > 300:
        errors.append(
            f"long-title:{len(clean_title)}"
        )

    if len(clean_body) < min_body_chars:
        errors.append(
            f"short-body:{len(clean_body)}"
        )

    if "## " not in clean_body:
        errors.append(
            "missing-markdown-sections"
        )

    if len(clean_meta) < 60:
        errors.append(
            f"short-meta:{len(clean_meta)}"
        )

    if len(clean_meta) > 320:
        errors.append(
            f"long-meta:{len(clean_meta)}"
        )

    normalized_topic = (
        normalize_image_topic(
            image_topic
        )
    )

    if (
        normalize_text(image_topic)
        .replace("-", "_")
        .replace(" ", "_")
        != normalized_topic
    ):
        errors.append(
            "invalid-image-topic"
        )

    if not urls:
        errors.append(
            "missing-source-urls"
        )
    elif any(
        not is_valid_http_url(url)
        for url in urls
    ):
        errors.append(
            "invalid-source-url"
        )

    if contains_unconfirmed_local_geography(
        brief,
        (
            f"{clean_title} "
            f"{clean_meta} "
            f"{clean_body}"
        ),
    ):
        errors.append(
            "unconfirmed-local-geography-in-seo"
        )

    hits = unsafe_pattern_hits(
        f"{clean_title}\n{clean_body}"
    )

    if hits:
        errors.append(
            "unsafe-pattern:"
            + ",".join(hits[:3])
        )

    errors.extend(
        unsupported_market_claim_errors(
            brief,
            clean_body,
        )
    )

    disclaimer_count = clean_body.count(
        DEFAULT_DISCLAIMER
    )

    disclaimer_required = bool(
        getattr(
            brief,
            "disclaimer_required",
            True,
        )
    )

    if (
        disclaimer_required
        and disclaimer_count != 1
    ):
        errors.append(
            "invalid-disclaimer-count"
        )

    if (
        not disclaimer_required
        and disclaimer_count != 0
    ):
        errors.append(
            "unexpected-disclaimer"
        )

    return errors
