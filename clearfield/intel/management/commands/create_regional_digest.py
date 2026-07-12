import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import timedelta
from difflib import SequenceMatcher
from typing import Any

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.db.models import Prefetch
from django.utils import timezone

from intel.models import (
    Article,
    Event,
    EventItem,
    RegionalDigest,
    RegionalDigestItem,
    RegionalDigestStatus,
)


SOURCE_CLASS_WEIGHTS = {
    "official": 1.00,
    "stats": 0.95,
    "agency": 0.85,
    "industry": 0.75,
    "commentary": 0.50,
}


REGION_ALIASES = {
    "north_ossetia": [
        "рсо-алания",
        "рсо алания",
        "северная осетия",
        "северной осетии",
        "северную осетию",
        "владикавказ",
        "алания",
    ],
    "russia": [
        "россия",
        "российская федерация",
        "российской федерации",
        "рф",
    ],
}


MEDICAL_CONTENT_FILTER_VERSION = 3


# Сильные признаки медицинской публикации.
# Считаются группами, а не количеством повторений одного слова.
MEDICAL_STRONG_GROUPS = {
    "health-authority": (
        r"\bминздрав\w*",
        r"\bздравоохран\w*",
        r"\bроспотребнадзор\w*",
    ),
    "medical-institution": (
        r"\bбольниц\w*",
        r"\bполиклиник\w*",
        r"\bроддом\w*",
        r"\bцрб\b",
        r"\bфельдшер\w*",
        r"\bмедицинск\w+\s+(?:центр|учрежден|комплекс|организац)",
    ),
    "medical-professionals": (
        r"\bврач\w*",
        r"\bмедик\w*",
        r"\bфельдшер\w*",
        r"\bакушер\w*",
    ),
    "patients-and-care": (
        r"\bпациент\w*",
        r"\bлечен\w*",
        r"\bмедицинск\w+\s+помощ",
        r"\bпервичн\w+\s+медицинск\w+\s+помощ",
        r"\bскор\w+\s+помощ",
    ),
    "clinical-topic": (
        r"\bдиагност\w*",
        r"\bзаболеван\w*",
        r"\bтерап\w*",
        r"\bхирург\w*",
        r"\bреабилитац\w*",
        r"\bреанимац\w*",
        r"\bсердечно[-\s]*легочн\w*",
        r"\bсердечно[-\s]*лёгочн\w*",
        r"\bмедицинск\w+\s+обследован\w*",
        r"\bпрофилактическ\w+\s+обследован\w*",
        r"\bдиагностическ\w+\s+обследован\w*",
        r"\bпройти\s+обследован\w*",
        r"\bнаправ\w+\s+на\s+обследован\w*",
        r"\bобследован\w+\s+(?:пациент\w*|дет\w*|женщин\w*|мужчин\w*|населен\w*)",
        r"\bдиспансеризац\w*",
    ),
    "laboratory-and-screening": (
        r"\bлаборатор\w*",
        r"\bанализ\w*",
        r"\bфлюорограф\w*",
        r"\bскрининг\w*",
    ),
    "vaccination-and-infection": (
        r"\bвакцин\w*",
        r"\bпривив\w*",
        r"\bинфекц\w*",
        r"\bэпидеми\w*",
        r"\bвич\b",
        r"\bспид\b",
        r"\bнаркоман\w*",
        r"\bнаркозависим\w*",
    ),
    "maternal-health": (
        r"\bбеременн\w*",
        r"\bродов\b",
        r"\bроды\b",
        r"\bбудущ\w+\s+мам",
        r"\bженск\w+\s+консультац",
    ),
    "pharmaceutical": (
        r"\bлекарств\w*",
        r"\bфармацевт\w*",
        r"\bпрепарат\w*",
    ),
}


# Сами по себе эти признаки недостаточны.
MEDICAL_WEAK_GROUPS = {
    "health": (
        r"\bздоровь\w*",
        r"\bздоров\w*",
    ),
    "prevention": (
        r"\bпрофилактик\w*",
    ),
    "sanitary": (
        r"\bсанитар\w*",
    ),
    "donation": (
        r"\bдонор\w*",
    ),
}


# Используются для диагностики загрязнённых тематических лент.
NON_MEDICAL_GROUPS = {
    "culture": (
        r"\bтеатр\w*",
        r"\bспектакл\w*",
        r"\bфестивал\w*",
        r"\bактер\w*",
        r"\bактёр\w*",
        r"\bсцен\w*",
    ),
    "sport": (
        r"\bспортсмен\w*",
        r"\bчемпионат\w*",
        r"\bпервенств\w*",
        r"\bборьб\w*",
        r"\bфинал\w*",
    ),
    "agriculture": (
        r"\bрыбовод\w*",
        r"\bсельск\w+\s+хозяйств",
        r"\bфермер\w*",
    ),
    "business": (
        r"\bналог\w*",
        r"\bпредпринимател\w*",
        r"\bбизнес\w*",
        r"\bэкономик\w*",
    ),
}


# Такие заголовки обычно описывают несколько несвязанных вопросов.
# Они допускаются только при явном медицинском фокусе в title/summary.
MIXED_NEWS_TITLE_PATTERNS = (
    r"\bаппаратн\w*\s+совещан\w*",
    r"\bоперативн\w*\s+совещан\w*",
    r"\bеженедельн\w*\s+совещан\w*",
    r"\bглавн\w*\s+новост\w*\s+(?:дня|недел)",
    r"\bитог\w*\s+(?:дня|недел|месяц)",
)


# Происшествие не становится медицинской новостью только потому,
# что в тексте упомянуты медики, смерть или медицинская помощь.
INCIDENT_TITLE_PATTERNS = (
    r"\bнашл\w*\s+тело\b",
    r"\bобнаруж\w*\s+тело\b",
    r"\bпогиб\w*\b",
    r"\bдтп\b",
    r"\bавари\w*\b",
    r"\bпожар\w*\b",
    r"\bуголовн\w*\s+дел\w*",
    r"\bмошенничеств\w*\b",
)


# Агрегаторы часто приклеивают после оригинального материала
# чужие публикации из других регионов.
AGGREGATOR_TAIL_PATTERNS = (
    r"\n\s*новости соседних регионов по теме\s*:?\s*\n",
    r"\n\s*новости других регионов по теме\s*:?\s*\n",
    r"\n\s*новости по теме\s*:?\s*\n",
    r"\n\s*другие новости по теме\s*:?\s*\n",
    r"\n\s*ещ[её] новости по теме\s*:?\s*\n",
    r"\n\s*по теме\s*\n",
)


SIGNATURE_STOP_WORDS = {
    "и", "а", "но", "или", "либо",
    "в", "во", "на", "по", "к", "ко",
    "от", "до", "для", "с", "со", "у",
    "о", "об", "обо", "при", "из", "за",
    "над", "под", "между", "через",
    "это", "эта", "этот", "эти",
    "тот", "та", "те",
    "как", "что", "чтобы",
    "который", "которая", "которые",
    "будет", "будут", "был", "была", "были",
    "есть", "уже", "еще", "ещё", "также",
    "северная", "северной", "северную",
    "осетия", "осетии", "рсо", "алания",
    "республика", "республики",
    "россия", "россии",
    "регион", "регионы",
    "году", "года",
    "сегодня", "вчера",
    "сообщил", "сообщила", "сообщили",
    "отметил", "отметила",
    "рассказал", "рассказала", "рассказали",
}


# Общие словосочетания не должны склеивать разные события.
GENERIC_SIGNATURE_BIGRAMS = {
    ("министр", "здравоох"),
    ("министе", "здравоох"),
    ("медицин", "помощь"),
    ("национал", "проект"),
    ("официал", "сайт"),
    ("стало", "известн"),
}


@dataclass
class EventCandidate:
    event: Event
    score: float
    published_at: Any
    normalized_title: str
    evidence: dict[str, Any]
    reason: str


def normalize_text(value: object) -> str:
    text = str(value or "").lower().replace("ё", "е")
    text = re.sub(r"[‐-‒–—−]", "-", text)
    text = re.sub(r"[^\w\s-]", " ", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_title(value: object) -> str:
    text = normalize_text(value)

    noise = {
        "новости",
        "официальный",
        "официальная",
        "сообщает",
        "сообщили",
        "рассказали",
        "опубликовал",
        "опубликован",
        "пресс-служба",
    }

    tokens = [
        token
        for token in text.split()
        if len(token) >= 3 and token not in noise
    ]

    return " ".join(tokens)


def title_similarity(left: str, right: str) -> float:
    if not left or not right:
        return 0.0

    left_tokens = set(left.split())
    right_tokens = set(right.split())

    if not left_tokens or not right_tokens:
        return 0.0

    intersection = len(left_tokens & right_tokens)
    union = len(left_tokens | right_tokens)
    jaccard = intersection / union if union else 0.0

    sequence = SequenceMatcher(
        None,
        left,
        right,
    ).ratio()

    return max(jaccard, sequence)



def matched_pattern_groups(
    text: str,
    groups: dict[str, tuple[str, ...]],
) -> list[str]:
    normalized = normalize_text(text)
    hits = []

    for label, patterns in groups.items():
        if any(
            re.search(pattern, normalized, flags=re.I)
            for pattern in patterns
        ):
            hits.append(label)

    return hits


def medical_content_relevance(
    event: Event,
) -> tuple[float, list[str], list[str], list[str]]:
    """
    Оценивает реальную медицинскую релевантность содержания.

    Event.topic и Source.topic не считаются доказательством:
    агрегаторы могут помещать нерелевантные публикации
    в тематические каналы.
    """

    content = event_full_text(event)

    strong_hits = matched_pattern_groups(
        content,
        MEDICAL_STRONG_GROUPS,
    )
    weak_hits = matched_pattern_groups(
        content,
        MEDICAL_WEAK_GROUPS,
    )
    negative_hits = matched_pattern_groups(
        content,
        NON_MEDICAL_GROUPS,
    )

    score = min(
        1.0,
        len(strong_hits) * 0.28
        + len(weak_hits) * 0.08,
    )

    # Театр, спорт или экономика без сильных медицинских
    # признаков не должны проходить через загрязнённую ленту.
    if negative_hits and not strong_hits:
        score = 0.0

    return (
        score,
        strong_hits,
        weak_hits,
        negative_hits,
    )


def signature_stem(word: str) -> str:
    """
    Лёгкая нормализация русских окончаний без внешних библиотек.
    Нужна только для поиска похожих сюжетов.
    """

    word = normalize_text(word)

    if len(word) >= 10:
        return word[:8]

    if len(word) >= 8:
        return word[:7]

    if len(word) >= 6:
        return word[:6]

    return word


def event_signature_tokens(event: Event) -> list[str]:
    source = normalize_text(
        f"{event.title or ''} {event.summary or ''}"
    )

    return [
        signature_stem(word)
        for word in source.split()
        if (
            len(word) >= 4
            and word not in SIGNATURE_STOP_WORDS
        )
    ]


def event_signature_bigrams(
    event: Event,
) -> set[tuple[str, str]]:
    tokens = event_signature_tokens(event)

    return {
        (left, right)
        for left, right in zip(tokens, tokens[1:])
        if (
            (left, right)
            not in GENERIC_SIGNATURE_BIGRAMS
        )
    }


def candidate_similarity(
    left: EventCandidate,
    right: EventCandidate,
) -> float:
    title_score = title_similarity(
        left.normalized_title,
        right.normalized_title,
    )

    left_tokens = set(
        event_signature_tokens(left.event)
    )
    right_tokens = set(
        event_signature_tokens(right.event)
    )

    intersection = left_tokens & right_tokens
    union = left_tokens | right_tokens

    jaccard = (
        len(intersection) / len(union)
        if union
        else 0.0
    )

    overlap = (
        len(intersection)
        / min(len(left_tokens), len(right_tokens))
        if left_tokens and right_tokens
        else 0.0
    )

    shared_bigrams = (
        event_signature_bigrams(left.event)
        & event_signature_bigrams(right.event)
    )

    close_in_time = False

    if left.published_at and right.published_at:
        close_in_time = (
            abs(
                (
                    left.published_at
                    - right.published_at
                ).total_seconds()
            )
            <= 3 * 24 * 60 * 60
        )

    semantic_score = max(
        title_score,
        jaccard,
    )

    # Совпавшее редкое словосочетание в близкий период:
    # например, "индивидуальное сопровождение".
    if shared_bigrams and close_in_time:
        semantic_score = max(
            semantic_score,
            0.82,
        )

    # Один заголовок может быть кратким пересказом другого.
    if overlap >= 0.55 and close_in_time:
        semantic_score = max(
            semantic_score,
            0.78,
        )

    return min(semantic_score, 1.0)



def clean_aggregator_article_text(
    value: object,
) -> str:
    """
    Удаляет приклеенные агрегатором подборки чужих публикаций.

    Обрезание выполняется только если маркер найден не в самом
    начале текста, чтобы не удалить короткий исходный материал.
    """

    text = str(value or "").strip()

    if not text:
        return ""

    cut_positions = []

    for pattern in AGGREGATOR_TAIL_PATTERNS:
        match = re.search(
            pattern,
            text,
            flags=re.I,
        )

        if match and match.start() >= 200:
            cut_positions.append(match.start())

    if cut_positions:
        text = text[:min(cut_positions)].rstrip()

    # Удаляем подряд идущие дубли строк, часто встречающиеся
    # в выгрузках агрегаторов.
    result_lines = []
    previous_normalized = ""

    for raw_line in text.splitlines():
        line = raw_line.strip()

        if not line:
            if result_lines and result_lines[-1] != "":
                result_lines.append("")
            continue

        normalized = normalize_text(line)

        if (
            normalized
            and normalized == previous_normalized
        ):
            continue

        result_lines.append(line)
        previous_normalized = normalized

    return "\n".join(result_lines).strip()


def title_matches_patterns(
    value: object,
    patterns: tuple[str, ...],
) -> bool:
    normalized = normalize_text(value)

    return any(
        re.search(pattern, normalized, flags=re.I)
        for pattern in patterns
    )

def event_article_body(event: Event) -> str:
    """
    Возвращает основной текст публикаций без искусственного
    повышения плотности повторяющимися заголовками.
    """

    chunks = []

    for event_item in event.items.all():
        raw = event_item.item
        article = getattr(raw, "article", None)

        if (
            article
            and article.text
            and not article.extract_error
        ):
            chunks.append(
                clean_aggregator_article_text(
                    article.text
                )
            )
        elif raw.summary:
            chunks.append(raw.summary)

    return "\n".join(chunks)


def split_substantive_sentences(
    value: object,
) -> list[str]:
    text = str(value or "")

    parts = re.split(
        r"(?<=[.!?])\s+|\n+",
        text,
    )

    return [
        normalize_text(part)
        for part in parts
        if len(normalize_text(part)) >= 35
    ]


def sentence_has_medical_anchor(
    sentence: str,
) -> bool:
    return bool(
        matched_pattern_groups(
            sentence,
            MEDICAL_STRONG_GROUPS,
        )
    )


def medical_focus_metrics(
    event: Event,
) -> dict[str, Any]:
    """
    Отличает полноценный медицинский материал от общей новости,
    где медицина упомянута только в одном абзаце.
    """

    title = str(event.title or "")
    summary = str(event.summary or "")
    body = event_article_body(event)

    headline_text = f"{title}\n{summary}"
    lead_text = f"{headline_text}\n{body[:1800]}"

    headline_strong_hits = matched_pattern_groups(
        headline_text,
        MEDICAL_STRONG_GROUPS,
    )

    headline_weak_hits = matched_pattern_groups(
        headline_text,
        MEDICAL_WEAK_GROUPS,
    )

    lead_strong_hits = matched_pattern_groups(
        lead_text,
        MEDICAL_STRONG_GROUPS,
    )

    sentences = split_substantive_sentences(body)

    medical_sentences = [
        sentence
        for sentence in sentences
        if sentence_has_medical_anchor(sentence)
    ]

    sentence_density = (
        len(medical_sentences) / len(sentences)
        if sentences
        else 0.0
    )

    headline_score = min(
        1.0,
        len(headline_strong_hits) * 0.35
        + len(headline_weak_hits) * 0.08,
    )

    lead_score = min(
        1.0,
        len(lead_strong_hits) * 0.20,
    )

    focus_score = min(
        1.0,
        headline_score * 0.55
        + lead_score * 0.20
        + sentence_density * 1.25,
    )

    return {
        "headline_strong_hits": headline_strong_hits,
        "headline_weak_hits": headline_weak_hits,
        "lead_strong_hits": lead_strong_hits,
        "sentence_count": len(sentences),
        "medical_sentence_count": len(
            medical_sentences
        ),
        "medical_sentence_density": round(
            sentence_density,
            4,
        ),
        "headline_score": round(
            headline_score,
            4,
        ),
        "lead_score": round(
            lead_score,
            4,
        ),
        "focus_score": round(
            focus_score,
            4,
        ),
    }

def source_id_for(item_id: int) -> str:
    return f"S{item_id}"


def event_id_for(event_id: int) -> str:
    return f"E{event_id}"


def trim_text(value: object, limit: int) -> str:
    text = str(value or "").strip()

    if len(text) <= limit:
        return text

    return text[:limit].rstrip() + "…"


def event_full_text(event: Event) -> str:
    chunks = [
        event.title or "",
        event.summary or "",
    ]

    for event_item in event.items.all():
        raw = event_item.item

        chunks.extend(
            [
                raw.title or "",
                raw.summary or "",
            ]
        )

        article = getattr(raw, "article", None)

        if article:
            chunks.extend(
                [
                    article.title or "",
                    article.text or "",
                ]
            )

    return "\n".join(chunks)


def matches_region(
    event: Event,
    region_code: str,
    region_markers: list[str],
) -> bool:
    if region_code:
        event_region = normalize_text(event.region)

        if event_region == normalize_text(region_code):
            return True

    if not region_markers:
        return True

    haystack = normalize_text(event_full_text(event))

    return any(
        marker in haystack
        for marker in region_markers
    )


def build_event_evidence(
    event: Event,
) -> tuple[dict[str, Any], Any, float, str]:
    sources = []
    source_classes = []
    published_values = []
    total_text_chars = 0

    for event_item in event.items.all():
        raw = event_item.item
        source = raw.source
        article = getattr(raw, "article", None)

        article_text = (
            clean_aggregator_article_text(
                article.text
            )
            if (
                article
                and article.text
                and not article.extract_error
            )
            else ""
        )

        source_class = str(
            getattr(source, "source_class", "") or ""
        )

        source_classes.append(source_class)
        total_text_chars += len(article_text or "")

        if raw.published_at:
            published_values.append(raw.published_at)

        sources.append(
            {
                "source_id": source_id_for(raw.id),
                "raw_item_id": raw.id,
                "source_database_id": source.id,
                "source_name": source.name,
                "source_class": source_class,
                "source_region": str(source.region or ""),
                "source_topic": str(source.topic or ""),
                "url": raw.url,
                "final_url": (
                    article.final_url
                    if article
                    else ""
                ),
                "published_at": (
                    raw.published_at.isoformat()
                    if raw.published_at
                    else None
                ),
                "title": trim_text(
                    article.title
                    if article and article.title
                    else raw.title,
                    600,
                ),
                "summary": trim_text(
                    raw.summary,
                    2500,
                ),
                "article_text": trim_text(
                    article_text,
                    9000,
                ),
                "extract_error": (
                    article.extract_error
                    if article
                    else "article-not-extracted"
                ),
            }
        )

    published_at = (
        max(published_values)
        if published_values
        else event.updated_at
    )

    best_source_weight = max(
        [
            SOURCE_CLASS_WEIGHTS.get(
                source_class,
                0.55,
            )
            for source_class in source_classes
        ]
        or [0.45]
    )

    article_density = min(
        1.0,
        math.log10(max(total_text_chars, 10)) / 4.0,
    )

    evidence_score = min(
        1.0,
        max(float(event.evidence_level or 0), 0.0) / 3.0,
    )

    score = (
        best_source_weight * 0.42
        + article_density * 0.28
        + evidence_score * 0.20
        + min(len(sources), 3) / 3.0 * 0.10
    )

    reason = (
        f"source_weight={best_source_weight:.2f}, "
        f"article_density={article_density:.2f}, "
        f"evidence={evidence_score:.2f}, "
        f"sources={len(sources)}"
    )

    evidence = {
        "event_id": event_id_for(event.id),
        "database_event_id": event.id,
        "title": trim_text(event.title, 700),
        "summary": trim_text(event.summary, 5000),
        "region": event.region,
        "topic": event.topic,
        "evidence_level": event.evidence_level,
        "published_at": (
            published_at.isoformat()
            if published_at
            else None
        ),
        "sources": sources,
    }

    return evidence, published_at, score, reason


def choose_distinct_events(
    candidates: list[EventCandidate],
    max_events: int,
    duplicate_threshold: float,
) -> tuple[list[EventCandidate], list[dict[str, Any]]]:
    selected: list[EventCandidate] = []
    duplicates: list[dict[str, Any]] = []

    for candidate in sorted(
        candidates,
        key=lambda item: (
            item.published_at or timezone.now(),
            item.score,
            item.event.id,
        ),
        reverse=True,
    ):
        duplicate_of = None
        duplicate_score = 0.0

        for existing in selected:
            similarity = candidate_similarity(
                candidate,
                existing,
            )

            if similarity >= duplicate_threshold:
                duplicate_of = existing
                duplicate_score = similarity
                break

        if duplicate_of is not None:
            duplicates.append(
                {
                    "event_id": candidate.event.id,
                    "duplicate_of_event_id": duplicate_of.event.id,
                    "similarity": round(
                        duplicate_score,
                        4,
                    ),
                    "title": candidate.event.title,
                }
            )
            continue

        selected.append(candidate)

        if len(selected) >= max_events:
            break

    return selected, duplicates


class Command(BaseCommand):
    help = (
        "Создаёт evidence-pack регионального или тематического "
        "дайджеста из нескольких разных событий."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--region",
            default="north_ossetia",
            help=(
                "Логический регион. Встроенные значения: "
                "north_ossetia, russia."
            ),
        )
        parser.add_argument(
            "--region-code",
            default="",
            help=(
                "Точное значение Event.region. "
                "Можно оставить пустым."
            ),
        )
        parser.add_argument(
            "--region-query",
            default="",
            help=(
                "Дополнительные текстовые маркеры региона "
                "через запятую."
            ),
        )
        parser.add_argument(
            "--region-label",
            default="",
        )
        parser.add_argument(
            "--topic",
            default="medicine",
        )
        parser.add_argument(
            "--days",
            type=int,
            default=30,
        )
        parser.add_argument(
            "--min-events",
            type=int,
            default=3,
        )
        parser.add_argument(
            "--max-events",
            type=int,
            default=6,
        )
        parser.add_argument(
            "--duplicate-threshold",
            type=float,
            default=0.72,
        )
        parser.add_argument(
            "--min-content-score",
            type=float,
            default=0.28,
            help=(
                "Минимальная содержательная релевантность теме. "
                "Для medicine значение 0.28 означает наличие "
                "хотя бы одной сильной медицинской группы."
            ),
        )
        parser.add_argument(
            "--min-medical-density",
            type=float,
            default=0.20,
            help=(
                "Минимальная доля медицинских предложений "
                "для материалов без медицинского фокуса "
                "в заголовке или summary."
            ),
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
        )
        parser.add_argument(
            "--show-candidates",
            action="store_true",
        )

    def handle(self, *args, **options):
        region = normalize_text(options["region"])
        region_code = str(options["region_code"] or "").strip()
        topic = str(options["topic"] or "").strip()
        days = max(int(options["days"]), 1)
        min_events = max(int(options["min_events"]), 1)
        max_events = max(
            int(options["max_events"]),
            min_events,
        )
        duplicate_threshold = float(
            options["duplicate_threshold"]
        )
        min_content_score = float(
            options["min_content_score"]
        )
        min_medical_density = float(
            options["min_medical_density"]
        )

        if not 0.0 <= min_medical_density <= 1.0:
            raise CommandError(
                "--min-medical-density must be between 0 and 1"
            )

        if not 0.0 <= min_content_score <= 1.0:
            raise CommandError(
                "--min-content-score must be between 0 and 1"
            )

        if not 0.0 <= duplicate_threshold <= 1.0:
            raise CommandError(
                "--duplicate-threshold must be between 0 and 1"
            )

        region_markers = list(
            REGION_ALIASES.get(region, [])
        )

        extra_markers = [
            normalize_text(value)
            for value in str(
                options["region_query"] or ""
            ).split(",")
            if normalize_text(value)
        ]

        for marker in extra_markers:
            if marker not in region_markers:
                region_markers.append(marker)

        region_label = (
            str(options["region_label"] or "").strip()
            or {
                "north_ossetia": "Северная Осетия",
                "russia": "Россия",
            }.get(region, region)
        )

        period_end = timezone.now()
        period_start = period_end - timedelta(days=days)

        event_items = EventItem.objects.select_related(
            "item",
            "item__source",
            "item__article",
        ).order_by(
            "-item__published_at",
            "-id",
        )

        queryset = (
            Event.objects
            .filter(updated_at__gte=period_start)
            .prefetch_related(
                Prefetch(
                    "items",
                    queryset=event_items,
                )
            )
            .order_by("-updated_at", "-id")
        )

        if topic:
            queryset = queryset.filter(topic=topic)

        candidates: list[EventCandidate] = []
        content_rejections: list[dict[str, Any]] = []

        for event in queryset:
            if not matches_region(
                event,
                region_code,
                region_markers,
            ):
                continue

            (
                content_score,
                strong_hits,
                weak_hits,
                negative_hits,
            ) = medical_content_relevance(event)

            focus = medical_focus_metrics(event)

            is_medical_topic = (
                normalize_text(topic)
                in {"medicine", "medical", "healthcare"}
            )

            if (
                is_medical_topic
                and content_score < min_content_score
            ):
                content_rejections.append(
                    {
                        "event_id": event.id,
                        "title": event.title,
                        "rejection_reason": (
                            "low-content-score"
                        ),
                        "content_score": round(
                            content_score,
                            3,
                        ),
                        "strong_hits": strong_hits,
                        "weak_hits": weak_hits,
                        "negative_hits": negative_hits,
                        **focus,
                    }
                )
                continue

            headline_has_medical_focus = bool(
                focus["headline_strong_hits"]
            )

            mixed_news_headline = title_matches_patterns(
                event.title,
                MIXED_NEWS_TITLE_PATTERNS,
            )

            incident_headline = title_matches_patterns(
                event.title,
                INCIDENT_TITLE_PATTERNS,
            )

            # Происшествия относятся к отдельной редакционной
            # категории. Упоминание медиков, тела, обследования
            # места или медицинской помощи не делает их частью
            # медицинского регионального дайджеста.
            if is_medical_topic and incident_headline:
                content_rejections.append(
                    {
                        "event_id": event.id,
                        "title": event.title,
                        "rejection_reason": (
                            "incident-headline"
                        ),
                        "content_score": round(
                            content_score,
                            3,
                        ),
                        "strong_hits": strong_hits,
                        "weak_hits": weak_hits,
                        "negative_hits": negative_hits,
                        **focus,
                    }
                )
                continue

            if (
                is_medical_topic
                and mixed_news_headline
                and not headline_has_medical_focus
            ):
                content_rejections.append(
                    {
                        "event_id": event.id,
                        "title": event.title,
                        "rejection_reason": (
                            "non-medical-headline"
                        ),
                        "content_score": round(
                            content_score,
                            3,
                        ),
                        "strong_hits": strong_hits,
                        "weak_hits": weak_hits,
                        "negative_hits": negative_hits,
                        **focus,
                    }
                )
                continue

            body_has_medical_focus = (
                len(strong_hits) >= 2
                and (
                    focus[
                        "medical_sentence_density"
                    ]
                    >= min_medical_density
                )
            )

            if (
                is_medical_topic
                and not headline_has_medical_focus
                and not body_has_medical_focus
            ):
                content_rejections.append(
                    {
                        "event_id": event.id,
                        "title": event.title,
                        "rejection_reason": (
                            "not-medical-focus"
                        ),
                        "content_score": round(
                            content_score,
                            3,
                        ),
                        "strong_hits": strong_hits,
                        "weak_hits": weak_hits,
                        "negative_hits": negative_hits,
                        **focus,
                    }
                )
                continue

            evidence, published_at, score, reason = (
                build_event_evidence(event)
            )

            score = min(
                1.0,
                score * 0.80
                + focus["focus_score"] * 0.20,
            )

            reason = (
                f"{reason}, "
                f"content_score={content_score:.2f}, "
                f"focus_score={focus['focus_score']:.2f}, "
                f"density="
                f"{focus['medical_sentence_density']:.2f}, "
                f"strong={','.join(strong_hits) or '-'}, "
                f"weak={','.join(weak_hits) or '-'}"
            )

            evidence["content_relevance"] = {
                "score": round(content_score, 4),
                "strong_hits": strong_hits,
                "weak_hits": weak_hits,
                "negative_hits": negative_hits,
                "focus": focus,
            }

            if not evidence["sources"]:
                continue

            normalized = normalize_title(
                event.title
                or event.summary
            )

            if len(normalized) < 12:
                continue

            candidates.append(
                EventCandidate(
                    event=event,
                    score=score,
                    published_at=published_at,
                    normalized_title=normalized,
                    evidence=evidence,
                    reason=reason,
                )
            )

        selected, duplicates = choose_distinct_events(
            candidates,
            max_events=max_events,
            duplicate_threshold=duplicate_threshold,
        )

        self.stdout.write(
            f"Region: {region_label}"
        )
        self.stdout.write(
            f"Topic: {topic or 'any'}"
        )
        self.stdout.write(
            f"Period: {period_start.isoformat()} "
            f"— {period_end.isoformat()}"
        )
        self.stdout.write(
            f"Candidates after content filter: {len(candidates)}"
        )
        self.stdout.write(
            "Filtered by content relevance: "
            f"{len(content_rejections)}"
        )
        self.stdout.write(
            f"Selected distinct events: {len(selected)}"
        )
        self.stdout.write(
            f"Removed probable duplicates: {len(duplicates)}"
        )

        if options["show_candidates"]:
            if content_rejections:
                self.stdout.write("")
                self.stdout.write(
                    "=== FILTERED AS NON-MEDICAL ==="
                )

                for rejected in content_rejections[:30]:
                    self.stdout.write(
                        f"Event #{rejected['event_id']} "
                        f"reason="
                        f"{rejected.get('rejection_reason', '-')} "
                        f"score={rejected['content_score']:.3f} "
                        f"density="
                        f"{rejected.get('medical_sentence_density', 0):.3f} "
                        f"headline="
                        f"{','.join(rejected.get('headline_strong_hits', [])) or '-'} "
                        f"negative="
                        f"{','.join(rejected['negative_hits']) or '-'} "
                        f"| {rejected['title'][:150]}"
                    )

            self.stdout.write("")
            self.stdout.write("=== SELECTED EVENTS ===")

            for position, candidate in enumerate(
                selected,
                start=1,
            ):
                self.stdout.write(
                    f"{position}. Event #{candidate.event.id} "
                    f"score={candidate.score:.3f} | "
                    f"{candidate.event.title[:160]}"
                )
                self.stdout.write(
                    f"   {candidate.reason}"
                )

            if duplicates:
                self.stdout.write("")
                self.stdout.write("=== PROBABLE DUPLICATES ===")

                for duplicate in duplicates[:30]:
                    self.stdout.write(
                        f"Event #{duplicate['event_id']} -> "
                        f"#{duplicate['duplicate_of_event_id']} "
                        f"similarity={duplicate['similarity']:.3f} | "
                        f"{duplicate['title'][:140]}"
                    )

        if len(selected) < min_events:
            raise CommandError(
                "Недостаточно разных событий для дайджеста: "
                f"{len(selected)} < {min_events}"
            )

        selected_event_ids = sorted(
            candidate.event.id
            for candidate in selected
        )

        criteria = {
            "digest_type": "regional",
            "region": region,
            "region_code": region_code,
            "region_label": region_label,
            "region_markers": region_markers,
            "topic": topic,
            "days": days,
            "min_events": min_events,
            "max_events": max_events,
            "duplicate_threshold": duplicate_threshold,
            "min_content_score": min_content_score,
            "min_medical_density": min_medical_density,
            "content_filter_version": (
                MEDICAL_CONTENT_FILTER_VERSION
            ),
        }

        evidence_pack = {
            "schema_version": 1,
            "digest_type": "regional",
            "region": {
                "code": region_code,
                "key": region,
                "label": region_label,
                "markers": region_markers,
            },
            "topic": topic,
            "period": {
                "start": period_start.isoformat(),
                "end": period_end.isoformat(),
            },
            "events": [
                candidate.evidence
                for candidate in selected
            ],
            "duplicate_events_excluded": duplicates,
            "content_rejections": content_rejections,
            "generation_policy": {
                "mode": "multi_event_digest",
                "external_knowledge_allowed": False,
                "each_section_requires_event_ids": True,
                "each_section_requires_source_ids": True,
                "trend_claims_allowed": False,
                "unsupported_inferences_allowed": False,
            },
        }

        fingerprint_payload = {
            "criteria": criteria,
            "event_ids": selected_event_ids,
        }

        group_key = hashlib.sha256(
            json.dumps(
                fingerprint_payload,
                ensure_ascii=False,
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()

        self.stdout.write("")
        self.stdout.write(
            f"Group key: {group_key}"
        )

        if options["dry_run"]:
            self.stdout.write(
                "Dry-run only. Database was not changed."
            )
            self.stdout.write("")
            self.stdout.write(
                json.dumps(
                    evidence_pack,
                    ensure_ascii=False,
                    indent=2,
                )[:12000]
            )
            return

        with transaction.atomic():
            digest, created = (
                RegionalDigest.objects.get_or_create(
                    group_key=group_key,
                    defaults={
                        "digest_type": "regional",
                        "region_code": region_code,
                        "region_label": region_label,
                        "region_query": ", ".join(
                            region_markers
                        ),
                        "topic": topic,
                        "period_start": period_start,
                        "period_end": period_end,
                        "criteria": criteria,
                        "evidence_pack": evidence_pack,
                        "status": RegionalDigestStatus.READY,
                    },
                )
            )

            if created:
                RegionalDigestItem.objects.bulk_create(
                    [
                        RegionalDigestItem(
                            digest=digest,
                            event=candidate.event,
                            position=position,
                            relevance_score=candidate.score,
                            selection_reason=candidate.reason,
                        )
                        for position, candidate in enumerate(
                            selected,
                            start=1,
                        )
                    ]
                )

        action = "Created" if created else "Already exists"

        self.stdout.write(
            self.style.SUCCESS(
                f"{action}: RegionalDigest #{digest.id}"
            )
        )
        self.stdout.write(
            f"Status: {digest.status}"
        )
        self.stdout.write(
            f"Events: {digest.digest_items.count()}"
        )
