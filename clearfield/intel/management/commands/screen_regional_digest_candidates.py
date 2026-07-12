import hashlib
import json
import os
import re
from datetime import timedelta
from typing import Any

from django.core.management.base import (
    BaseCommand,
    CommandError,
)
from django.db import (
    close_old_connections,
    transaction,
)
from django.db.models import Prefetch
from django.utils import timezone

from intel.llm.ollama_client import (
    generate_with_ollama,
    parse_json_response as parse_ollama_json_response,
)


_SEMANTIC_NUMERIC_FIELDS = {
    "medical_focus",
    "regional_relevance",
    "source_sufficiency",
    "priority",
    "semantic_score",
    "score",
    "confidence",
}


def _coerce_semantic_number(value):
    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)):
        return value

    if not isinstance(value, str):
        return value

    normalized = value.strip().replace(",", ".")

    is_percent = normalized.endswith("%")

    if is_percent:
        normalized = normalized[:-1].strip()

    try:
        number = float(normalized)
    except (TypeError, ValueError):
        return value

    if is_percent:
        number /= 100.0

    return number


def _normalize_semantic_numeric_fields(value):
    if isinstance(value, list):
        return [
            _normalize_semantic_numeric_fields(item)
            for item in value
        ]

    if not isinstance(value, dict):
        return value

    result = {}

    for key, item in value.items():
        normalized_item = (
            _normalize_semantic_numeric_fields(item)
        )

        if key in _SEMANTIC_NUMERIC_FIELDS:
            normalized_item = _coerce_semantic_number(
                normalized_item
            )

        result[key] = normalized_item

    return result


def parse_json_response(raw_text):
    payload = parse_ollama_json_response(raw_text)

    return _normalize_semantic_numeric_fields(
        payload
    )
from intel.management.commands.create_regional_digest import (
    REGION_ALIASES,
    EventCandidate,
    build_event_evidence,
    candidate_similarity,
    event_article_body,
    matches_region,
    normalize_text,
    normalize_title,
)
from intel.models import (
    Event,
    EventItem,
    RegionalDigest,
    RegionalDigestItem,
    RegionalDigestStatus,
)


SCREEN_SYSTEM_PROMPT = """
Ты проверяешь кандидатов для регионального медицинского дайджеста.

Работай только с переданным текстом.
Не используй внешние знания.
Не переписывай новости и не извлекай новые факты.

Для каждого event_id определи:

1. Является ли медицина основной темой публикации.
2. Относится ли событие к указанному региону.
3. Достаточно ли в тексте фактических сведений.
4. Является ли публикация общей сводкой, где медицина
   упомянута только эпизодически.
5. Является ли публикация происшествием, криминальной
   или спортивной новостью вместо медицинской.
6. Какой у материала основной тип события.

Правила:
- слово "здоровье" само по себе не делает материал медицинским;
- упоминание Минздрава среди участников не делает общую
  новость медицинской;
- медицинское сопровождение фестиваля не делает новость
  о фестивале медицинской;
- обследование территории или объекта не является
  медицинским обследованием;
- первая помощь, профилактика заболеваний, работа больниц,
  диагностика, лечение и организация здравоохранения
  относятся к медицинской тематике;
- include должен отражать твоё решение, но окончательные
  пороги применит программа;
- верни решение для каждого переданного event_id;
- не добавляй новые event_id;
- верни только JSON.
""".strip()


SELECTION_SYSTEM_PROMPT = """
Ты выбираешь события для регионального медицинского дайджеста.

Все переданные материалы уже прошли предварительную
семантическую проверку.

Твоя задача:
- найти публикации об одном и том же событии;
- для каждого дубля указать основную публикацию;
- выбрать разнообразный набор разных событий;
- не объединять разные события только из-за общей медицинской темы;
- не считать общей тенденцией несколько отдельных сообщений;
- отдавать приоритет официальным и фактически насыщенным источникам;
- сохранить разные направления региональной медицины;
- вернуть решение для каждого event_id;
- не добавлять новые event_id;
- верни только JSON.
""".strip()


def clip(value: object, limit: int) -> str:
    text = str(value or "").strip()

    if len(text) <= limit:
        return text

    return text[:limit].rstrip() + "…"


def parse_bool(
    value: object,
    field_name: str,
) -> bool:
    if isinstance(value, bool):
        return value

    if isinstance(value, str):
        normalized = value.strip().lower()

        if normalized in {"true", "yes", "1"}:
            return True

        if normalized in {"false", "no", "0"}:
            return False

    raise CommandError(
        f"{field_name} must be boolean, got {value!r}"
    )


def parse_score(value: Any, field_name: str) -> float:
    """
    Parse score values from CLI args and LLM JSON.

    CLI thresholds remain strict. LLM screening scores are
    allowed to be noisy: if one score is malformed, we treat
    it as 0.0 so that the candidate fails thresholds instead
    of crashing the whole daily pipeline.
    """

    is_llm_field = str(field_name).startswith("E")

    def normalize_number(number: float) -> float | None:
        if number > 1.0 and number <= 100.0:
            number = number / 100.0

        if 0.0 <= number <= 1.0:
            return number

        return None

    def extract(raw_value):
        if isinstance(raw_value, bool):
            return None

        if isinstance(raw_value, (int, float)):
            return normalize_number(float(raw_value))

        if isinstance(raw_value, str):
            raw = (
                raw_value
                .strip()
                .replace(",", ".")
                .replace("−", "-")
            )

            if not raw:
                return None

            percent = "%" in raw

            match = re.search(
                r"[-+]?\d+(?:\.\d+)?",
                raw,
            )

            if not match:
                return None

            try:
                number = float(match.group(0))
            except ValueError:
                return None

            if percent:
                number = number / 100.0

            return normalize_number(number)

        if isinstance(raw_value, dict):
            preferred_keys = (
                "score",
                "value",
                "rating",
                "number",
                "numeric",
                "medical_focus",
                "regional_relevance",
                "source_sufficiency",
                "priority",
                "semantic_score",
                "confidence",
            )

            for key in preferred_keys:
                if key not in raw_value:
                    continue

                parsed = extract(raw_value[key])

                if parsed is not None:
                    return parsed

            for nested in raw_value.values():
                parsed = extract(nested)

                if parsed is not None:
                    return parsed

            return None

        if isinstance(raw_value, (list, tuple)):
            for nested in raw_value:
                parsed = extract(nested)

                if parsed is not None:
                    return parsed

            return None

        return None

    parsed = extract(value)

    if parsed is not None:
        return parsed

    if is_llm_field:
        return 0.0

    raise CommandError(
        f"{field_name} must be numeric"
    )

def event_identifier(event_id: int) -> str:
    return f"E{event_id}"


def build_preview(
    candidate: EventCandidate,
) -> dict[str, Any]:
    evidence = candidate.evidence
    body = event_article_body(candidate.event)

    sources = []

    for source in evidence.get("sources") or []:
        sources.append(
            {
                "source_id": source.get("source_id"),
                "source_name": source.get("source_name"),
                "source_class": source.get("source_class"),
                "published_at": source.get("published_at"),
            }
        )

    return {
        "event_id": event_identifier(
            candidate.event.id
        ),
        "title": clip(
            candidate.event.title,
            500,
        ),
        "summary": clip(
            candidate.event.summary,
            700,
        ),
        "article_lead": clip(
            body,
            900,
        ),
        "event_region": candidate.event.region,
        "event_topic": candidate.event.topic,
        "published_at": (
            candidate.published_at.isoformat()
            if candidate.published_at
            else None
        ),
        "technical_score": round(
            candidate.score,
            4,
        ),
        "sources": sources,
    }


def collect_candidates(
    *,
    period_start,
    period_end,
    region: str,
    region_code: str,
    region_markers: list[str],
    topic: str,
    max_candidates: int,
    min_article_chars: int,
) -> list[dict[str, Any]]:
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

    # Это только широкий предварительный канал.
    # Его метаданным окончательно не доверяем.
    if topic:
        queryset = queryset.filter(topic=topic)

    published_event_ids = set(
        RegionalDigest.objects
        .filter(
            status=RegionalDigestStatus.PUBLISHED,
        )
        .values_list(
            "digest_items__event_id",
            flat=True,
        )
    )

    records = []

    for event in queryset:
        if event.id in published_event_ids:
            continue

        if not matches_region(
            event,
            region_code,
            region_markers,
        ):
            continue

        body = event_article_body(event)
        normalized_body = normalize_text(body)

        if len(normalized_body) < min_article_chars:
            continue

        (
            evidence,
            published_at,
            technical_score,
            technical_reason,
        ) = build_event_evidence(event)

        if not evidence.get("sources"):
            continue

        age_days = 0.0

        if published_at:
            age_days = max(
                0.0,
                (
                    period_end - published_at
                ).total_seconds()
                / 86400.0,
            )

        period_days = max(
            1.0,
            (
                period_end - period_start
            ).total_seconds()
            / 86400.0,
        )

        freshness = max(
            0.0,
            1.0 - age_days / period_days,
        )

        ranking_score = (
            technical_score * 0.75
            + freshness * 0.25
        )

        candidate = EventCandidate(
            event=event,
            score=technical_score,
            published_at=published_at,
            normalized_title=normalize_title(
                event.title or event.summary
            ),
            evidence=evidence,
            reason=technical_reason,
        )

        records.append(
            {
                "candidate": candidate,
                "preview": build_preview(candidate),
                "technical_score": technical_score,
                "freshness": freshness,
                "ranking_score": ranking_score,
            }
        )

    records.sort(
        key=lambda item: (
            item["ranking_score"],
            item["candidate"].published_at
            or period_start,
            item["candidate"].event.id,
        ),
        reverse=True,
    )

    return records[:max_candidates]


def build_screen_prompt(
    *,
    region_label: str,
    topic: str,
    batch: list[dict[str, Any]],
) -> str:
    schema = {
        "items": [
            {
                "event_id": "E123",
                "include": True,
                "primary_topic": "medicine",
                "medical_focus": 0.95,
                "regional_relevance": 0.95,
                "source_sufficiency": 0.80,
                "is_mixed_news": False,
                "is_incident": False,
                "event_type": "healthcare_program",
                "reason": (
                    "Основная тема материала — организация "
                    "медицинской помощи в регионе."
                ),
            }
        ]
    }

    payload = {
        "region": region_label,
        "requested_topic": topic,
        "events": [
            record["preview"]
            for record in batch
        ],
    }

    return (
        "Проверь каждый кандидат для регионального "
        "медицинского дайджеста.\n\n"
        "Ожидаемая JSON-схема:\n"
        + json.dumps(
            schema,
            ensure_ascii=False,
            indent=2,
        )
        + "\n\nКАНДИДАТЫ:\n"
        + json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
        )
    )



def split_screen_batches(
    *,
    records: list[dict[str, Any]],
    region_label: str,
    topic: str,
    requested_batch_size: int,
    max_prompt_chars: int,
) -> list[list[dict[str, Any]]]:
    """
    Формирует пакеты одновременно по количеству событий
    и по фактическому размеру пользовательского промпта.

    max_prompt_chars оставляет запас контекста для:
    - system prompt;
    - JSON-ответа модели;
    - токенизации русского текста.
    """

    batches: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []

    for record in records:
        proposed = current + [record]

        exceeds_count = (
            len(proposed) > requested_batch_size
        )

        proposed_prompt = build_screen_prompt(
            region_label=region_label,
            topic=topic,
            batch=proposed,
        )

        exceeds_chars = (
            len(proposed_prompt) > max_prompt_chars
        )

        if current and (
            exceeds_count
            or exceeds_chars
        ):
            batches.append(current)
            current = [record]

            single_prompt = build_screen_prompt(
                region_label=region_label,
                topic=topic,
                batch=current,
            )

            if len(single_prompt) > max_prompt_chars:
                event_id = record["preview"]["event_id"]

                raise CommandError(
                    f"{event_id}: single screening prompt "
                    f"is too large: {len(single_prompt)} chars"
                )
        else:
            current = proposed

    if current:
        batches.append(current)

    return batches

def validate_screen_response(
    payload: dict[str, Any],
    expected_ids: set[str],
) -> dict[str, dict[str, Any]]:
    if not isinstance(payload, dict):
        raise CommandError(
            "Semantic screening returned non-object JSON"
        )

    items = payload.get("items")

    if not isinstance(items, list):
        raise CommandError(
            "Semantic screening returned no items list"
        )

    decisions: dict[str, dict[str, Any]] = {}

    for index, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            raise CommandError(
                f"Screen item #{index} is not an object"
            )

        event_id = str(
            item.get("event_id") or ""
        ).strip()

        if event_id not in expected_ids:
            raise CommandError(
                f"Screening returned unknown event_id: "
                f"{event_id!r}"
            )

        if event_id in decisions:
            raise CommandError(
                f"Duplicate screening decision: {event_id}"
            )

        reason = str(
            item.get("reason") or ""
        ).strip()

        if len(reason) < 10:
            raise CommandError(
                f"{event_id}: reason is too short"
            )

        decisions[event_id] = {
            "event_id": event_id,
            "model_include": parse_bool(
                item.get("include"),
                f"{event_id}.include",
            ),
            "primary_topic": str(
                item.get("primary_topic") or ""
            ).strip(),
            "medical_focus": parse_score(
                item.get("medical_focus"),
                f"{event_id}.medical_focus",
            ),
            "regional_relevance": parse_score(
                item.get("regional_relevance"),
                f"{event_id}.regional_relevance",
            ),
            "source_sufficiency": parse_score(
                item.get("source_sufficiency"),
                f"{event_id}.source_sufficiency",
            ),
            "is_mixed_news": parse_bool(
                item.get("is_mixed_news"),
                f"{event_id}.is_mixed_news",
            ),
            "is_incident": parse_bool(
                item.get("is_incident"),
                f"{event_id}.is_incident",
            ),
            "event_type": str(
                item.get("event_type") or ""
            ).strip(),
            "reason": reason,
        }

    missing = sorted(
        expected_ids - set(decisions)
    )

    if missing:
        raise CommandError(
            "Model did not return decisions for: "
            + ", ".join(missing)
        )

    return decisions


def apply_semantic_thresholds(
    *,
    decision: dict[str, Any],
    min_medical_focus: float,
    min_regional_relevance: float,
    min_source_sufficiency: float,
) -> bool:
    return bool(
        decision["model_include"]
        and decision["medical_focus"]
        >= min_medical_focus
        and decision["regional_relevance"]
        >= min_regional_relevance
        and decision["source_sufficiency"]
        >= min_source_sufficiency
        and not decision["is_mixed_news"]
        and not decision["is_incident"]
    )


def semantic_score(
    decision: dict[str, Any],
) -> float:
    return (
        decision["medical_focus"] * 0.50
        + decision["regional_relevance"] * 0.25
        + decision["source_sufficiency"] * 0.25
    )



def canonical_record_quality(
    record: dict[str, Any],
) -> float:
    """
    При выборе основной публикации одного события
    качество и надёжность источника важнее свежести
    вторичной перепечатки.
    """

    return (
        record["semantic_score"] * 0.55
        + record["technical_score"] * 0.35
        + record["ranking_score"] * 0.10
    )


def choose_quality_distinct_records(
    *,
    records: list[dict[str, Any]],
    max_events: int,
    duplicate_threshold: float,
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    ranked = sorted(
        records,
        key=lambda record: (
            canonical_record_quality(record),
            record["technical_score"],
            record["semantic_score"],
            record["ranking_score"],
        ),
        reverse=True,
    )

    selected: list[dict[str, Any]] = []
    duplicates: list[dict[str, Any]] = []

    for record in ranked:
        duplicate_of = None
        duplicate_similarity = 0.0

        for existing in selected:
            similarity = candidate_similarity(
                record["candidate"],
                existing["candidate"],
            )

            if similarity >= duplicate_threshold:
                duplicate_of = existing
                duplicate_similarity = similarity
                break

        if duplicate_of is not None:
            duplicates.append(
                {
                    "event_id": (
                        record["preview"]["event_id"]
                    ),
                    "duplicate_of_event_id": (
                        duplicate_of[
                            "preview"
                        ]["event_id"]
                    ),
                    "similarity": round(
                        duplicate_similarity,
                        4,
                    ),
                    "discarded_quality": round(
                        canonical_record_quality(record),
                        4,
                    ),
                    "canonical_quality": round(
                        canonical_record_quality(
                            duplicate_of
                        ),
                        4,
                    ),
                    "reason": (
                        "Смысловой дубль; сохранена "
                        "публикация с более сильным "
                        "источником и общим рейтингом."
                    ),
                }
            )
            continue

        selected.append(record)

        if len(selected) >= max_events:
            break

    return selected, duplicates

def build_selection_prompt(
    *,
    region_label: str,
    min_events: int,
    max_events: int,
    records: list[dict[str, Any]],
) -> str:
    schema = {
        "items": [
            {
                "event_id": "E123",
                "select": True,
                "duplicate_of": None,
                "priority": 0.92,
                "reason": (
                    "Самостоятельное медицинское событие "
                    "с достаточным количеством фактов."
                ),
            },
            {
                "event_id": "E124",
                "select": False,
                "duplicate_of": "E123",
                "priority": 0.60,
                "reason": (
                    "Описывает то же событие, что и E123."
                ),
            },
        ]
    }

    payload = {
        "region": region_label,
        "min_events": min_events,
        "max_events": max_events,
        "events": [
            {
                "event_id": record["preview"]["event_id"],
                "title": clip(
                    record["preview"]["title"],
                    400,
                ),
                "summary": clip(
                    record["preview"]["summary"],
                    500,
                ),
                "article_lead": clip(
                    record["preview"]["article_lead"],
                    650,
                ),
                "event_region": (
                    record["preview"]["event_region"]
                ),
                "published_at": (
                    record["preview"]["published_at"]
                ),
                "sources": (
                    record["preview"]["sources"]
                ),
                "semantic_decision": record[
                    "semantic_decision"
                ],
                "semantic_score": round(
                    record["semantic_score"],
                    4,
                ),
            }
            for record in records
        ],
    }

    return (
        "Проверь прошедшие события совместно, найди "
        "смысловые дубли и выбери разнообразный набор.\n\n"
        "duplicate_of должен быть null либо event_id "
        "из этого же списка.\n\n"
        "Ожидаемая JSON-схема:\n"
        + json.dumps(
            schema,
            ensure_ascii=False,
            indent=2,
        )
        + "\n\nСОБЫТИЯ:\n"
        + json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
        )
    )


def validate_selection_response(
    payload: dict[str, Any],
    expected_ids: set[str],
) -> dict[str, dict[str, Any]]:
    if not isinstance(payload, dict):
        raise CommandError(
            "Selection returned non-object JSON"
        )

    items = payload.get("items")

    if not isinstance(items, list):
        raise CommandError(
            "Selection returned no items list"
        )

    decisions: dict[str, dict[str, Any]] = {}

    for index, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            raise CommandError(
                f"Selection item #{index} is not an object"
            )

        event_id = str(
            item.get("event_id") or ""
        ).strip()

        if event_id not in expected_ids:
            raise CommandError(
                f"Selection returned unknown event_id: "
                f"{event_id!r}"
            )

        if event_id in decisions:
            raise CommandError(
                f"Duplicate selection decision: {event_id}"
            )

        duplicate_value = item.get("duplicate_of")

        duplicate_of = (
            str(duplicate_value).strip()
            if duplicate_value
            else None
        )

        if duplicate_of is not None:
            if duplicate_of not in expected_ids:
                raise CommandError(
                    f"{event_id}: unknown duplicate_of "
                    f"{duplicate_of}"
                )

            if duplicate_of == event_id:
                raise CommandError(
                    f"{event_id}: cannot duplicate itself"
                )

        reason = str(
            item.get("reason") or ""
        ).strip()

        if len(reason) < 10:
            raise CommandError(
                f"{event_id}: selection reason is too short"
            )

        decisions[event_id] = {
            "event_id": event_id,
            "select": parse_bool(
                item.get("select"),
                f"{event_id}.select",
            ),
            "duplicate_of": duplicate_of,
            "priority": parse_score(
                item.get("priority"),
                f"{event_id}.priority",
            ),
            "reason": reason,
        }

    missing = sorted(
        expected_ids - set(decisions)
    )

    if missing:
        raise CommandError(
            "Selection did not return decisions for: "
            + ", ".join(missing)
        )

    return decisions


class Command(BaseCommand):
    help = (
        "Проверяет кандидатов для регионального дайджеста "
        "с помощью LLM. Без --execute модель не вызывается. "
        "С --no-save результат не записывается в БД."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--region",
            default="north_ossetia",
        )
        parser.add_argument(
            "--region-code",
            default="",
        )
        parser.add_argument(
            "--region-query",
            default="",
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
            default=90,
        )
        parser.add_argument(
            "--min-events",
            type=int,
            default=1,
        )
        parser.add_argument(
            "--max-events",
            type=int,
            default=6,
        )
        parser.add_argument(
            "--max-candidates",
            type=int,
            default=30,
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=10,
        )
        parser.add_argument(
            "--max-screen-prompt-chars",
            type=int,
            default=15000,
            help=(
                "Максимальный размер пользовательского "
                "промпта одной screening-партии."
            ),
        )
        parser.add_argument(
            "--max-selection-prompt-chars",
            type=int,
            default=16000,
            help=(
                "Максимальный размер совместного промпта "
                "для дедупликации и выбора событий."
            ),
        )
        parser.add_argument(
            "--min-article-chars",
            type=int,
            default=250,
        )
        parser.add_argument(
            "--min-medical-focus",
            type=float,
            default=0.70,
        )
        parser.add_argument(
            "--min-regional-relevance",
            type=float,
            default=0.70,
        )
        parser.add_argument(
            "--min-source-sufficiency",
            type=float,
            default=0.55,
        )
        parser.add_argument(
            "--duplicate-threshold",
            type=float,
            default=0.72,
        )
        parser.add_argument(
            "--model",
            default="",
        )
        parser.add_argument(
            "--execute",
            action="store_true",
            help="Вызвать LLM.",
        )
        parser.add_argument(
            "--no-save",
            action="store_true",
            help=(
                "Вызвать LLM, но не сохранять "
                "RegionalDigest."
            ),
        )
        parser.add_argument(
            "--show-decisions",
            action="store_true",
        )

    def handle(self, *args, **options):
        execute = bool(options["execute"])
        no_save = bool(options["no_save"])

        if no_save and not execute:
            raise CommandError(
                "--no-save имеет смысл только вместе "
                "с --execute"
            )

        region = normalize_text(options["region"])
        region_code = str(
            options["region_code"] or ""
        ).strip()

        region_label = (
            str(options["region_label"] or "").strip()
            or {
                "north_ossetia": "Северная Осетия",
                "russia": "Россия",
            }.get(region, region)
        )

        topic = str(
            options["topic"] or ""
        ).strip()

        days = max(
            int(options["days"]),
            1,
        )

        min_events = max(
            int(options["min_events"]),
            1,
        )

        max_events = max(
            int(options["max_events"]),
            min_events,
        )

        max_candidates = max(
            int(options["max_candidates"]),
            max_events,
        )

        batch_size = max(
            1,
            min(
                int(options["batch_size"]),
                15,
            ),
        )

        max_screen_prompt_chars = max(
            8000,
            int(options["max_screen_prompt_chars"]),
        )

        max_selection_prompt_chars = max(
            8000,
            int(options["max_selection_prompt_chars"]),
        )

        min_article_chars = max(
            int(options["min_article_chars"]),
            100,
        )

        min_medical_focus = parse_score(
            options["min_medical_focus"],
            "min_medical_focus",
        )

        min_regional_relevance = parse_score(
            options["min_regional_relevance"],
            "min_regional_relevance",
        )

        min_source_sufficiency = parse_score(
            options["min_source_sufficiency"],
            "min_source_sufficiency",
        )

        duplicate_threshold = parse_score(
            options["duplicate_threshold"],
            "duplicate_threshold",
        )

        selected_model = (
            str(options["model"] or "").strip()
            or None
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

        period_end = timezone.now()
        period_start = (
            period_end - timedelta(days=days)
        )

        records = collect_candidates(
            period_start=period_start,
            period_end=period_end,
            region=region,
            region_code=region_code,
            region_markers=region_markers,
            topic=topic,
            max_candidates=max_candidates,
            min_article_chars=min_article_chars,
        )

        target_event_id_raw = str(
            os.getenv(
                "REGIONAL_DIGEST_TARGET_EVENT_ID"
            )
            or ""
        ).strip()

        if target_event_id_raw:
            try:
                target_event_id = int(
                    target_event_id_raw
                )
            except ValueError as exc:
                raise CommandError(
                    "REGIONAL_DIGEST_TARGET_EVENT_ID "
                    "must be an integer"
                ) from exc

            target_event_key = (
                f"E{target_event_id}"
            )

            records = [
                record
                for record in records
                if str(
                    (
                        record.get("preview")
                        or {}
                    ).get("event_id")
                    or ""
                ).strip().upper()
                == target_event_key
            ]

            if len(records) != 1:
                raise CommandError(
                    "Target event was not found as "
                    "exactly one candidate: "
                    f"event_id={target_event_id}, "
                    f"matches={len(records)}"
                )

            self.stdout.write(
                self.style.WARNING(
                    "Targeted screening enabled: "
                    f"Event #{target_event_id}"
                )
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
            f"Candidates for semantic screening: "
            f"{len(records)}"
        )
        self.stdout.write(
            f"Requested batch size: {batch_size}"
        )
        self.stdout.write(
            "Screen prompt budget: "
            f"{max_screen_prompt_chars} chars"
        )
        self.stdout.write(
            "Selection prompt budget: "
            f"{max_selection_prompt_chars} chars"
        )

        for position, record in enumerate(
            records,
            start=1,
        ):
            preview = record["preview"]

            self.stdout.write(
                f"{position}. {preview['event_id']} "
                f"technical="
                f"{record['technical_score']:.3f} "
                f"ranking="
                f"{record['ranking_score']:.3f} | "
                f"{preview['title'][:140]}"
            )

        if len(records) < min_events:
            raise CommandError(
                "Недостаточно технических кандидатов: "
                f"{len(records)} < {min_events}"
            )

        if not execute:
            self.stdout.write("")
            self.stdout.write(
                self.style.WARNING(
                    "Dry-run only. Model was not called. "
                    "Database was not changed."
                )
            )
            return

        screening_decisions = {}
        screening_audit = []
        total_elapsed_ms = 0
        resolved_model = ""

        screen_batches = split_screen_batches(
            records=records,
            region_label=region_label,
            topic=topic,
            requested_batch_size=batch_size,
            max_prompt_chars=max_screen_prompt_chars,
        )

        self.stdout.write(
            f"Actual screening batches: "
            f"{len(screen_batches)}"
        )

        for batch_number, batch in enumerate(
            screen_batches,
            start=1,
        ):
            expected_ids = {
                record["preview"]["event_id"]
                for record in batch
            }

            prompt = build_screen_prompt(
                region_label=region_label,
                topic=topic,
                batch=batch,
            )

            self.stdout.write(
                f"Screening batch #{batch_number}: "
                f"{len(batch)} events"
            )

            close_old_connections()

            result = generate_with_ollama(
                prompt=prompt,
                system=SCREEN_SYSTEM_PROMPT,
                json_mode=True,
                model=selected_model,
            )

            total_elapsed_ms += result.elapsed_ms
            resolved_model = result.model

            payload = parse_json_response(
                result.text
            )

            decisions = validate_screen_response(
                payload,
                expected_ids,
            )

            screening_decisions.update(
                decisions
            )

            screening_audit.append(
                {
                    "batch_number": batch_number,
                    "event_ids": sorted(expected_ids),
                    "prompt": prompt,
                    "model": result.model,
                    "elapsed_ms": result.elapsed_ms,
                    "raw_response": result.raw,
                    "validated_decisions": decisions,
                }
            )

        accepted_records = []

        for record in records:
            event_id = record["preview"]["event_id"]
            decision = screening_decisions[event_id]

            passed = apply_semantic_thresholds(
                decision=decision,
                min_medical_focus=min_medical_focus,
                min_regional_relevance=(
                    min_regional_relevance
                ),
                min_source_sufficiency=(
                    min_source_sufficiency
                ),
            )

            decision["threshold_pass"] = passed

            semantic_value = semantic_score(decision)

            record["semantic_decision"] = decision
            record["semantic_score"] = semantic_value

            if passed:
                accepted_records.append(record)

        accepted_records.sort(
            key=lambda item: (
                item["semantic_score"],
                item["ranking_score"],
            ),
            reverse=True,
        )

        self.stdout.write("")
        self.stdout.write(
            f"Passed semantic thresholds: "
            f"{len(accepted_records)}"
        )

        if options["show_decisions"]:
            self.stdout.write("")
            self.stdout.write(
                "=== SEMANTIC DECISIONS ==="
            )

            for record in records:
                decision = record[
                    "semantic_decision"
                ]

                self.stdout.write(
                    f"{decision['event_id']} "
                    f"pass={decision['threshold_pass']} "
                    f"focus="
                    f"{decision['medical_focus']:.2f} "
                    f"region="
                    f"{decision['regional_relevance']:.2f} "
                    f"source="
                    f"{decision['source_sufficiency']:.2f} "
                    f"mixed={decision['is_mixed_news']} "
                    f"incident={decision['is_incident']} "
                    f"type={decision['event_type']} | "
                    f"{decision['reason']}"
                )

        if len(accepted_records) < min_events:
            raise CommandError(
                "После семантической проверки осталось "
                f"недостаточно событий: "
                f"{len(accepted_records)} < {min_events}"
            )

        # Сначала объединяем смысловые дубли.
        # Канонической становится публикация с наиболее
        # сильным сочетанием semantic и source quality,
        # а не просто самая свежая перепечатка.
        (
            distinct_records,
            deterministic_duplicates,
        ) = choose_quality_distinct_records(
            records=accepted_records,
            max_events=min(
                len(accepted_records),
                max_events + 3,
            ),
            duplicate_threshold=(
                duplicate_threshold
            ),
        )

        if len(distinct_records) < min_events:
            raise CommandError(
                "После детерминированной дедупликации "
                "осталось недостаточно событий"
            )

        distinct_records.sort(
            key=lambda item: (
                item["semantic_score"],
                item["ranking_score"],
            ),
            reverse=True,
        )

        selection_prompt = build_selection_prompt(
            region_label=region_label,
            min_events=min_events,
            max_events=max_events,
            records=distinct_records,
        )

        while (
            len(selection_prompt)
            > max_selection_prompt_chars
            and len(distinct_records) > min_events
        ):
            removed = distinct_records.pop()

            self.stdout.write(
                "Selection context trim: removed "
                f"{removed['preview']['event_id']} "
                "from the joint comparison pool"
            )

            selection_prompt = build_selection_prompt(
                region_label=region_label,
                min_events=min_events,
                max_events=max_events,
                records=distinct_records,
            )

        if (
            len(selection_prompt)
            > max_selection_prompt_chars
        ):
            raise CommandError(
                "Selection prompt remains too large: "
                f"{len(selection_prompt)} chars for "
                f"{len(distinct_records)} events"
            )

        self.stdout.write(
            "Selection prompt chars: "
            f"{len(selection_prompt)}"
        )

        self.stdout.write("")
        self.stdout.write(
            "Running joint semantic selection for "
            f"{len(distinct_records)} events"
        )

        close_old_connections()

        selection_result = generate_with_ollama(
            prompt=selection_prompt,
            system=SELECTION_SYSTEM_PROMPT,
            json_mode=True,
            model=selected_model,
        )

        total_elapsed_ms += (
            selection_result.elapsed_ms
        )
        resolved_model = selection_result.model

        selection_payload = parse_json_response(
            selection_result.text
        )

        expected_selection_ids = {
            record["preview"]["event_id"]
            for record in distinct_records
        }

        selection_decisions = (
            validate_selection_response(
                selection_payload,
                expected_selection_ids,
            )
        )

        for record in distinct_records:
            event_id = record["preview"]["event_id"]
            record["selection_decision"] = (
                selection_decisions[event_id]
            )

        selected_records = [
            record
            for record in distinct_records
            if (
                record["selection_decision"]["select"]
                and record[
                    "selection_decision"
                ]["duplicate_of"] is None
            )
        ]

        selected_records.sort(
            key=lambda item: (
                item[
                    "selection_decision"
                ]["priority"],
                item["semantic_score"],
                item["ranking_score"],
            ),
            reverse=True,
        )

        selected_records = selected_records[
            :max_events
        ]

        if len(selected_records) < min_events:
            raise CommandError(
                "Совместная семантическая проверка "
                "выбрала недостаточно событий: "
                f"{len(selected_records)} < {min_events}"
            )

        self.stdout.write("")
        self.stdout.write(
            "=== FINAL SELECTED EVENTS ==="
        )

        for position, record in enumerate(
            selected_records,
            start=1,
        ):
            preview = record["preview"]
            decision = record["selection_decision"]

            self.stdout.write(
                f"{position}. {preview['event_id']} "
                f"priority={decision['priority']:.2f} "
                f"semantic="
                f"{record['semantic_score']:.2f} | "
                f"{preview['title']}"
            )
            self.stdout.write(
                f"   {decision['reason']}"
            )

        evidence_pack = {
            "schema_version": 2,
            "digest_type": "regional",
            "region": {
                "key": region,
                "code": region_code,
                "label": region_label,
                "markers": region_markers,
            },
            "topic": topic,
            "period": {
                "start": period_start.isoformat(),
                "end": period_end.isoformat(),
            },
            "events": [
                record["candidate"].evidence
                for record in selected_records
            ],
            "semantic_screening": {
                "model": resolved_model,
                "total_elapsed_ms": total_elapsed_ms,
                "thresholds": {
                    "medical_focus": (
                        min_medical_focus
                    ),
                    "regional_relevance": (
                        min_regional_relevance
                    ),
                    "source_sufficiency": (
                        min_source_sufficiency
                    ),
                },
                "screening_decisions": (
                    screening_decisions
                ),
                "deterministic_duplicates": (
                    deterministic_duplicates
                ),
                "selection_decisions": (
                    selection_decisions
                ),
                "screening_audit": (
                    screening_audit
                ),
                "selection_audit": {
                    "prompt": selection_prompt,
                    "model": selection_result.model,
                    "elapsed_ms": (
                        selection_result.elapsed_ms
                    ),
                    "raw_response": (
                        selection_result.raw
                    ),
                },
            },
            "generation_policy": {
                "external_knowledge_allowed": False,
                "each_section_requires_event_ids": True,
                "each_section_requires_source_ids": True,
                "trend_claims_allowed": False,
                "unsupported_inferences_allowed": False,
            },
        }

        selected_event_ids = [
            record["candidate"].event.id
            for record in selected_records
        ]

        criteria = {
            "pipeline": "semantic-regional-screen-v1",
            "region": region,
            "region_code": region_code,
            "region_label": region_label,
            "region_markers": region_markers,
            "topic": topic,
            "period_start_date": (
                period_start.date().isoformat()
            ),
            "period_end_date": (
                period_end.date().isoformat()
            ),
            "days": days,
            "min_events": min_events,
            "max_events": max_events,
            "max_candidates": max_candidates,
            "batch_size": batch_size,
            "max_screen_prompt_chars": (
                max_screen_prompt_chars
            ),
            "max_selection_prompt_chars": (
                max_selection_prompt_chars
            ),
            "min_article_chars": min_article_chars,
            "min_medical_focus": (
                min_medical_focus
            ),
            "min_regional_relevance": (
                min_regional_relevance
            ),
            "min_source_sufficiency": (
                min_source_sufficiency
            ),
            "duplicate_threshold": (
                duplicate_threshold
            ),
            "screening_model": resolved_model,
            "selected_event_ids": (
                selected_event_ids
            ),
        }

        group_key = hashlib.sha256(
            json.dumps(
                {
                    "pipeline": (
                        "semantic-regional-screen-v1"
                    ),
                    "region": region,
                    "topic": topic,
                    "period_start": (
                        period_start.date().isoformat()
                    ),
                    "period_end": (
                        period_end.date().isoformat()
                    ),
                    "event_ids": sorted(
                        selected_event_ids
                    ),
                    "model": resolved_model,
                },
                ensure_ascii=False,
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()

        self.stdout.write("")
        self.stdout.write(
            f"Group key: {group_key}"
        )
        self.stdout.write(
            f"Total LLM elapsed: {total_elapsed_ms} ms"
        )

        if no_save:
            self.stdout.write(
                self.style.WARNING(
                    "Model screening completed. "
                    "Database was not changed because "
                    "--no-save was used."
                )
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
                        "status": (
                            RegionalDigestStatus.READY
                        ),
                    },
                )
            )

            if not created:
                if digest.body:
                    raise CommandError(
                        "Existing digest already has body; "
                        "semantic screening will not "
                        "overwrite it"
                    )

                if digest.status not in {
                    RegionalDigestStatus.DRAFT,
                    RegionalDigestStatus.READY,
                    RegionalDigestStatus.ERROR,
                }:
                    raise CommandError(
                        "Existing digest has protected "
                        f"status: {digest.status}"
                    )

                digest.region_code = region_code
                digest.region_label = region_label
                digest.region_query = ", ".join(
                    region_markers
                )
                digest.topic = topic
                digest.period_start = period_start
                digest.period_end = period_end
                digest.criteria = criteria
                digest.evidence_pack = evidence_pack
                digest.status = (
                    RegionalDigestStatus.READY
                )
                digest.llm_error = ""

                digest.save(
                    update_fields=[
                        "region_code",
                        "region_label",
                        "region_query",
                        "topic",
                        "period_start",
                        "period_end",
                        "criteria",
                        "evidence_pack",
                        "status",
                        "llm_error",
                        "updated_at",
                    ]
                )

                digest.digest_items.all().delete()

            RegionalDigestItem.objects.bulk_create(
                [
                    RegionalDigestItem(
                        digest=digest,
                        event=record["candidate"].event,
                        position=position,
                        relevance_score=(
                            record["semantic_score"]
                        ),
                        selection_reason=(
                            record[
                                "selection_decision"
                            ]["reason"]
                        ),
                    )
                    for position, record in enumerate(
                        selected_records,
                        start=1,
                    )
                ]
            )

        action = (
            "Created"
            if created
            else "Updated"
        )

        self.stdout.write(
            self.style.SUCCESS(
                f"{action} RegionalDigest #{digest.id}"
            )
        )
        self.stdout.write(
            f"Status: {digest.status}"
        )
        self.stdout.write(
            f"Events: {digest.digest_items.count()}"
        )
        self.stdout.write(
            f"Body chars: {len(digest.body or '')}"
        )
        self.stdout.write(
            f"Published at: {digest.published_at}"
        )
