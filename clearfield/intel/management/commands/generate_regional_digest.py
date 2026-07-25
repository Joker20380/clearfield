import hashlib
import html
import json
import re
from collections import Counter
from typing import Any

from django.core.management.base import (
    BaseCommand,
    CommandError,
)
from django.db import (
    close_old_connections,
    transaction,
)
from django.utils.text import slugify

from intel.llm.ollama_client import (
    generate_with_ollama,
    parse_json_response,
)
from intel.medical_editorial_validation import (
    unsupported_claim_hits,
)
from intel.models import (
    RegionalDigest,
    RegionalDigestStatus,
)


FACT_SYSTEM_PROMPT = """
Ты — редакционный модуль извлечения доказательств.

Работай только с переданными источниками.
Не используй внешние знания.
Не делай выводов и не объясняй значение событий.
Не объединяй разные события в один факт.
Не добавляй причины, последствия, цели или эффекты,
если они прямо не указаны в источнике.

Каждый факт должен:
- быть атомарным;
- относиться только к одному event_id;
- содержать существующие source_ids;
- содержать короткую точную цитату из одного
  из указанных источников;
- сохранять числа, даты, должности и названия без изменений.

Верни только JSON.
""".strip()


ARTICLE_SYSTEM_PROMPT = """
Ты — редактор регионального медицинского дайджеста.

Тебе передаются только уже проверенные атомарные факты.
Использовать внешние знания и добавлять новые факты запрещено.

Правила:
- это дайджест нескольких разных событий, а не единая история;
- границы между событиями должны сохраняться;
- каждый текстовый блок обязан указывать fact_ids;
- нельзя добавлять новые организации, услуги, эффекты,
  рекомендации, диагнозы или статистику;
- нельзя утверждать наличие тенденции или системного улучшения;
- нельзя добавлять рекламный CTA;
- нельзя дописывать лабораторную тематику,
  если её нет в фактах;
- нельзя давать индивидуальные медицинские рекомендации;
- факты можно переформулировать, но нельзя расширять;
- заголовок и вводный абзац должны описывать формат дайджеста;
- основной объём создаётся за счёт разных событий,
  а не за счёт домыслов внутри одного события;
- создай ровно один section для каждого event_id;
- один section может использовать факты только одного event_id;
- каждый атомарный факт должен быть использован ровно
  в одном основном section;
- lead и conclusion должны перечислять только те
  fact_ids, факты которых прямо пересказаны в тексте блока;
- lead и conclusion не обязаны охватывать все события;
- нельзя упоминать событие в lead или conclusion,
  если его fact_ids не указаны в этом блоке;
- fact_ids блока должны содержать каждый факт,
  использованный в тексте этого блока;
- сохраняй время каждого утверждения:
  "будет реализован" нельзя заменять на "реализован";
- для lead используй нейтральную конструкцию
  "в подборку вошли сообщения о...";
- для conclusion используй нейтральную конструкцию
  "подборка объединяет сообщения о...";
- не утверждай, что все описанные события уже завершены;
- не называй событие инициативой, улучшением,
  модернизацией, развитием, повышением качества
  или системным изменением, если этого нет в фактах;
- не утверждай, что была оценена "качество ремонта",
  если факт говорит только об оценке работы учреждения
  после ремонта;
- не называй все источники официальными,
  если это не следует из переданных фактов;
- заголовки разделов должны буквально соответствовать
  подтверждённому событию;
- не утверждай, что подборка является исчерпывающим
  перечнем ключевых событий региона;
- период отбора можно использовать только для описания
  формата подборки, но не как утверждение, что каждое
  событие происходило на протяжении всего периода.

Верни только JSON.
""".strip()


BAD_TEXT_PATTERNS = (
    r"```",
    r"\{\{[^}]+\}\}",
    r"\[\s*(?:указать|вставить)[^\]]*\]",
    r"\bкак\s+ии\b",
    r"\bя\s+не\s+могу\b",
)


COMPOSITION_CLAIM_PATTERNS = (
    (
        r"\bулучш\w*",
        "утверждение об улучшении",
    ),
    (
        r"\bмодернизац\w*",
        "утверждение о модернизации",
    ),
    (
        r"\bповыш\w+\s+"
        r"(?:качеств\w*|уровн\w*|доступност\w*)",
        "утверждение о повышении показателей",
    ),
    (
        r"\bкачеств\w+\s+ремонт\w*",
        "оценка качества ремонта",
    ),
    (
        r"\bсистемн\w+\s+"
        r"(?:изменен\w*|улучшен\w*|развити\w*|подход\w*)",
        "системное обобщение",
    ),
)


FUTURE_FACT_PATTERNS = (
    r"\bбудет\b",
    r"\bбудут\b",
    r"\bпланиру\w*",
    r"\bпредсто\w*",
    r"\bсобира\w*",
    r"\bпредполага\w*",
    r"\bнамечен\w*",
)

FUTURE_EVENT_COMPLETION_PATTERNS = (
    (
        r"\b(?:подход\w*|модел\w*|систем\w*|"
        r"сопровождени\w*)\b"
        r".{0,100}"
        r"\b(?:реализован\w*|внедрен\w*|"
        r"запущен\w*)\b"
    ),
    (
        r"\b(?:реализован\w*|внедрен\w*|"
        r"запущен\w*)\b"
        r".{0,100}"
        r"\b(?:подход\w*|модел\w*|систем\w*|"
        r"сопровождени\w*)\b"
    ),
)


def normalize(value: object) -> str:
    text = str(value or "").lower().replace("ё", "е")
    text = re.sub(r"[‐-‒–—−]", "-", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def clip(value: object, limit: int) -> str:
    text = str(value or "").strip()

    if len(text) <= limit:
        return text

    return text[:limit].rstrip() + "…"


def compact_evidence_pack(
    digest: RegionalDigest,
) -> dict[str, Any]:
    source_pack = digest.evidence_pack or {}
    compact_events = []

    for event in source_pack.get("events") or []:
        compact_sources = []

        for source in event.get("sources") or []:
            compact_sources.append(
                {
                    "source_id": source.get("source_id"),
                    "source_name": source.get("source_name"),
                    "source_class": source.get("source_class"),
                    "published_at": source.get("published_at"),
                    "url": source.get("final_url")
                    or source.get("url"),
                    "title": clip(
                        source.get("title"),
                        500,
                    ),
                    "summary": clip(
                        source.get("summary"),
                        1200,
                    ),
                    "article_text": clip(
                        source.get("article_text"),
                        4500,
                    ),
                }
            )

        compact_events.append(
            {
                "event_id": event.get("event_id"),
                "title": clip(
                    event.get("title"),
                    600,
                ),
                "summary": clip(
                    event.get("summary"),
                    1800,
                ),
                "published_at": event.get("published_at"),
                "sources": compact_sources,
            }
        )

    return {
        "schema_version": 1,
        "region": source_pack.get("region") or {
            "label": digest.region_label,
            "code": digest.region_code,
        },
        "topic": source_pack.get("topic") or digest.topic,
        "period": source_pack.get("period") or {
            "start": digest.period_start.isoformat(),
            "end": digest.period_end.isoformat(),
        },
        "events": compact_events,
    }


def build_source_registry(
    evidence_pack: dict[str, Any],
) -> tuple[set[str], dict[str, dict[str, str]]]:
    event_ids: set[str] = set()
    sources: dict[str, dict[str, str]] = {}

    for event in evidence_pack.get("events") or []:
        event_id = str(event.get("event_id") or "").strip()

        if not event_id:
            continue

        event_ids.add(event_id)

        for source in event.get("sources") or []:
            source_id = str(
                source.get("source_id") or ""
            ).strip()

            if not source_id:
                continue

            source_text = "\n".join(
                [
                    str(source.get("title") or ""),
                    str(source.get("summary") or ""),
                    str(source.get("article_text") or ""),
                ]
            )

            sources[source_id] = {
                "event_id": event_id,
                "text": source_text,
                "url": str(source.get("url") or ""),
                "source_name": str(
                    source.get("source_name") or ""
                ),
            }

    return event_ids, sources



def evidence_fingerprint(
    evidence_pack: dict[str, Any],
) -> str:
    """
    Позволяет проверить, что сохранённые факты относятся
    именно к текущей версии evidence-pack.
    """

    payload = json.dumps(
        evidence_pack,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )

    return hashlib.sha256(
        payload.encode("utf-8")
    ).hexdigest()

def build_fact_prompt(
    evidence_pack: dict[str, Any],
) -> str:
    schema = {
        "source_sufficient": True,
        "reason": "",
        "facts": [
            {
                "fact_id": "F1",
                "event_id": "E123",
                "source_ids": ["S456"],
                "statement": (
                    "Краткий атомарный подтверждённый факт."
                ),
                "evidence_quote": (
                    "Точная короткая цитата из источника."
                ),
                "fact_type": "event",
            }
        ],
    }

    return (
        "Извлеки подтверждённые атомарные факты "
        "для регионального медицинского дайджеста.\n\n"
        "Если источников недостаточно, верни "
        'source_sufficient=false и объясни причину.\n\n'
        "Для каждого события извлеки не менее двух фактов, "
        "если источник действительно содержит два "
        "самостоятельных утверждения.\n\n"
        "evidence_quote должна дословно присутствовать "
        "в одном из source_ids этого факта.\n\n"
        "Ожидаемая схема:\n"
        + json.dumps(
            schema,
            ensure_ascii=False,
            indent=2,
        )
        + "\n\nИСТОЧНИКИ:\n"
        + json.dumps(
            evidence_pack,
            ensure_ascii=False,
            indent=2,
        )
    )


def quote_is_grounded(
    quote: str,
    source_ids: list[str],
    source_registry: dict[str, dict[str, str]],
) -> bool:
    normalized_quote = normalize(quote)

    if len(normalized_quote) < 8:
        return False

    for source_id in source_ids:
        source = source_registry.get(source_id)

        if not source:
            continue

        if normalized_quote in normalize(source["text"]):
            return True

    return False


def validate_fact_pack(
    payload: dict[str, Any],
    evidence_pack: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise CommandError(
            "Fact extraction returned non-object JSON"
        )

    if payload.get("source_sufficient") is False:
        reason = str(
            payload.get("reason")
            or "Source pack is insufficient"
        )
        raise CommandError(
            f"Source pack rejected by model: {reason}"
        )

    event_ids, source_registry = (
        build_source_registry(evidence_pack)
    )

    raw_facts = payload.get("facts")

    if not isinstance(raw_facts, list):
        raise CommandError(
            "Fact extraction returned no facts list"
        )

    validated_facts = []
    seen_fact_ids = set()
    event_counter: Counter[str] = Counter()

    for index, raw_fact in enumerate(
        raw_facts,
        start=1,
    ):
        if not isinstance(raw_fact, dict):
            raise CommandError(
                f"Fact #{index} is not an object"
            )

        fact_id = str(
            raw_fact.get("fact_id") or ""
        ).strip()

        event_id = str(
            raw_fact.get("event_id") or ""
        ).strip()

        statement = str(
            raw_fact.get("statement") or ""
        ).strip()

        evidence_quote = str(
            raw_fact.get("evidence_quote") or ""
        ).strip()

        raw_source_ids = raw_fact.get("source_ids")

        if not fact_id or fact_id in seen_fact_ids:
            raise CommandError(
                f"Invalid or duplicate fact_id: {fact_id!r}"
            )

        if not re.fullmatch(r"F[1-9]\d*", fact_id):
            raise CommandError(
                f"Invalid fact_id format: {fact_id}"
            )

        if event_id not in event_ids:
            raise CommandError(
                f"{fact_id}: unknown event_id {event_id}"
            )

        if not isinstance(raw_source_ids, list):
            raise CommandError(
                f"{fact_id}: source_ids must be a list"
            )

        source_ids = [
            str(value).strip()
            for value in raw_source_ids
            if str(value).strip()
        ]

        if not source_ids:
            raise CommandError(
                f"{fact_id}: no source_ids"
            )

        for source_id in source_ids:
            source = source_registry.get(source_id)

            if source is None:
                raise CommandError(
                    f"{fact_id}: unknown source_id "
                    f"{source_id}"
                )

            if source["event_id"] != event_id:
                raise CommandError(
                    f"{fact_id}: source {source_id} "
                    f"belongs to another event"
                )

        if len(normalize(statement)) < 20:
            raise CommandError(
                f"{fact_id}: statement is too short"
            )

        if not quote_is_grounded(
            evidence_quote,
            source_ids,
            source_registry,
        ):
            raise CommandError(
                f"{fact_id}: evidence_quote was not found "
                f"verbatim in the referenced sources"
            )

        seen_fact_ids.add(fact_id)
        event_counter[event_id] += 1

        validated_facts.append(
            {
                "fact_id": fact_id,
                "event_id": event_id,
                "source_ids": source_ids,
                "statement": statement,
                "evidence_quote": evidence_quote,
                "fact_type": str(
                    raw_fact.get("fact_type")
                    or "event"
                ),
            }
        )

    missing_events = sorted(
        event_ids - set(event_counter)
    )

    if missing_events:
        raise CommandError(
            "No extracted facts for events: "
            + ", ".join(missing_events)
        )

    if len(validated_facts) < len(event_ids) * 2:
        raise CommandError(
            "Too few grounded facts: "
            f"{len(validated_facts)} for "
            f"{len(event_ids)} events"
        )

    return {
        "source_sufficient": True,
        "facts": validated_facts,
        "fact_count": len(validated_facts),
        "event_fact_counts": dict(event_counter),
    }



SITE_SEMANTIC_CORE = [
    {
        "key": "general_laboratory",
        "label": "Базовая лабораторная диагностика",
        "always": True,
        "triggers": [],
        "phrases": [
            "лабораторная диагностика во Владикавказе",
            "медицинская лаборатория во Владикавказе",
            "сдать анализы во Владикавказе",
            "анализы в Северной Осетии",
        ],
        "patient_context": (
            "Использовать как общий контекст сайта: "
            "лабораторная диагностика помогает пациенту "
            "уточнять состояние здоровья вместе с врачом."
        ),
    },
    {
        "key": "pregnancy",
        "label": "Беременность и женское здоровье",
        "triggers": [
            "беремен",
            "роды",
            "родов",
            "материн",
            "женская консультация",
            "постановка на учет",
            "акушер",
            "гинеколог",
            "перинат",
        ],
        "phrases": [
            "анализы при беременности",
            "лабораторные исследования для беременных",
            "общий анализ крови",
            "биохимический анализ крови",
            "анализы на инфекции",
            "гормональные исследования",
            "пренатальное наблюдение",
        ],
        "patient_context": (
            "Связывать с регулярным наблюдением будущих мам, "
            "контролем показателей и исследованиями, которые "
            "может назначать врач во время беременности."
        ),
    },
    {
        "key": "prevention",
        "label": "Профилактика и контроль здоровья",
        "triggers": [
            "профилактик",
            "здоровый образ жизни",
            "диспансер",
            "обследован",
            "осмотр",
            "контроль здоровья",
            "подрост",
            "наркот",
            "зависим",
        ],
        "phrases": [
            "профилактические обследования",
            "лабораторная диагностика",
            "контроль состояния здоровья",
            "общий анализ крови",
            "биохимический анализ крови",
            "анализы для оценки общего состояния организма",
        ],
        "patient_context": (
            "Использовать как пациентское пояснение: профилактика "
            "часто связана с регулярным наблюдением, обследованиями "
            "и интерпретацией результатов врачом."
        ),
    },
    {
        "key": "infection",
        "label": "Инфекции и эпидемиологический контроль",
        "triggers": [
            "инфекц",
            "вирус",
            "бактер",
            "грипп",
            "орви",
            "корь",
            "ковид",
            "пцр",
            "санитар",
            "роспотреб",
            "вакцин",
        ],
        "phrases": [
            "анализы на инфекции",
            "ПЦР-диагностика",
            "серологические исследования",
            "лабораторное подтверждение инфекций",
            "исследования при симптомах инфекции",
        ],
        "patient_context": (
            "Связывать с диагностикой инфекций осторожно: "
            "лабораторные методы могут использоваться врачом "
            "для уточнения причины симптомов и контроля ситуации."
        ),
    },
    {
        "key": "cardio_metabolic",
        "label": "Сердечно-сосудистые и обменные риски",
        "triggers": [
            "сердц",
            "сосуд",
            "давлен",
            "инфаркт",
            "инсульт",
            "холестерин",
            "диабет",
            "сахар",
            "ожирен",
            "метабол",
        ],
        "phrases": [
            "липидный профиль",
            "контроль холестерина",
            "анализ глюкозы крови",
            "биохимический анализ крови",
            "лабораторный контроль обмена веществ",
        ],
        "patient_context": (
            "Использовать как общий контекст контроля факторов риска "
            "и регулярного наблюдения по назначению врача."
        ),
    },
    {
        "key": "endocrinology",
        "label": "Гормоны и эндокринология",
        "triggers": [
            "гормон",
            "щитовид",
            "эндокрин",
            "тиреотроп",
            "ттг",
            "сахарный диабет",
        ],
        "phrases": [
            "анализы на гормоны",
            "гормональные исследования",
            "лабораторная оценка щитовидной железы",
            "ТТГ",
            "эндокринологические анализы",
        ],
        "patient_context": (
            "Связывать с врачебным наблюдением и уточнением "
            "гормональных показателей без самостоятельных назначений."
        ),
    },
    {
        "key": "children",
        "label": "Детское здоровье",
        "triggers": [
            "дет",
            "ребен",
            "ребён",
            "школьник",
            "подрост",
            "педиатр",
            "несовершеннолет",
        ],
        "phrases": [
            "анализы для детей",
            "профилактические обследования детей",
            "лабораторная диагностика для детей",
            "общий анализ крови у детей",
        ],
        "patient_context": (
            "Использовать осторожно как контекст профилактического "
            "наблюдения детей и подростков по рекомендации врача."
        ),
    },
]


def semantic_core_text_from_digest(
    digest,
    fact_pack: dict[str, Any],
) -> str:
    chunks = [
        str(getattr(digest, "title", "") or ""),
        str(getattr(digest, "region_label", "") or ""),
        str(getattr(digest, "topic", "") or ""),
    ]

    for fact in fact_pack.get("facts") or []:
        chunks.extend(
            [
                str(fact.get("statement") or ""),
                str(fact.get("evidence_quote") or ""),
                str(fact.get("fact_type") or ""),
            ]
        )

    try:
        for item in digest.digest_items.select_related("event").all():
            event = item.event
            chunks.extend(
                [
                    str(getattr(event, "title", "") or ""),
                    str(getattr(event, "summary", "") or ""),
                    str(getattr(event, "topic", "") or ""),
                    str(getattr(event, "region", "") or ""),
                ]
            )
    except Exception:
        pass

    return "\n".join(chunks)


SEMANTIC_TRIGGER_OVERRIDES = {
    # Не активировать профилактическое ядро от любого
    # упоминания обычного осмотра или проверки помещений.
    "prevention": (
        "профилактик",
        "профилактический осмотр",
        "диспансеризац",
        "медицинский скрининг",
        "скрининговое обследование",
        "контроль состояния здоровья",
    ),

    # Слово «санитарный» само по себе не означает,
    # что материал посвящён инфекциям или ПЦР.
    "infection": (
        "инфекц",
        "вирус",
        "вич",
        "спид",
        "пцр",
        "серолог",
        "бактери",
        "заражен",
        "заражён",
    ),

    # Не использовать короткий корень «дет»:
    # он создаёт случайные совпадения внутри других слов.
    "children": (
        "ребен",
        "ребён",
        "дети",
        "детей",
        "детям",
        "детск",
        "педиатр",
        "новорожден",
        "подрост",
    ),
}


def semantic_group_triggers(
    group: dict[str, Any],
) -> tuple[str, ...]:
    key = str(group.get("key") or "")

    overridden = SEMANTIC_TRIGGER_OVERRIDES.get(
        key
    )

    if overridden is not None:
        return tuple(overridden)

    return tuple(
        group.get("triggers")
        or ()
    )


def select_site_semantic_core_from_text(
    text: str,
    *,
    max_groups: int = 3,
    max_phrases_per_group: int = 7,
) -> list[dict[str, Any]]:
    normalized_text = normalize(text)
    selected = []

    for group in SITE_SEMANTIC_CORE:
        triggers = semantic_group_triggers(
            group
        )
        is_always = bool(group.get("always"))

        if not is_always and not any(
            normalize(trigger) in normalized_text
            for trigger in triggers
        ):
            continue

        selected.append(
            {
                "key": group["key"],
                "label": group["label"],
                "phrases": list(
                    group["phrases"][:max_phrases_per_group]
                ),
                "patient_context": group["patient_context"],
                "required": not is_always,
                "selection_mode": (
                    "fallback"
                    if is_always
                    else "triggered"
                ),
            }
        )

        if len(selected) >= max_groups:
            break

    return selected


def select_site_semantic_core(
    digest,
    fact_pack: dict[str, Any],
) -> list[dict[str, Any]]:
    return select_site_semantic_core_from_text(
        semantic_core_text_from_digest(
            digest,
            fact_pack,
        )
    )


def semantic_phrase_hits(
    content: str,
    semantic_core: list[dict[str, Any]],
) -> list[str]:
    normalized_content = normalize(content)
    hits = []

    for group in semantic_core:
        for phrase in group.get("phrases") or []:
            normalized_phrase = normalize(phrase)

            if (
                normalized_phrase
                and normalized_phrase in normalized_content
                and phrase not in hits
            ):
                hits.append(phrase)

    return hits


def semantic_phrase_occurrences(
    content: str,
    semantic_core: list[dict[str, Any]],
) -> dict[str, int]:
    normalized_content = normalize(content)
    occurrences = {}

    for group in semantic_core:
        for phrase in group.get("phrases") or []:
            normalized_phrase = normalize(phrase)

            if not normalized_phrase:
                continue

            count = normalized_content.count(normalized_phrase)

            if count:
                occurrences[phrase] = count

    return occurrences


def article_facts_for_composition(
    fact_pack: dict[str, Any],
) -> list[dict[str, Any]]:
    allowed_fields = (
        "fact_id",
        "event_id",
        "source_ids",
        "statement",
        "fact_type",
    )

    return [
        {
            field: fact.get(field)
            for field in allowed_fields
            if field in fact
        }
        for fact in (
            fact_pack.get("facts")
            or []
        )
    ]


def article_fact_support_text(
    fact_pack: dict[str, Any],
) -> str:
    return "\n".join(
        str(fact.get("statement") or "")
        for fact in (
            fact_pack.get("facts")
            or []
        )
    )


def validate_meta_description_length(
    meta_description: str,
) -> None:
    length = len(
        str(meta_description or "").strip()
    )

    if length < 120:
        raise CommandError(
            "Generated meta_description is too short: "
            f"{length} < 120"
        )

    if length > 180:
        raise CommandError(
            "Generated meta_description is too long: "
            f"{length} > 180"
        )


def compact_meta_description(
    value: str,
    *,
    max_length: int = 180,
) -> str:
    text = re.sub(
        r"\s+",
        " ",
        str(value or ""),
    ).strip()

    if len(text) <= max_length:
        return text

    window = text[:max_length + 1]

    sentence_cut = max(
        window.rfind(". "),
        window.rfind("! "),
        window.rfind("? "),
    )

    if sentence_cut >= 119:
        return window[
            :sentence_cut + 1
        ].strip()

    word_cut = window.rfind(
        " ",
        119,
        max_length + 1,
    )

    if word_cut < 0:
        word_cut = window.rfind(" ")

    if word_cut < 0:
        result = window[:max_length]
    else:
        result = window[:word_cut]

    result = result.rstrip(
        " ,;:–—-"
    )

    if not result.endswith(
        (".", "!", "?")
    ):
        if len(result) >= max_length:
            result = result[
                :max_length - 1
            ].rstrip()

        result += "."

    return result[:max_length]


def normalize_single_event_sections(
    payload: dict[str, Any],
    fact_pack: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return payload

    event_ids = {
        str(fact.get("event_id") or "").strip()
        for fact in (
            fact_pack.get("facts")
            or []
        )
        if str(
            fact.get("event_id")
            or ""
        ).strip()
    }

    if len(event_ids) != 1:
        return payload

    blocks = payload.get("blocks")

    if not isinstance(blocks, list):
        return payload

    section_blocks = [
        block
        for block in blocks
        if (
            isinstance(block, dict)
            and block.get("kind") == "section"
        )
    ]

    if len(section_blocks) <= 1:
        return payload

    merged_texts = []
    merged_fact_ids = []

    for block in section_blocks:
        block_text = str(
            block.get("text")
            or ""
        ).strip()

        if (
            block_text
            and block_text not in merged_texts
        ):
            merged_texts.append(block_text)

        for fact_id in (
            block.get("fact_ids")
            or []
        ):
            fact_id = str(
                fact_id or ""
            ).strip()

            if (
                fact_id
                and fact_id
                not in merged_fact_ids
            ):
                merged_fact_ids.append(
                    fact_id
                )

    first_section = dict(
        section_blocks[0]
    )

    first_section["text"] = (
        "\n\n".join(merged_texts)
    )

    first_section["fact_ids"] = (
        merged_fact_ids
    )

    normalized_blocks = []
    section_inserted = False

    for block in blocks:
        if not isinstance(block, dict):
            normalized_blocks.append(block)
            continue

        if block.get("kind") != "section":
            normalized_blocks.append(
                dict(block)
            )
            continue

        if section_inserted:
            continue

        normalized_blocks.append(
            first_section
        )

        section_inserted = True

    normalized_payload = dict(payload)

    normalized_payload["blocks"] = (
        normalized_blocks
    )

    return normalized_payload


def build_article_prompt(
    digest: RegionalDigest,
    fact_pack: dict[str, Any],
    *,
    retry_feedback: str = "",
) -> str:
    article_input = {
        "region": {
            "code": digest.region_code,
            "label": digest.region_label,
        },
        "topic": digest.topic,
        "period": {
            "start": digest.period_start.isoformat(),
            "end": digest.period_end.isoformat(),
        },
        "facts": article_facts_for_composition(
            fact_pack
        ),
    }

    site_semantic_core = select_site_semantic_core(
        digest,
        fact_pack,
    )

    article_input["site_semantic_core"] = (
        site_semantic_core
    )

    retry_feedback = str(
        retry_feedback or ""
    ).strip()

    retry_section = ""

    if retry_feedback:
        retry_section = (
            "\n\nИСПРАВЛЕНИЕ ПРЕДЫДУЩЕЙ ПОПЫТКИ:\n"
            + retry_feedback[:2000]
            + "\nСформируй полностью новый JSON. "
            + "Исправь указанную ошибку, но не добавляй "
            + "факты, которых нет в ПРОВЕРЕННЫХ ФАКТАХ."
        )

    schema = {
        "title": (
            "Здравоохранение Северной Осетии: "
            "события периода"
        ),
        "slug": "zdravookhranenie-severnoy-osetii-obzor",
        "meta_description": (
            "В регионе зафиксированы два события в сфере "
            "здравоохранения; дайджест передаёт подтверждённые "
            "факты без оценок и неподтверждённых выводов."
        ),
        "image_topic": "healthcare_region",
        "blocks": [
            {
                "kind": "lead",
                "heading": "",
                "text": (
                    "Вводный абзац, основанный "
                    "на перечисленных фактах."
                ),
                "fact_ids": ["F1", "F3"],
            },
            {
                "kind": "section",
                "heading": "Заголовок события",
                "text": (
                    "Отдельный раздел об одном событии."
                ),
                "fact_ids": ["F1", "F2"],
            },
            {
                "kind": "conclusion",
                "heading": "Итоги",
                "text": (
                    "Осторожное фактическое завершение."
                ),
                "fact_ids": ["F1", "F3"],
            },
        ],
    }

    return (
        "Подготовь уникальный региональный медицинский "
        "дайджест только из переданных атомарных фактов. "
        "Поле statement является единственным разрешённым "
        "источником содержания статьи. Не добавляй имена, "
        "процедуры, выводы и подробности, которых нет в "
        "statement.\n\n"
        "Каждый блок обязан иметь непустой fact_ids. "
        "Количество блоков kind=section должно точно "
        "совпадать с количеством уникальных event_id. "
        "Для каждого события создай ровно один section. "
        "Если событие одно, создай ровно один section "
        "и помести в него все факты этого события. "
        "Не разделяй одно событие на несколько тематических "
        "section. Используй все события из фактов.\n\n"
        "Не вставляй source_ids и fact_ids в пользовательский "
        "текст: они нужны только в JSON для проверки.\n\n"
        "Поле meta_description обязательно должно быть "
        "уникальным, фактическим и содержать от 120 до 180 "
        "символов с пробелами. Не копируй текст-пример из "
        "схемы. Не используй общие заглушки вроде «краткое "
        "описание регионального дайджеста».\n\n"
        "СЕМАНТИЧЕСКОЕ ЯДРО САЙТА:\n"
        "В поле site_semantic_core переданы разрешённые фразы "
        "семантического ядра лабораторного сайта. У каждой "
        "группы есть поле required. Для группы с required=true "
        "естественно используй хотя бы одну подходящую фразу. "
        "Для группы с required=false фразы необязательны: "
        "используй их только при прямой связи с фактами. "
        "Не вставляй лабораторную фразу только ради SEO. "
        "Всего используй не более 2-4 релевантных фраз. "
        "Не добавляй нерелевантные анализы. Не утверждай, что "
        "пациент обязан сдать конкретный анализ. "
        "Не формулируй неподтверждённые ожидаемые эффекты: "
        "не пиши, что меры улучшат, повысят, обеспечат, "
        "ускорят или приведут к повышению качества. "
        "В title, meta_description и blocks вообще не используй "
        "слова и словоформы с корнями «улучш», «повыш», "
        "«обеспеч», «ускор» и «эффектив». Не добавляй выводы "
        "о пользе, последствиях, результативности и ожидаемом "
        "влиянии описанных событий. Пациентский контекст "
        "разрешён только как нейтральное напоминание о том, "
        "что выбор обследования и интерпретацию результатов "
        "выполняет врач. Не связывай события с предполагаемой "
        "пользой, доступностью или качеством медицинской помощи. "
        "Не используй рекламные обещания "
        "и не набивай текст повторяющимися ключами.\n\n"
        "Ожидаемая схема:\n"
        + json.dumps(
            schema,
            ensure_ascii=False,
            indent=2,
        )
        + "\n\nПРОВЕРЕННЫЕ ФАКТЫ:\n"
        + json.dumps(
            article_input,
            ensure_ascii=False,
            indent=2,
        )
        + retry_section
    )


def source_display_name(
    source: dict[str, Any],
) -> str:
    return str(
        source.get("source_name")
        or source.get("title")
        or source.get("source_id")
        or "источник"
    ).strip()


def source_external_url(
    source: dict[str, Any],
) -> str:
    for key in (
        "final_url",
        "url",
        "source_url",
        "canonical_url",
    ):
        value = str(source.get(key) or "").strip()

        if value.startswith(("http://", "https://")):
            return value

    return ""


def build_source_links_for_fact_ids(
    fact_ids: list[str],
    facts_by_id: dict[str, dict[str, Any]],
    source_registry: dict[str, dict[str, Any]],
) -> list[dict[str, str]]:
    links = []
    seen = set()

    for fact_id in fact_ids:
        fact = facts_by_id.get(fact_id) or {}

        for source_id in fact.get("source_ids") or []:
            source = source_registry.get(str(source_id)) or {}
            url = source_external_url(source)

            if not url:
                continue

            label = source_display_name(source)
            key = (url, label)

            if key in seen:
                continue

            seen.add(key)

            links.append(
                {
                    "source_id": str(source_id),
                    "label": label,
                    "url": url,
                }
            )

    return links


def build_fact_source_link_map(
    facts: list[dict[str, Any]],
    source_registry: dict[str, dict[str, Any]],
) -> dict[str, list[dict[str, str]]]:
    facts_by_id = {
        str(fact.get("fact_id") or "").strip(): fact
        for fact in facts
        if str(fact.get("fact_id") or "").strip()
    }

    return {
        fact_id: build_source_links_for_fact_ids(
            [fact_id],
            facts_by_id,
            source_registry,
        )
        for fact_id in facts_by_id
    }


def append_source_links(
    text: str,
    links: list[dict[str, str]],
) -> str:
    if not links:
        return text

    anchors = []

    for link in links:
        label = html.escape(
            link.get("label") or "источник",
            quote=True,
        )
        url = html.escape(
            link.get("url") or "",
            quote=True,
        )

        if not url:
            continue

        anchors.append(
            '<a href="'
            + url
            + '" target="_blank" '
            + 'rel="noopener noreferrer">'
            + label
            + '</a>'
        )

    if not anchors:
        return text

    prefix = "Источник" if len(anchors) == 1 else "Источники"

    return (
        text.rstrip()
        + " "
        + '<span class="regional-source-link">'
        + prefix
        + ": "
        + ", ".join(anchors)
        + ".</span>"
    )


def render_body(
    blocks: list[dict[str, Any]],
    *,
    facts: list[dict[str, Any]] | None = None,
    source_registry: dict[str, dict[str, Any]] | None = None,
    include_source_links: bool = False,
) -> str:
    parts = []

    facts_by_id = {
        str(fact.get("fact_id") or "").strip(): fact
        for fact in (facts or [])
        if str(fact.get("fact_id") or "").strip()
    }

    source_registry = source_registry or {}

    for block in blocks:
        kind = block["kind"]
        heading = block["heading"]
        text = block["text"].strip()

        if include_source_links:
            text = append_source_links(
                text,
                build_source_links_for_fact_ids(
                    block.get("fact_ids") or [],
                    facts_by_id,
                    source_registry,
                ),
            )

        if kind == "lead":
            parts.append(text)
            continue

        if heading:
            parts.append(f"## {heading}\n\n{text}")
        else:
            parts.append(text)

    return "\n\n".join(parts).strip()


def all_source_text(
    evidence_pack: dict[str, Any],
) -> str:
    chunks = []

    for event in evidence_pack.get("events") or []:
        chunks.extend(
            [
                str(event.get("title") or ""),
                str(event.get("summary") or ""),
            ]
        )

        for source in event.get("sources") or []:
            chunks.extend(
                [
                    str(source.get("title") or ""),
                    str(source.get("summary") or ""),
                    str(source.get("article_text") or ""),
                ]
            )

    return "\n".join(chunks)


COMPOSITION_PROCEDURAL_CLAIM_PATTERNS = (
    (
        r"\bжалоб\w*",
        "сведения о жалобах",
    ),
    (
        r"\bадвокат\w*",
        "сведения об адвокатах",
    ),
    (
        r"\bродствен\w*",
        "сведения о родственниках",
    ),
    (
        r"\b(?:прав\w*\s+)?удерживаем\w*",
        "сведения о правах удерживаемых лиц",
    ),
    (
        r"\bсоответств\w*\s+"
        r"(?:требован\w*|законодательств\w*)",
        "утверждение о соответствии требованиям",
    ),
    (
        r"\bоптимизац\w*",
        "утверждение об оптимизации",
    ),
    (
        r"\bмобильн\w*\s+узл\w*\s+связ\w*",
        "сведения о мобильном узле связи",
    ),
    (
        r"\bбеспилот\w*",
        "сведения о беспилотных системах",
    ),
    (
        r"\bпресс[- ]центр\w*",
        "сведения о пресс-центре",
    ),
    (
        r"\bрадиационно[- ]хим\w*",
        "сведения о радиационно-химическом наблюдении",
    ),
)


def validate_article_payload(
    payload: dict[str, Any],
    fact_pack: dict[str, Any],
    evidence_pack: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise CommandError(
            "Article generation returned non-object JSON"
        )

    title = str(payload.get("title") or "").strip()
    meta_description = str(
        payload.get("meta_description") or ""
    ).strip()
    image_topic = str(
        payload.get("image_topic")
        or "healthcare_region"
    ).strip()

    raw_blocks = payload.get("blocks")

    if len(normalize(title)) < 30:
        raise CommandError(
            "Generated digest title is too short"
        )

    validate_meta_description_length(
        meta_description
    )

    if not isinstance(raw_blocks, list):
        raise CommandError(
            "Generated digest has no blocks list"
        )

    raw_fact_event_ids = {
        str(fact.get("event_id") or "").strip()
        for fact in (fact_pack.get("facts") or [])
        if str(fact.get("event_id") or "").strip()
    }

    min_blocks = 3 if len(raw_fact_event_ids) <= 1 else 4

    if not min_blocks <= len(raw_blocks) <= 10:
        raise CommandError(
            "Digest must contain between "
            f"{min_blocks} and 10 blocks"
        )

    facts = {
        fact["fact_id"]: fact
        for fact in fact_pack["facts"]
    }

    validated_blocks = []
    used_fact_ids = set()
    seen_texts = set()

    for index, raw_block in enumerate(
        raw_blocks,
        start=1,
    ):
        if not isinstance(raw_block, dict):
            raise CommandError(
                f"Block #{index} is not an object"
            )

        kind = str(
            raw_block.get("kind") or ""
        ).strip()

        heading = str(
            raw_block.get("heading") or ""
        ).strip()

        text = str(
            raw_block.get("text") or ""
        ).strip()

        raw_fact_ids = raw_block.get("fact_ids")

        if kind not in {
            "lead",
            "section",
            "conclusion",
        }:
            raise CommandError(
                f"Block #{index}: invalid kind {kind!r}"
            )

        if kind == "section" and len(normalize(heading)) < 5:
            raise CommandError(
                f"Block #{index}: section heading is too short"
            )

        if len(normalize(text)) < 80:
            raise CommandError(
                f"Block #{index}: text is too short"
            )

        if len(text) > 2200:
            raise CommandError(
                f"Block #{index}: text is too long"
            )

        if not isinstance(raw_fact_ids, list):
            raise CommandError(
                f"Block #{index}: fact_ids must be a list"
            )

        fact_ids = [
            str(value).strip()
            for value in raw_fact_ids
            if str(value).strip()
        ]

        if not fact_ids:
            raise CommandError(
                f"Block #{index}: no fact_ids"
            )

        unknown_fact_ids = [
            fact_id
            for fact_id in fact_ids
            if fact_id not in facts
        ]

        if unknown_fact_ids:
            raise CommandError(
                f"Block #{index}: unknown fact_ids: "
                + ", ".join(unknown_fact_ids)
            )

        referenced_fact_text = "\n".join(
            [
                str(
                    facts[fact_id].get(
                        "statement"
                    )
                    or ""
                )
                + "\n"
                + str(
                    facts[fact_id].get(
                        "evidence_quote"
                    )
                    or ""
                )
                for fact_id in fact_ids
            ]
        )

        references_future_fact = any(
            re.search(
                pattern,
                referenced_fact_text,
                flags=re.I | re.M,
            )
            for pattern in FUTURE_FACT_PATTERNS
        )

        block_uses_future_wording = any(
            re.search(
                pattern,
                text,
                flags=re.I | re.M,
            )
            for pattern in FUTURE_FACT_PATTERNS
        )

        block_claims_completed_future_event = any(
            re.search(
                pattern,
                text,
                flags=re.I | re.M | re.S,
            )
            for pattern in (
                FUTURE_EVENT_COMPLETION_PATTERNS
            )
        )

        if (
            references_future_fact
            and block_claims_completed_future_event
            and not block_uses_future_wording
        ):
            raise CommandError(
                f"Block #{index}: a future event "
                "was changed into a completed event"
            )

        normalized_text = normalize(text)

        if normalized_text in seen_texts:
            raise CommandError(
                f"Block #{index}: duplicate text"
            )

        if any(
            re.search(
                pattern,
                text,
                flags=re.I | re.M,
            )
            for pattern in BAD_TEXT_PATTERNS
        ):
            raise CommandError(
                f"Block #{index}: bad LLM artifact"
            )

        seen_texts.add(normalized_text)
        used_fact_ids.update(fact_ids)

        validated_blocks.append(
            {
                "kind": kind,
                "heading": heading,
                "text": text,
                "fact_ids": fact_ids,
            }
        )

    fact_event_map = {
        fact_id: fact["event_id"]
        for fact_id, fact in facts.items()
    }

    all_event_ids = {
        fact["event_id"]
        for fact in facts.values()
    }

    all_fact_ids = set(facts)
    section_event_ids: list[str] = []
    section_fact_ids: list[str] = []

    for block_index, block in enumerate(
        validated_blocks,
        start=1,
    ):
        block_fact_ids = block["fact_ids"]

        block_event_ids = {
            fact_event_map[fact_id]
            for fact_id in block_fact_ids
        }

        if block["kind"] == "section":
            if len(block_event_ids) != 1:
                raise CommandError(
                    f"Block #{block_index}: one section "
                    "must describe exactly one event"
                )

            section_event_ids.extend(
                block_event_ids
            )
            section_fact_ids.extend(
                block_fact_ids
            )

        elif block["kind"] in {
            "lead",
            "conclusion",
        }:
            if not block_event_ids:
                raise CommandError(
                    f"Block #{block_index}: "
                    f"{block['kind']} must reference "
                    "at least one grounded event"
                )

            if len(block_fact_ids) != len(
                set(block_fact_ids)
            ):
                raise CommandError(
                    f"Block #{block_index}: duplicate "
                    "fact_ids are not allowed"
                )

    if len(section_event_ids) != len(all_event_ids):
        raise CommandError(
            "Digest must contain exactly one section "
            "for every event"
        )

    if set(section_event_ids) != all_event_ids:
        raise CommandError(
            "Section event coverage does not match "
            "the grounded fact-pack"
        )

    if len(section_event_ids) != len(
        set(section_event_ids)
    ):
        raise CommandError(
            "More than one section was generated "
            "for the same event"
        )

    if len(section_fact_ids) != len(
        set(section_fact_ids)
    ):
        raise CommandError(
            "A grounded fact was used in more than "
            "one main section"
        )

    if set(section_fact_ids) != all_fact_ids:
        raise CommandError(
            "Main sections must use every grounded "
            "fact exactly once"
        )

    used_event_ids = {
        fact_event_map[fact_id]
        for fact_id in used_fact_ids
    }

    missing_event_ids = sorted(
        all_event_ids - used_event_ids
    )

    if missing_event_ids:
        raise CommandError(
            "Generated digest did not use events: "
            + ", ".join(missing_event_ids)
        )

    body = render_body(validated_blocks)

    all_fact_support_text = "\n".join(
        [
            str(fact.get("statement") or "")
            + "\n"
            + str(fact.get("evidence_quote") or "")
            for fact in fact_pack["facts"]
        ]
    )

    fact_pack_contains_future_event = any(
        re.search(
            pattern,
            all_fact_support_text,
            flags=re.I | re.M,
        )
        for pattern in FUTURE_FACT_PATTERNS
    )

    summary_text = "\n".join(
        [
            title,
            meta_description,
        ]
    )

    summary_claims_completed_future_event = any(
        re.search(
            pattern,
            summary_text,
            flags=re.I | re.M | re.S,
        )
        for pattern in (
            FUTURE_EVENT_COMPLETION_PATTERNS
        )
    )

    summary_uses_future_wording = any(
        re.search(
            pattern,
            summary_text,
            flags=re.I | re.M,
        )
        for pattern in FUTURE_FACT_PATTERNS
    )

    if (
        fact_pack_contains_future_event
        and summary_claims_completed_future_event
        and not summary_uses_future_wording
    ):
        raise CommandError(
            "Title or meta_description changed "
            "a future event into a completed event"
        )

    fact_support_text = (
        article_fact_support_text(
            fact_pack
        )
    )

    composition_text = "\n".join(
        [
            title,
            meta_description,
            body,
        ]
    )

    for pattern, description in (
        *COMPOSITION_CLAIM_PATTERNS,
        *COMPOSITION_PROCEDURAL_CLAIM_PATTERNS,
    ):
        if (
            re.search(
                pattern,
                composition_text,
                flags=re.I | re.M,
            )
            and not re.search(
                pattern,
                fact_support_text,
                flags=re.I | re.M,
            )
        ):
            raise CommandError(
                "Generated digest contains unsupported "
                f"{description}"
            )

    unsupported = unsupported_claim_hits(
        generated_text=f"{title}\n{body}",
        source_text=fact_support_text,
    )

    if unsupported:
        raise CommandError(
            "Generated digest contains unsupported claims: "
            + ", ".join(unsupported)
        )

    _, source_registry_for_links = build_source_registry(
        evidence_pack
    )

    body = render_body(
        validated_blocks,
        facts=fact_pack["facts"],
        source_registry=source_registry_for_links,
        include_source_links=True,
    )

    site_semantic_core = select_site_semantic_core_from_text(
        "\n".join(
            [
                all_source_text(evidence_pack),
                "\n".join(
                    str(fact.get("statement") or "")
                    for fact in fact_pack.get("facts") or []
                ),
                "\n".join(
                    str(fact.get("evidence_quote") or "")
                    for fact in fact_pack.get("facts") or []
                ),
            ]
        )
    )

    used_site_semantic_phrases = semantic_phrase_hits(
        body,
        site_semantic_core,
    )

    required_site_semantic_core = [
        group
        for group in site_semantic_core
        if group.get("required", True)
    ]

    used_required_site_semantic_phrases = (
        semantic_phrase_hits(
            body,
            required_site_semantic_core,
        )
    )

    if (
        required_site_semantic_core
        and not used_required_site_semantic_phrases
    ):
        raise CommandError(
            "Generated digest did not use any required "
            "site semantic core phrase"
        )

    semantic_occurrences = semantic_phrase_occurrences(
        body,
        site_semantic_core,
    )

    repeated_phrases = [
        phrase
        for phrase, count in semantic_occurrences.items()
        if count > 2
    ]

    if repeated_phrases:
        raise CommandError(
            "Generated digest repeats semantic core phrases "
            "too often: "
            + ", ".join(repeated_phrases)
        )

    if sum(semantic_occurrences.values()) > 10:
        raise CommandError(
            "Generated digest looks like keyword stuffing: "
            "too many semantic core phrase occurrences"
        )

    raw_slug = str(payload.get("slug") or "").strip()
    slug = slugify(raw_slug or title)

    if not slug:
        raise CommandError(
            "Could not build digest slug"
        )

    source_map = {
        "facts": fact_pack["facts"],
        "source_links": build_fact_source_link_map(
            fact_pack["facts"],
            source_registry_for_links,
        ),
        "blocks": [
            {
                "kind": block["kind"],
                "heading": block["heading"],
                "fact_ids": block["fact_ids"],
            }
            for block in validated_blocks
        ],
        "used_fact_ids": sorted(used_fact_ids),
        "used_event_ids": sorted(used_event_ids),
        "site_semantic_core": site_semantic_core,
        "used_site_semantic_phrases": used_site_semantic_phrases,
        "required_site_semantic_core": (
            required_site_semantic_core
        ),
        "used_required_site_semantic_phrases": (
            used_required_site_semantic_phrases
        ),
    }

    return {
        "title": title,
        "slug": slug,
        "meta_description": meta_description,
        "image_topic": image_topic,
        "body": body,
        "blocks": validated_blocks,
        "source_map": source_map,
    }


class Command(BaseCommand):
    help = (
        "Генерирует grounded региональный дайджест "
        "в два этапа: facts -> article. "
        "Без --execute модель не вызывается."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--digest-id",
            type=int,
            required=True,
        )
        parser.add_argument(
            "--model",
            default="",
        )
        parser.add_argument(
            "--retry-feedback",
            default="",
            help=(
                "Ошибка предыдущей попытки композиции, "
                "которую модель должна исправить."
            ),
        )
        parser.add_argument(
            "--execute",
            action="store_true",
            help=(
                "Реально вызвать модель. "
                "Без флага выполняется только dry-run."
            ),
        )
        parser.add_argument(
            "--facts-only",
            action="store_true",
            help=(
                "Извлечь и проверить атомарные факты, "
                "но не создавать текст дайджеста."
            ),
        )
        parser.add_argument(
            "--compose-only",
            action="store_true",
            help=(
                "Создать текст из ранее сохранённого "
                "grounded fact-pack без повторного "
                "извлечения фактов."
            ),
        )
        parser.add_argument(
            "--replace-review",
            action="store_true",
            help=(
                "Разрешить заменить существующий текст "
                "дайджеста со статусом review. "
                "Используется только с --compose-only."
            ),
        )
        parser.add_argument(
            "--show-prompt",
            action="store_true",
        )

    def handle(self, *args, **options):
        digest_id = options["digest_id"]
        execute = options["execute"]
        facts_only = bool(options["facts_only"])
        compose_only = bool(options["compose_only"])
        replace_review = bool(
            options["replace_review"]
        )
        selected_model = options["model"] or None

        if facts_only and not execute:
            raise CommandError(
                "--facts-only requires --execute"
            )

        if compose_only and not execute:
            raise CommandError(
                "--compose-only requires --execute"
            )

        if facts_only and compose_only:
            raise CommandError(
                "--facts-only and --compose-only "
                "cannot be used together"
            )

        if replace_review and not compose_only:
            raise CommandError(
                "--replace-review requires --compose-only"
            )

        if replace_review and not compose_only:
            raise CommandError(
                "--replace-review requires --compose-only"
            )

        try:
            digest = RegionalDigest.objects.get(
                pk=digest_id
            )
        except RegionalDigest.DoesNotExist as exc:
            raise CommandError(
                f"RegionalDigest #{digest_id} not found"
            ) from exc

        evidence_pack = compact_evidence_pack(digest)

        event_ids, source_registry = (
            build_source_registry(evidence_pack)
        )

        if len(event_ids) < 1:
            raise CommandError(
                "At least one distinct event is required"
            )

        if len(source_registry) < 1:
            raise CommandError(
                "At least one source is required"
            )

        fact_prompt = build_fact_prompt(
            evidence_pack
        )

        self.stdout.write(
            f"RegionalDigest #{digest.id}"
        )
        self.stdout.write(
            f"Status: {digest.status}"
        )
        self.stdout.write(
            f"Events: {len(event_ids)}"
        )
        self.stdout.write(
            f"Sources: {len(source_registry)}"
        )
        self.stdout.write(
            f"Evidence chars: "
            f"{len(json.dumps(evidence_pack, ensure_ascii=False))}"
        )
        self.stdout.write(
            f"Fact prompt chars: {len(fact_prompt)}"
        )

        for event in evidence_pack["events"]:
            self.stdout.write(
                f"- {event['event_id']}: "
                f"{event['title'][:140]}"
            )

        if options["show_prompt"]:
            self.stdout.write("")
            self.stdout.write(
                "=== FACT EXTRACTION PROMPT PREVIEW ==="
            )
            self.stdout.write(
                fact_prompt[:7000]
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

        allowed_statuses = {
            RegionalDigestStatus.READY,
            RegionalDigestStatus.ERROR,
        }

        if compose_only and replace_review:
            allowed_statuses.add(
                RegionalDigestStatus.REVIEW
            )

        if digest.status not in allowed_statuses:
            raise CommandError(
                "Digest status is not allowed for this "
                f"operation: {digest.status}"
            )

        if digest.body and not replace_review:
            raise CommandError(
                "Digest already has generated body. "
                "Use --compose-only --replace-review "
                "for an explicit replacement."
            )

        original_digest_status = digest.status
        original_digest_had_body = bool(digest.body)

        total_elapsed_ms = 0
        article_prompt = ""
        article_result = None
        raw_article_payload = None

        try:
            current_fingerprint = evidence_fingerprint(
                evidence_pack
            )

            if compose_only:
                existing_source_map = (
                    digest.source_map
                    if isinstance(
                        digest.source_map,
                        dict,
                    )
                    else {}
                )

                stored_fact_pack = (
                    existing_source_map.get(
                        "grounded_fact_pack"
                    )
                )

                stored_fact_meta = (
                    existing_source_map.get(
                        "grounded_fact_pack_meta"
                    )
                    or {}
                )

                if not isinstance(
                    stored_fact_pack,
                    dict,
                ):
                    raise CommandError(
                        "Stored grounded fact-pack "
                        "was not found"
                    )

                stored_fingerprint = (
                    stored_fact_meta.get(
                        "evidence_fingerprint"
                    )
                )

                if (
                    stored_fingerprint
                    != current_fingerprint
                ):
                    raise CommandError(
                        "Stored fact-pack belongs to "
                        "another evidence-pack version"
                    )

                fact_pack = validate_fact_pack(
                    stored_fact_pack,
                    evidence_pack,
                )

                fact_model = str(
                    stored_fact_meta.get("model")
                    or digest.llm_model
                    or ""
                )

                fact_stage_audit = {
                    "stage": "reused-stored-facts",
                    "model": fact_model,
                    "elapsed_ms": 0,
                    "evidence_fingerprint": (
                        current_fingerprint
                    ),
                    "validated_fact_pack": (
                        fact_pack
                    ),
                }

                self.stdout.write(
                    self.style.SUCCESS(
                        "Stored grounded fact-pack "
                        "validated and reused"
                    )
                )
                self.stdout.write(
                    f"Stored facts: "
                    f"{fact_pack['fact_count']}"
                )

            else:
                close_old_connections()

                fact_result = generate_with_ollama(
                    prompt=fact_prompt,
                    system=FACT_SYSTEM_PROMPT,
                    json_mode=True,
                    model=selected_model,
                )

                total_elapsed_ms += (
                    fact_result.elapsed_ms
                )

                raw_fact_payload = (
                    parse_json_response(
                        fact_result.text
                    )
                )

                fact_pack = validate_fact_pack(
                    raw_fact_payload,
                    evidence_pack,
                )

                fact_model = fact_result.model

                fact_stage_audit = {
                    "stage": "fact-extraction",
                    "model": fact_result.model,
                    "elapsed_ms": (
                        fact_result.elapsed_ms
                    ),
                    "evidence_fingerprint": (
                        current_fingerprint
                    ),
                    "raw_response": (
                        fact_result.raw
                    ),
                    "validated_fact_pack": (
                        fact_pack
                    ),
                }

            if facts_only:
                close_old_connections()

                fingerprint = evidence_fingerprint(
                    evidence_pack
                )

                fact_audit = {
                    "stage": "fact-extraction",
                    "model": fact_result.model,
                    "elapsed_ms": fact_result.elapsed_ms,
                    "evidence_fingerprint": fingerprint,
                    "raw_response": fact_result.raw,
                    "validated_fact_pack": fact_pack,
                }

                with transaction.atomic():
                    digest = (
                        RegionalDigest.objects
                        .select_for_update()
                        .get(pk=digest.id)
                    )

                    if digest.body:
                        raise CommandError(
                            "Digest already has generated body"
                        )

                    existing_source_map = (
                        digest.source_map
                        if isinstance(
                            digest.source_map,
                            dict,
                        )
                        else {}
                    )

                    source_map = dict(
                        existing_source_map
                    )

                    source_map[
                        "grounded_fact_pack"
                    ] = fact_pack

                    source_map[
                        "grounded_fact_pack_meta"
                    ] = {
                        "schema_version": 1,
                        "model": fact_result.model,
                        "elapsed_ms": (
                            fact_result.elapsed_ms
                        ),
                        "evidence_fingerprint": (
                            fingerprint
                        ),
                    }

                    digest.source_map = source_map
                    digest.status = (
                        RegionalDigestStatus.READY
                    )
                    digest.llm_model = fact_result.model
                    digest.llm_prompt = fact_prompt
                    digest.llm_response_raw = (
                        json.dumps(
                            fact_audit,
                            ensure_ascii=False,
                            indent=2,
                        )
                    )
                    digest.llm_elapsed_ms = (
                        fact_result.elapsed_ms
                    )
                    digest.llm_error = ""

                    digest.save(
                        update_fields=[
                            "source_map",
                            "status",
                            "llm_model",
                            "llm_prompt",
                            "llm_response_raw",
                            "llm_elapsed_ms",
                            "llm_error",
                            "updated_at",
                        ]
                    )

                self.stdout.write(
                    self.style.SUCCESS(
                        "Facts-only extraction completed "
                        f"for RegionalDigest #{digest.id}"
                    )
                )
                self.stdout.write(
                    f"Status: {digest.status}"
                )
                self.stdout.write(
                    f"Facts: {fact_pack['fact_count']}"
                )
                self.stdout.write(
                    "Evidence fingerprint: "
                    f"{fingerprint}"
                )

                for event_id, count in sorted(
                    fact_pack[
                        "event_fact_counts"
                    ].items()
                ):
                    self.stdout.write(
                        f"- {event_id}: {count} facts"
                    )

                self.stdout.write(
                    f"Body chars: "
                    f"{len(digest.body or '')}"
                )
                self.stdout.write(
                    f"Published at: "
                    f"{digest.published_at}"
                )
                return

            article_prompt = build_article_prompt(
                digest,
                fact_pack,
                retry_feedback=options.get(
                    "retry_feedback",
                    "",
                ),
            )

            close_old_connections()

            article_result = generate_with_ollama(
                prompt=article_prompt,
                system=ARTICLE_SYSTEM_PROMPT,
                json_mode=True,
                model=selected_model,
            )

            total_elapsed_ms += article_result.elapsed_ms

            raw_article_payload = parse_json_response(
                article_result.text
            )

            if not isinstance(
                raw_article_payload,
                dict,
            ):
                raise CommandError(
                    "Article response must be "
                    "a JSON object"
                )

            original_meta_description = str(
                raw_article_payload.get(
                    "meta_description"
                )
                or ""
            ).strip()

            compacted_meta_description = (
                compact_meta_description(
                    original_meta_description
                )
            )

            if (
                compacted_meta_description
                != original_meta_description
            ):
                raw_article_payload = dict(
                    raw_article_payload
                )

                raw_article_payload[
                    "meta_description"
                ] = compacted_meta_description


            raw_article_payload = (
                normalize_single_event_sections(
                    raw_article_payload,
                    fact_pack,
                )
            )

            article = validate_article_payload(
                raw_article_payload,
                fact_pack,
                evidence_pack,
            )

            close_old_connections()

            response_audit = {
                "fact_extraction": (
                    fact_stage_audit
                ),
                "article_generation": {
                    "model": article_result.model,
                    "elapsed_ms": article_result.elapsed_ms,
                    "response": article_result.raw,
                    "validated_blocks": article["blocks"],
                },
            }

            with transaction.atomic():
                digest = RegionalDigest.objects.select_for_update().get(
                    pk=digest.id
                )

                if digest.body and not replace_review:
                    raise CommandError(
                        "Digest body changed during composition"
                    )

                existing_source_map = (
                    digest.source_map
                    if isinstance(
                        digest.source_map,
                        dict,
                    )
                    else {}
                )

                final_source_map = dict(
                    existing_source_map
                )

                final_source_map.update(
                    article["source_map"]
                )

                final_source_map[
                    "grounded_fact_pack"
                ] = fact_pack

                fact_meta = dict(
                    final_source_map.get(
                        "grounded_fact_pack_meta"
                    )
                    or {}
                )

                fact_meta.update(
                    {
                        "schema_version": 1,
                        "model": fact_model,
                        "evidence_fingerprint": (
                            current_fingerprint
                        ),
                    }
                )

                final_source_map[
                    "grounded_fact_pack_meta"
                ] = fact_meta

                digest.title = article["title"]
                digest.slug = article["slug"]
                digest.meta_description = (
                    article["meta_description"]
                )
                digest.body = article["body"]
                digest.source_map = final_source_map
                digest.status = RegionalDigestStatus.REVIEW
                digest.llm_model = article_result.model
                if compose_only:
                    digest.llm_prompt = (
                        "=== STORED GROUNDED FACT PACK ===\n\n"
                        + json.dumps(
                            fact_pack,
                            ensure_ascii=False,
                            indent=2,
                        )
                        + "\n\n"
                        + "=== ARTICLE COMPOSITION ===\n\n"
                        + article_prompt
                    )
                else:
                    digest.llm_prompt = (
                        fact_prompt
                        + "\n\n"
                        + "=== ARTICLE COMPOSITION ===\n\n"
                        + article_prompt
                    )
                digest.llm_response_raw = json.dumps(
                    response_audit,
                    ensure_ascii=False,
                    indent=2,
                )
                digest.llm_elapsed_ms = total_elapsed_ms
                digest.llm_error = ""

                digest.save(
                    update_fields=[
                        "title",
                        "slug",
                        "meta_description",
                        "body",
                        "source_map",
                        "status",
                        "llm_model",
                        "llm_prompt",
                        "llm_response_raw",
                        "llm_elapsed_ms",
                        "llm_error",
                        "updated_at",
                    ]
                )

            self.stdout.write(
                self.style.SUCCESS(
                    f"Generated RegionalDigest #{digest.id} "
                    f"in {total_elapsed_ms} ms"
                )
            )
            self.stdout.write(
                f"Status: {digest.status}"
            )
            self.stdout.write(
                f"Facts: {fact_pack['fact_count']}"
            )
            self.stdout.write(
                f"Body chars: {len(digest.body)}"
            )

        except Exception as exc:
            close_old_connections()

            failure_update = {
                "llm_error": str(exc)[:8000],
                "llm_elapsed_ms": (
                    total_elapsed_ms or None
                ),
            }

            if article_result is not None:
                failure_audit = {
                    "fact_extraction": fact_stage_audit,
                    "article_generation": {
                        "stage": "validation-failed",
                        "validation_error": str(exc),
                        "model": article_result.model,
                        "elapsed_ms": (
                            article_result.elapsed_ms
                        ),
                        "response": article_result.raw,
                        "parsed_payload": (
                            raw_article_payload
                        ),
                    },
                }

                failure_update[
                    "llm_response_raw"
                ] = json.dumps(
                    failure_audit,
                    ensure_ascii=False,
                    indent=2,
                    default=str,
                )

                failure_update[
                    "llm_model"
                ] = article_result.model

                if article_prompt:
                    failure_update[
                        "llm_prompt"
                    ] = (
                        "=== STORED GROUNDED "
                        "FACT PACK ===\n\n"
                        + json.dumps(
                            fact_pack,
                            ensure_ascii=False,
                            indent=2,
                        )
                        + "\n\n"
                        + "=== ARTICLE "
                        "COMPOSITION ===\n\n"
                        + article_prompt
                    )

            preserve_existing_review = bool(
                compose_only
                and replace_review
                and original_digest_status
                == RegionalDigestStatus.REVIEW
                and original_digest_had_body
            )

            if preserve_existing_review:
                failure_update["status"] = (
                    RegionalDigestStatus.REVIEW
                )
            else:
                failure_update["status"] = (
                    RegionalDigestStatus.ERROR
                )

            RegionalDigest.objects.filter(
                pk=digest.id
            ).update(**failure_update)

            raise CommandError(
                f"Regional digest generation failed: {exc}"
            ) from exc
