from __future__ import annotations

import json
import re
from typing import Any

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import close_old_connections, transaction
from django.utils import timezone
from django.utils.text import slugify
from django.db.models.functions import TruncDate

from intel.llm.ollama_client import (
    OllamaError,
    generate_with_ollama,
    parse_json_response,
)
from intel.medical_editorial_validation import unsupported_claim_hits

from intel.models import (
    GeneratedMedicalNews,
    MedicalBrief,
    MedicalBriefStatus,
    MedicalNewsStatus,
)


IMAGE_TOPICS = (
    "laboratory_diagnostics",
    "prevention_health",
    "medical_technologies",
    "medical_science",
    "education_staff",
    "affordable_medicine",
    "healthy_lifestyle",
    "healthcare_region",
    "north_ossetia_news",
    "emergency_care",
    "pharma",
    "cardiology",
    "oncology",
    "diabetes",
    "immunity_infections",
    "rehabilitation",
    "maternal_child_health",
    "general_medical_news",
)

DEFAULT_IMAGE_TOPIC = "general_medical_news"


SYSTEM_PROMPT = """
Ты медицинский редактор информационного медицинского сайта.

Твоя задача — писать информационные медицинские новости для пациентов.

Главный принцип:
Пиши только на основании входных данных. Не расширяй медицинскую тему за счёт догадок.

География и локальный контекст:
- Регион аудитории сайта не является местом исходного события.
- Место события определяй только по подтверждённым фактам.
- Не переноси событие во Владикавказ или Северную Осетию ради SEO.
- Не добавляй Владикавказ, Северную Осетию, РСО-Аланию или Аланию в заголовок, slug, meta description или фактическую часть, если этой географии нет в подтверждённых фактах.
- Локальный фирменный блок лаборатории добавляется системой отдельно после проверки текста.
- Не утверждай, что ФАП, поликлиника, больница или другой объект проводит лабораторные исследования, если это прямо не указано в фактах.

Жёсткие правила:
- Не ставь диагнозы.
- Не назначай лечение.
- Не давай индивидуальных медицинских рекомендаций.
- Не обещай результат.
- Не запугивай пациента.
- Не придумывай факты, которых нет во входных данных.
- Не копируй источники дословно.
- Не используй категоричные формулировки там, где нужна осторожность.
- Не называй конкретные анализы, показатели, биомаркеры или лабораторные панели, если они прямо не указаны во входных данных.
- Не придумывай правила подготовки к анализам.
- Не советуй отменять лекарства, менять питание, исключать алкоголь или выполнять другие действия перед анализом, если это прямо не указано во входных данных.
- Не добавляй лабораторную диагностику, анализы или подготовку к ним, если эта тема естественно не следует из исходного события.
- Не превращай общее событие в статью про анализы или лабораторию. Исходное событие должно оставаться главной темой.
- Если конкретная медицинская рекомендация не подтверждена источником, просто не добавляй её.
- Не объясняй читателю, каких сведений нет в источниках или входных данных.
- Не добавляй дисклеймер в создаваемый текст: система автоматически добавит один стандартный дисклеймер после проверки.

Редакционная точность:
- Не добавляй универсальные медицинские фразы, не связанные с исходным событием.
- Не добавляй фразы об анализах и лабораторной диагностике только ради медицинского дисклеймера.
- Не используй прямую речь и длинные цитаты.
- Высказывания людей передавай только косвенной речью.
- Русские кавычки используй только для официальных названий организаций, проектов и документов.
- Не копируй из источника длинные высказывания должностных лиц.
- Не обещай, что ремонт, оборудование или проект обязательно повысят качество помощи, если такого вывода нет в подтверждённых фактах.
- Перед выдачей вычитай текст: исправь опечатки, грамматику и неудачные заголовки разделов.

Запрещённые формулировки, если их нет во входных данных:
- "конкретные лабораторные панели или показатели, если они не указаны во входных данных"
- "следует исключить алкоголь"
- "нужно отказаться от лекарств"
- "необходимо соблюдать диету"
- "обязательно сдайте анализ"
- "этот анализ выявляет заболевание"
- "результат покажет наличие болезни"

Стиль:
- Спокойный.
- Понятный.
- Профессиональный.
- Без рекламной агрессии.
- Без канцелярского перегруза.
- Без медицинского давления на пациента.

Выбор визуальной темы:
- Верни поле image_topic.
- Выбирай тему по основному событию и подтверждённым фактам.
- Не выбирай laboratory_diagnostics только из-за обязательного блока о лабораторной диагностике.
- Упоминание Владикавказа или Северной Осетии само по себе не означает north_ossetia_news.
- Выбирай наиболее конкретную подходящую тему.
- Если точной темы нет, используй general_medical_news.

Допустимые значения image_topic:
- laboratory_diagnostics
- prevention_health
- medical_technologies
- medical_science
- education_staff
- affordable_medicine
- healthy_lifestyle
- healthcare_region
- north_ossetia_news
- emergency_care
- pharma
- cardiology
- oncology
- diabetes
- immunity_infections
- rehabilitation
- maternal_child_health
- general_medical_news

Формат ответа:
Верни строго JSON без Markdown-обёртки.

Схема JSON:
{
  "image_topic": "одно допустимое значение из списка",
  "title": "заголовок новости",
  "slug": "latin-slug",
  "meta_description": "SEO description до 300 символов",
  "body_markdown": "полный текст новости в Markdown",
  "source_note": "кратко: на основании каких источников подготовлен материал",
  "quality_score": 0
}
""".strip()


FORBIDDEN_IF_NOT_IN_SOURCE = (
    "печёночные пробы",
    "печеночные пробы",
    "билирубин",
    "ферритин",
    "ттг",
    "витамин d",
    "витамин д",
    "гемоглобин",
    "лейкоциты",
    "холестерин",
    "липидограмма",
    "глюкоза",
    "пцр",
    "онкомаркер",
    "онкомаркеры",
    "исключить алкоголь",
    "исключение алкоголя",
    "отказ от алкоголя",
    "отказаться от алкоголя",
    "отказ от медикаментов",
    "отказаться от лекарств",
    "прекратить прием лекарств",
    "прекратить приём лекарств",
    "соблюдать диету",
    "соблюдать рацион",
    "сбалансированное питание",
)


def normalize_text(value: str) -> str:
    value = (value or "").lower()
    value = value.replace("ё", "е")

    # Приводим Unicode-дефисы и тире к обычному дефису.
    # Например: РСО–Алания -> рсо-алания.
    value = re.sub(r"[‐-‒–—−]", "-", value)
    value = re.sub(r"\s*-\s*", "-", value)

    value = re.sub(r"\s+", " ", value)
    return value.strip()


def confirmed_source_text(brief: MedicalBrief) -> str:
    """
    Только подтверждённая фактологическая часть.

    region_text, angle и SEO-ключи не являются доказательством
    места исходного события.
    """

    return normalize_text(
        " ".join(
            [
                brief.title or "",
                brief.facts or "",
                brief.event.title if brief.event else "",
                brief.event.summary if brief.event else "",
            ]
        )
    )


def source_contains(brief: MedicalBrief, phrase: str) -> bool:
    return normalize_text(phrase) in confirmed_source_text(brief)


DEFAULT_NEWS_DISCLAIMER = (
    "Материал носит информационный характер "
    "и не заменяет консультацию врача."
)

LEGACY_ANALYSIS_DISCLAIMER = (
    "Интерпретацию результатов анализов должен проводить специалист "
    "с учётом жалоб, анамнеза и других данных пациента."
)


def get_medical_news_disclaimer() -> str:
    """
    Возвращает универсальный дисклеймер.

    Старое лабораторное продолжение не подходит кадровым,
    инфраструктурным, образовательным и другим общим новостям.
    """

    value = str(
        getattr(
            settings,
            "MEDICAL_NEWS_DISCLAIMER",
            DEFAULT_NEWS_DISCLAIMER,
        )
        or ""
    ).strip()

    value = re.sub(
        r"\s*Интерпретацию результатов анализов.*$",
        "",
        value,
        flags=re.I | re.S,
    ).strip()

    return value or DEFAULT_NEWS_DISCLAIMER


def apply_system_disclaimer(body: str) -> str:
    """
    Удаляет старые варианты дисклеймера из LLM-текста и добавляет
    единый системный дисклеймер ровно один раз.
    """

    cleaned = str(body or "").strip()

    known_sentences = (
        DEFAULT_NEWS_DISCLAIMER,
        LEGACY_ANALYSIS_DISCLAIMER,
    )

    for sentence in known_sentences:
        cleaned = re.sub(
            re.escape(sentence),
            "",
            cleaned,
            flags=re.I,
        )

    cleaned = re.sub(r"[ \t]+\n", "\n", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    cleaned = cleaned.strip()

    disclaimer = get_medical_news_disclaimer()

    if not cleaned:
        return disclaimer

    return f"{cleaned}\n\n{disclaimer}"


EDITORIAL_META_PATTERNS = (
    (
        "source-gap-commentary",
        re.compile(
            r"\bне\s+упомина\w+\s+в\s+источник\w+",
            flags=re.I,
        ),
    ),
    (
        "missing-input-commentary",
        re.compile(
            r"\b(?:отсутств\w+|не\s+указан\w+)"
            r"\s+(?:в\s+)?(?:источник\w+|"
            r"входн\w+\s+данн\w+)",
            flags=re.I,
        ),
    ),
    (
        "generic-test-preparation",
        re.compile(
            r"\bподготовк\w+\s+к\s+"
            r"(?:люб\w+\s+)?"
            r"(?:медицинск\w+\s+)?"
            r"исследован\w+",
            flags=re.I,
        ),
    ),
    (
        "extra-medical-disclaimer",
        re.compile(
            r"\bне\s+явля\w+\s+"
            r"медицинск\w+\s+назначен\w+",
            flags=re.I,
        ),
    ),
    (
        "duplicate-system-disclaimer",
        re.compile(
            r"\b(?:материал|информация)\w*\s+"
            r"нос\w+\s+информационн\w+\s+"
            r"характер\w*.{0,160}"
            r"\bне\s+заменя\w+.{0,80}"
            r"\bконсультац\w+\s+врач\w+",
            flags=re.I,
        ),
    ),
    (
        "generic-care-direction",
        re.compile(
            r"\bдля\s+получен\w+\s+"
            r"точн\w+\s+и\s+актуальн\w+\s+"
            r"медицинск\w+\s+помощ\w+",
            flags=re.I,
        ),
    ),
)


def medical_editorial_meta_hits(value: str) -> list[str]:
    normalized = normalize_text(value)

    return [
        label
        for label, pattern
        in EDITORIAL_META_PATTERNS
        if pattern.search(normalized)
    ]


def markdown_section_count(value: str) -> int:
    return len(
        re.findall(
            r"(?m)^\s*#{2,3}\s+\S",
            str(value or ""),
        )
    )


def validate_medical_safety(brief: MedicalBrief, title: str, body: str) -> list[str]:
    """
    Постгенерационная проверка.

    Она не заменяет редактора, но ловит грубые LLM-галлюцинации:
    конкретные анализы и инструкции по подготовке, которых не было во входных данных.
    """

    errors: list[str] = []
    generated_text = normalize_text(f"{title} {body}")

    for phrase in FORBIDDEN_IF_NOT_IN_SOURCE:
        normalized_phrase = normalize_text(phrase)

        if normalized_phrase in generated_text and not source_contains(brief, phrase):
            errors.append(
                f"Модель использовала неподтверждённую медицинскую конкретику: {phrase}"
            )

    source_text = confirmed_source_text(brief)

    locality_groups = {
        "Владикавказ": (
            "владикавказ",
        ),
        "Северная Осетия": (
            "северная осетия",
            "северной осетии",
            "северную осетию",
            "рсо-алания",
            "рсо алания",
            "республика северная осетия",
        ),
    }

    for locality_name, markers in locality_groups.items():
        generated_has_locality = any(
            marker in generated_text
            for marker in markers
        )

        source_has_locality = any(
            marker in source_text
            for marker in markers
        )

        if generated_has_locality and not source_has_locality:
            errors.append(
                "Модель добавила неподтверждённую географию "
                f"события: {locality_name}"
            )

    unsupported_hits = unsupported_claim_hits(
        generated_text=f"{title}\n{body}",
        source_text=source_text,
    )

    for claim_name in unsupported_hits:
        errors.append(
            "Модель добавила неподтверждённое утверждение: "
            f"{claim_name}"
        )

    raw_generated_text = f"{title}\n{body}"

    placeholder_patterns = [
        r"\[\s*(?:указать|вставить)[^\]]*\]",
        r"\{\{[^}]+\}\}",
    ]

    if any(
        re.search(pattern, raw_generated_text, flags=re.I)
        for pattern in placeholder_patterns
    ):
        errors.append(
            "Модель оставила редакционный шаблон или placeholder"
        )

    # Для автоматических публикаций используем косвенную речь.
    # Прямой речью считаются:
    # - отдельные строки, начинающиеся с тире;
    # - длинные высказывания внутри русских кавычек.
    #
    # Короткие названия организаций и проектов в «кавычках»
    # прямой речью не считаются.
    raw_body = str(body or "")

    for hit in medical_editorial_meta_hits(
        raw_generated_text
    ):
        errors.append(
            "Модель добавила редакционную "
            f"мета-фразу: {hit}"
        )

    section_count = markdown_section_count(
        raw_body
    )

    if section_count < 2:
        errors.append(
            "Модель не добавила минимум два "
            "Markdown-подзаголовка второго уровня"
        )

    quote_candidates: list[str] = []

    for line in raw_body.splitlines():
        stripped = line.strip()

        if re.match(r"^—\s+\S", stripped):
            quote_candidates.append(
                stripped.lstrip("—").strip()
            )

    for match in re.finditer(
        r"«([^»]{120,})»",
        raw_body,
        flags=re.I | re.S,
    ):
        quote_candidates.append(
            re.sub(
                r"\s+",
                " ",
                match.group(1),
            ).strip()
        )

    unique_quotes: list[str] = []
    seen_quotes: set[str] = set()

    for quote in quote_candidates:
        normalized_quote = normalize_text(quote)

        if not normalized_quote:
            continue

        if normalized_quote in seen_quotes:
            continue

        seen_quotes.add(normalized_quote)
        unique_quotes.append(quote)

    if unique_quotes:
        errors.append(
            "Модель оставила прямую речь вместо косвенного пересказа: "
            f"{len(unique_quotes)} фрагмент(а)"
        )

    for quote in unique_quotes:
        normalized_quote = normalize_text(quote)

        # Убираем авторскую ремарку у цитат формата
        # «текст, — отметил Иванов».
        for attribution in (
            "-сказал",
            "-отметил",
            "-сообщил",
            "-добавил",
            "-рассказал",
            "-прокомментировал",
            "-подчеркнул",
        ):
            if attribution in normalized_quote:
                normalized_quote = normalized_quote.split(
                    attribution,
                    1,
                )[0].rstrip(" ,.-")
                break

        probe = normalized_quote[:140].strip()

        if (
            len(probe) >= 60
            and probe not in source_text
        ):
            errors.append(
                "Модель добавила прямую цитату, "
                "не подтверждённую исходными фактами"
            )

    # Старые универсальные вставки больше не допускаются даже
    # в слегка переформулированном виде.
    editorial_stock_phrases = (
        "зависит от конкретного исследования",
        "условия лучше уточнять заранее",
        "интерпретацию результатов анализов",
        "роль лабораторной диагностики",
        "лабораторная диагностика во владикавказе",
    )

    for phrase in editorial_stock_phrases:
        normalized_phrase = normalize_text(phrase)

        if normalized_phrase in generated_text:
            errors.append(
                "Модель использовала нерелевантную шаблонную фразу: "
                f"{phrase}"
            )

    # Ремонт, оборудование или организационные изменения сами по себе
    # не доказывают рост качества медицинской помощи.
    # Такие выводы допустимы только при наличии соответствующего
    # утверждения в подтверждённых фактах.
    if "качеств" not in source_text:
        unsupported_quality_patterns = (
            r"повыс\w*\s+качеств",
            r"улучш\w*\s+качеств",
            r"качественн\w+\s+медицинск\w+\s+услуг",
            r"не\s+долж\w*[^.]{0,120}"
            r"повли\w*[^.]{0,120}качеств",
            r"обеспеч\w*[^.]{0,120}качеств",
        )

        for pattern in unsupported_quality_patterns:
            if re.search(
                pattern,
                generated_text,
                flags=re.I,
            ):
                errors.append(
                    "Модель сделала неподтверждённый вывод "
                    "об изменении качества медицинской помощи"
                )
                break

    return errors


def build_user_prompt(brief: MedicalBrief) -> str:
    confirmed_length = len(
        normalize_text(confirmed_source_text(brief))
    )

    if confirmed_length < 1200:
        article_length_instruction = (
            "Объём: примерно 1000–1800 знаков. "
            "Источник содержит мало подробностей, поэтому не растягивай "
            "материал и не заполняй пробелы предположениями. "
            "Лучше написать короткую фактическую новость."
        )
    elif confirmed_length < 2500:
        article_length_instruction = (
            "Объём: примерно 1600–2800 знаков. "
            "Не увеличивай объём за счёт неподтверждённых услуг, целей, "
            "эффектов, маршрутов или организационных деталей."
        )
    else:
        article_length_instruction = (
            "Объём: примерно 2500–5000 знаков, только в пределах "
            "подтверждённых исходных данных."
        )

    disclaimer = get_medical_news_disclaimer()

    seo_keyword = (
        brief.target_keyword.strip()
        if brief.target_keyword
        else "не задан"
    )

    return f"""
Подготовь информационную медицинскую новость.

Исходный заголовок:
{brief.title}

Редакционный угол:
{brief.angle or "Сохранить основную тему исходного события."}

Целевая аудитория:
{brief.audience}

Регион аудитории сайта — это не место события:
{brief.region_text}

Необязательный SEO-ориентир:
{seo_keyword}

Дополнительные ключевые фразы:
{brief.secondary_keywords or "нет"}

Подтверждённые факты:
{brief.facts or "нет данных"}

Источники:
{brief.source_urls or "нет ссылок"}

Ограничения безопасности:
{brief.safety_notes or "Не ставить диагнозы, не назначать лечение, не обещать результат."}

Системный дисклеймер, который будет добавлен после генерации:
{disclaimer}

Требования:
- Напиши материал на русском языке.
- {article_length_instruction}
- Используй Markdown.
- Начни с короткого введения.
- Используй 2–4 смысловых подзаголовка второго уровня в формате «## Заголовок».
- Добавь осторожный вывод.
- Не добавляй дисклеймер в body_markdown:
  система добавит его автоматически после проверки текста.
- Не добавляй универсальные фразы об анализах,
  результатах исследований или лабораторной диагностике
  только ради медицинского завершения статьи.
- Не используй прямую речь и не вставляй длинные цитаты.
- Высказывания должностных лиц и специалистов передавай только косвенной речью.
- Не используй универсальные фразы о подготовке к исследованиям,
  анализах или интерпретации результатов, если статья не посвящена этой теме.
- Не утверждай, что ремонт, оборудование или организационный проект
  улучшит либо повысит качество медицинской помощи, если этого вывода
  нет в подтверждённых фактах.
- Перед ответом вычитай заголовки и основной текст на опечатки,
  грамматические ошибки и несоответствие смыслу раздела.
- Не утверждай, что ремонт или новое оборудование гарантированно
  улучшат качество помощи, если это прямо не указано в фактах.
- Исходное событие должно оставаться главной темой статьи.
- Не создавай обязательный блок о лабораторной диагностике.
- Если SEO-ориентир не задан, не добавляй самостоятельно анализы,
  лабораторную диагностику или подготовку к исследованиям.
- Если SEO-ориентир задан, используй его только при естественной связи
  с подтверждёнными фактами и максимум в одном смысловом фрагменте.
- Не добавляй к заголовку фразы «роль лабораторной диагностики»,
  «важность анализов» и похожие SEO-приписки.
- Не добавляй конкретные анализы, отсутствующие во входных данных.
- Не придумывай правила подготовки к исследованиям.
- Не приписывай медицинской организации услуги,
  которые не указаны в источниках.
- Не добавляй Владикавказ или Северную Осетию,
  если этой географии нет в подтверждённых фактах.
- Не обсуждай, каких сведений нет в источниках или входных данных.
- Не пиши, что анализы, методы диагностики или услуги
  не упоминаются в источниках.
- Не добавляй дополнительные дисклеймеры, предупреждения
  о медицинских назначениях или универсальные направления к врачу:
  система добавит один стандартный дисклеймер автоматически.
- Если SEO-ориентир нельзя использовать естественно
  и строго в пределах подтверждённых фактов, полностью пропусти его.
- Не добавляй локальный CTA или рекламный блок.
- Не вставляй список источников в основной текст.
- Не оставляй placeholders.
- Поле image_topic выбирай по основной теме исходного события.
""".strip()

def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def normalize_quality_score(value: Any) -> int:
    score = safe_int(value, 0)

    if score < 0:
        return 0

    if score > 100:
        return 100

    return score


def normalize_image_topic(value: Any) -> str:
    topic = str(value or "").strip().lower()
    topic = topic.replace("-", "_").replace(" ", "_")

    aliases = {
        "lab": "laboratory_diagnostics",
        "laboratory": "laboratory_diagnostics",
        "laboratory_diagnostic": "laboratory_diagnostics",
        "prevention": "prevention_health",
        "technology": "medical_technologies",
        "technologies": "medical_technologies",
        "science": "medical_science",
        "education": "education_staff",
        "staff": "education_staff",
        "accessibility": "affordable_medicine",
        "lifestyle": "healthy_lifestyle",
        "regional_healthcare": "healthcare_region",
        "north_ossetia": "north_ossetia_news",
        "emergency": "emergency_care",
        "pharmaceuticals": "pharma",
        "immunity": "immunity_infections",
        "infections": "immunity_infections",
        "maternal_health": "maternal_child_health",
        "child_health": "maternal_child_health",
        "general": "general_medical_news",
    }

    topic = aliases.get(topic, topic)

    if topic not in IMAGE_TOPICS:
        return DEFAULT_IMAGE_TOPIC

    return topic


def make_slug(value: str, fallback_id: int) -> str:
    result = slugify(value or "", allow_unicode=False)

    if not result:
        result = f"medical-news-{fallback_id}"

    return result[:255]


def extract_news_payload(llm_text: str, brief_id: int) -> dict[str, Any]:
    data = parse_json_response(llm_text)

    title = str(data.get("title") or "").strip()
    body = str(data.get("body_markdown") or "").strip()

    if not title:
        raise OllamaError("LLM JSON does not contain title")

    if not body:
        raise OllamaError("LLM JSON does not contain body_markdown")

    slug = make_slug(str(data.get("slug") or title), brief_id)

    meta_description = str(data.get("meta_description") or "").strip()

    if len(meta_description) > 320:
        meta_description = meta_description[:317].rstrip() + "..."

    return {
        "image_topic": normalize_image_topic(data.get("image_topic")),
        "title": title[:300],
        "slug": slug,
        "meta_description": meta_description,
        "body": body,
        "source_note": str(data.get("source_note") or "").strip(),
        "quality_score": normalize_quality_score(data.get("quality_score")),
    }


def save_generation_error(
    brief_id: int,
    brief_title: str,
    prompt: str,
    model: str,
    error: Exception,
) -> None:
    """
    Сохраняем ошибку генерации в БД.

    После долгого LLM-запроса MySQL мог закрыть idle-соединение,
    поэтому перед записью всегда закрываем старые соединения.
    """

    close_old_connections()

    GeneratedMedicalNews.objects.create(
        brief_id=brief_id,
        title=f"Ошибка генерации: {brief_title[:220]}",
        slug=make_slug(f"generation-error-{brief_id}", brief_id),
        meta_description="",
        body="",
        source_note="",
        quality_score=0,
        status=MedicalNewsStatus.ERROR,
        llm_model=model,
        llm_prompt=prompt,
        llm_response_raw="",
        llm_elapsed_ms=None,
        llm_error=str(error),
    )


class Command(BaseCommand):
    help = "Генерирует медицинские новости через Ollama из готовых MedicalBrief."

    def add_arguments(self, parser):
        parser.add_argument(
            "--limit",
            type=int,
            default=3,
            help="Сколько новостей сгенерировать за один запуск.",
        )
        parser.add_argument(
            "--status",
            default=MedicalBriefStatus.READY,
            choices=[
                MedicalBriefStatus.DRAFT,
                MedicalBriefStatus.READY,
            ],
            help="Из какого статуса брать MedicalBrief.",
        )
        parser.add_argument(
            "--brief-ids",
            default="",
            help=(
                "Список ID MedicalBrief через запятую. "
                "Позволяет выполнить точечную генерацию."
            ),
        )
        parser.add_argument(
            "--model",
            default="",
            help="Переопределить модель Ollama для этого запуска.",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help="Генерировать даже если у задания уже есть новости.",
        )
        parser.add_argument(
            "--skip-safety-check",
            action="store_true",
            help="Отключить постпроверку медицинской безопасности. Только для отладки.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Показать prompt без запроса к Ollama и без записи в базу.",
        )

    def handle(self, *args, **options):
        limit = options["limit"]
        status = options["status"]
        brief_ids_raw = options["brief_ids"]
        model = options["model"] or getattr(settings, "OLLAMA_MODEL", "")
        force = options["force"]
        skip_safety_check = options["skip_safety_check"]
        dry_run = options["dry_run"]

        requested_brief_ids: list[int] = []

        if brief_ids_raw:
            for token in re.split(r"[\s,]+", brief_ids_raw.strip()):
                if not token:
                    continue

                try:
                    requested_brief_ids.append(int(token))
                except ValueError as exc:
                    raise CommandError(
                        f"Некорректный MedicalBrief ID: {token}"
                    ) from exc

            requested_brief_ids = list(
                dict.fromkeys(requested_brief_ids)
            )

        if not getattr(settings, "LLM_ENABLED", False) and not dry_run:
            self.stdout.write(self.style.ERROR("LLM_DISABLED: установи LLM_ENABLED=True в .env"))
            return

        close_old_connections()

        queryset = (
            MedicalBrief.objects
            .filter(status=status)
            .select_related("event")
            .annotate(
                created_day=TruncDate(
                    "created_at"
                )
            )
            .order_by(
                "-created_day",
                "id",
            )
        )

        if requested_brief_ids:
            queryset = queryset.filter(
                id__in=requested_brief_ids,
            )

        if not force:
            # Записи error/rejected не должны навсегда блокировать
            # повторную генерацию задания. Блокируем только задания,
            # у которых уже есть актуальная review/published версия.
            queryset = queryset.exclude(
                generated_news__status__in=[
                    "review",
                    "published",
                ],
            ).distinct()

        brief_ids = list(
            queryset
            .distinct()
            .values_list("id", flat=True)[:limit]
        )

        if not brief_ids:
            self.stdout.write(self.style.WARNING("Нет MedicalBrief для генерации."))
            return

        generated = 0
        failed = 0

        for brief_id in brief_ids:
            close_old_connections()

            try:
                brief = MedicalBrief.objects.select_related("event").get(pk=brief_id)
            except MedicalBrief.DoesNotExist:
                continue

            prompt = build_user_prompt(brief)

            self.stdout.write("")
            self.stdout.write(self.style.NOTICE(f"MedicalBrief #{brief.pk}: {brief.title[:120]}"))

            if dry_run:
                self.stdout.write("-" * 80)
                self.stdout.write(prompt)
                self.stdout.write("-" * 80)
                continue

            brief_title = brief.title
            selected_model = model or getattr(settings, "OLLAMA_MODEL", "")

            try:
                # Перед долгим внешним запросом закрываем DB-соединение:
                # во время генерации оно всё равно не используется.
                close_old_connections()

                result = generate_with_ollama(
                    prompt=prompt,
                    system=SYSTEM_PROMPT,
                    json_mode=True,
                    model=selected_model,
                )

                payload = extract_news_payload(result.text, brief.pk)

                safety_errors = []

                if not skip_safety_check:
                    safety_errors = validate_medical_safety(
                        brief=brief,
                        title=payload["title"],
                        body=payload["body"],
                    )

                if safety_errors:
                    raise OllamaError(
                        "Medical safety check failed: "
                        + "; ".join(safety_errors[:5])
                    )

                # Стандартный дисклеймер добавляется только после
                # проверки чистого текста, созданного моделью.
                payload["body"] = apply_system_disclaimer(
                    payload["body"]
                )

                # После долгого LLM-запроса MySQL мог закрыть idle-соединение.
                # Поэтому обязательно закрываем старое соединение перед записью.
                close_old_connections()

                with transaction.atomic():
                    news = GeneratedMedicalNews.objects.create(
                        brief_id=brief.pk,
                        image_topic=payload["image_topic"],
                        title=payload["title"],
                        slug=payload["slug"],
                        meta_description=payload["meta_description"],
                        body=payload["body"],
                        source_note=payload["source_note"],
                        disclaimer=get_medical_news_disclaimer(),
                        quality_score=payload["quality_score"],
                        status=MedicalNewsStatus.REVIEW,
                        llm_model=result.model,
                        llm_prompt=prompt,
                        llm_response_raw=json.dumps(result.raw, ensure_ascii=False, indent=2),
                        llm_elapsed_ms=result.elapsed_ms,
                        llm_error="",
                    )

                    MedicalBrief.objects.filter(pk=brief.pk).update(
                        status=MedicalBriefStatus.USED,
                        used_at=timezone.now(),
                    )

                generated += 1

                self.stdout.write(
                    self.style.SUCCESS(
                        f"Создана новость #{news.pk}: {news.title[:120]} "
                        f"({result.elapsed_ms} ms)"
                    )
                )

            except Exception as exc:
                failed += 1

                try:
                    save_generation_error(
                        brief_id=brief.pk,
                        brief_title=brief_title,
                        prompt=prompt,
                        model=selected_model,
                        error=exc,
                    )
                except Exception as save_exc:
                    self.stdout.write(
                        self.style.ERROR(
                            f"Не удалось сохранить ошибку генерации в БД: {save_exc}"
                        )
                    )

                self.stdout.write(
                    self.style.ERROR(
                        f"Ошибка генерации MedicalBrief #{brief.pk}: {exc}"
                    )
                )

        if dry_run:
            self.stdout.write(self.style.SUCCESS(f"Dry-run завершён. Заданий: {len(brief_ids)}"))
        else:
            self.stdout.write(
                self.style.SUCCESS(
                    f"Генерация завершена. Успешно: {generated}, ошибок: {failed}"
                )
            )
