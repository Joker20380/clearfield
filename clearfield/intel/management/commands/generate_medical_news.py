from __future__ import annotations

import json
import re
from typing import Any

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import close_old_connections, transaction
from django.utils import timezone
from django.utils.text import slugify

from intel.llm.ollama_client import (
    OllamaError,
    generate_with_ollama,
    parse_json_response,
)
from intel.models import (
    GeneratedMedicalNews,
    MedicalBrief,
    MedicalBriefStatus,
    MedicalNewsStatus,
)


SYSTEM_PROMPT = """
Ты медицинский редактор сайта лабораторной диагностики.

Твоя задача — писать информационные медицинские новости для пациентов.

Главный принцип:
Пиши только на основании входных данных. Не расширяй медицинскую тему за счёт догадок.

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
- Если входное событие связано с диспансеризацией, профилактическим осмотром, организацией медицинской помощи или оборудованием, пиши о лабораторной диагностике только в общем виде.
- Не превращай общее событие в статью про конкретный анализ, если конкретный анализ не указан во входных данных.
- Вместо конкретных медицинских рекомендаций используй безопасную формулировку: "Подготовка зависит от конкретного исследования, поэтому условия лучше уточнять заранее".
- Обязательно указывай, что материал информационный и не заменяет консультацию врача.

Безопасные формулировки:
- "лабораторная диагностика может быть частью профилактического обследования"
- "перечень исследований определяется специалистом"
- "интерпретация результатов проводится с учётом жалоб, анамнеза и других данных"
- "подготовка зависит от конкретного исследования"
- "условия сдачи анализа лучше уточнить заранее"

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

Формат ответа:
Верни строго JSON без Markdown-обёртки.

Схема JSON:
{
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
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def source_contains(brief: MedicalBrief, phrase: str) -> bool:
    source_text = normalize_text(
        " ".join(
            [
                brief.title or "",
                brief.angle or "",
                brief.target_keyword or "",
                brief.secondary_keywords or "",
                brief.facts or "",
                brief.source_urls or "",
                brief.safety_notes or "",
                brief.event.title if brief.event else "",
                brief.event.summary if brief.event else "",
            ]
        )
    )

    return normalize_text(phrase) in source_text


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

    return errors


def build_user_prompt(brief: MedicalBrief) -> str:
    disclaimer = getattr(
        settings,
        "MEDICAL_NEWS_DISCLAIMER",
        "Материал носит информационный характер и не заменяет консультацию врача.",
    )

    return f"""
Подготовь SEO-новость для сайта медицинской лаборатории.

Редакционное задание:
{brief.title}

Редакционный угол:
{brief.angle or "Подать как информационный материал для пациентов."}

Целевая аудитория:
{brief.audience}

Регион:
{brief.region_text}

Главная SEO-фраза:
{brief.target_keyword}

Дополнительные ключевые фразы:
{brief.secondary_keywords or "нет"}

Подтверждённые факты:
{brief.facts or "нет данных"}

Источники:
{brief.source_urls or "нет ссылок"}

Ограничения безопасности:
{brief.safety_notes or "Не ставить диагнозы, не назначать лечение, не обещать результат."}

Обязательный дисклеймер:
{disclaimer}

Требования к тексту:
- Напиши материал на русском языке.
- Объём: примерно 3500–6000 знаков.
- Используй Markdown.
- Структура:
  1. Короткое введение
  2. Несколько смысловых подзаголовков
  3. Блок о роли лабораторной диагностики
  4. Осторожный вывод
  5. Дисклеймер
- Не вставляй список источников в основной текст, вынеси его в source_note.
- Не делай автопубликационный стиль "срочно", "сенсация", "обязательно".
- Не добавляй конкретные анализы, которых нет во входных данных.
- Не добавляй инструкции по подготовке к анализам, если они не указаны во входных данных.
- Не используй фразы вроде "следует исключить алкоголь", "отказаться от медикаментов", "соблюдать рацион", если этого нет в источниках.
- Если нужно упомянуть подготовку, пиши только общую безопасную фразу: "Подготовка зависит от конкретного исследования, поэтому условия лучше уточнять заранее".
- Если входное событие общее, например диспансеризация, профилактика, оборудование, организация помощи, не превращай его в статью про конкретный анализ.
- Если конкретный анализ не указан во входных данных, используй общие слова: "лабораторные исследования", "анализы", "профилактическое обследование", "лабораторная диагностика".
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
        model = options["model"] or getattr(settings, "OLLAMA_MODEL", "")
        force = options["force"]
        skip_safety_check = options["skip_safety_check"]
        dry_run = options["dry_run"]

        if not getattr(settings, "LLM_ENABLED", False) and not dry_run:
            self.stdout.write(self.style.ERROR("LLM_DISABLED: установи LLM_ENABLED=True в .env"))
            return

        close_old_connections()

        queryset = (
            MedicalBrief.objects
            .filter(status=status)
            .select_related("event")
            .order_by("created_at")
        )

        if not force:
            queryset = queryset.filter(generated_news__isnull=True)

        brief_ids = list(queryset.distinct().values_list("id", flat=True)[:limit])

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

                # После долгого LLM-запроса MySQL мог закрыть idle-соединение.
                # Поэтому обязательно закрываем старое соединение перед записью.
                close_old_connections()

                with transaction.atomic():
                    news = GeneratedMedicalNews.objects.create(
                        brief_id=brief.pk,
                        title=payload["title"],
                        slug=payload["slug"],
                        meta_description=payload["meta_description"],
                        body=payload["body"],
                        source_note=payload["source_note"],
                        disclaimer=getattr(
                            settings,
                            "MEDICAL_NEWS_DISCLAIMER",
                            "Материал носит информационный характер и не заменяет консультацию врача.",
                        ),
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
