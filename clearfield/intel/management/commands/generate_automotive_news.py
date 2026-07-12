from __future__ import annotations

import json
import re
from typing import Any

from django.conf import settings
from django.core.management.base import (
    BaseCommand,
    CommandError,
)
from django.db import (
    close_old_connections,
    transaction,
)
from django.utils import timezone

from intel.automotive_editorial_validation import (
    IMAGE_TOPICS,
    apply_automotive_disclaimer,
    get_automotive_disclaimer,
    make_automotive_slug,
    minimum_body_chars_for_brief,
    normalize_image_topic,
    normalize_quality_score,
    normalize_source_urls,
    validate_automotive_news,
)
from intel.llm.ollama_client import (
    OllamaError,
    generate_with_ollama,
    parse_json_response,
)
from intel.models import (
    AutomotiveBrief,
    AutomotiveBriefStatus,
    AutomotiveNewsStatus,
    GeneratedAutomotiveNews,
)


SYSTEM_PROMPT = """
Ты редактор автомобильного информационного сайта и автосервиса.

Твоя задача — подготовить полезную автомобильную новость на русском языке
исключительно на основании переданных подтверждённых фактов.

Главные правила:
- Исходное событие должно оставаться главной темой.
- Каждое конкретное утверждение должно прямо следовать
  из подтверждённых фактов.
- Не придумывай причины изменения продаж, спроса, цены,
  производства, импорта или других показателей.
- Не придумывай возможные последствия для цен, поставок,
  доступности автомобилей, бизнеса или регионов.
- Если причина или последствие не указаны во входных фактах,
  не обсуждай их вообще.
- Не добавляй универсальные советы, прогнозы и рассуждения
  только для увеличения объёма текста.
- Для новости о новой модели не придумывай её рыночный класс,
  позиционирование, целевую аудиторию, стратегию бренда,
  формат презентации или значение модели для компании.
- Не утверждай отсутствие экспортных планов, региональных продаж,
  характеристик или сроков, если источник прямо этого не сообщает.
- Не перечисляй сведения, которых нет во входных фактах:
  не создавай заключительный абзац о неизвестных характеристиках,
  экспорте, других рынках, региональной доступности или начале продаж.
- Не добавляй слова «официально», «презентация», «анонс»
  и описание обстоятельств представления модели, если этих слов
  и обстоятельств нет в подтверждённых фактах.
- Не превращай факт производства по полному циклу в вывод
  о стратегии, локализации, импортозамещении или планах компании.
- Формулировка «будут известны позднее» не означает наличие
  установленных сроков. Не придумывай и не интерпретируй сроки.
- Не пиши, что информация появится в ближайшее время:
  передавай только подтверждённую формулировку источника.
- Не объясняй, что локализация подтверждает намерения компании
  или развитие производства, если такого заявления нет в фактах.
- Не придумывай факты, характеристики, нормы, сроки, цены и причины поломок.
- Не копируй источник дословно.
- Не ставь технический диагноз автомобилю дистанционно.
- Не утверждай, что конкретную деталь обязательно нужно заменить.
- Код ошибки сам по себе не является диагнозом.
- Не обещай результат ремонта.
- Не утверждай, что движение безопасно, если автомобиль не осмотрен.
- При признаках, связанных с тормозами, рулевым управлением,
  перегревом, утечкой жидкостей или серьёзной вибрацией,
  используй осторожную формулировку о необходимости проверки.
- Не добавляй конкретные процедуры ремонта, моменты затяжки,
  электрические схемы и технические значения, которых нет во входных фактах.

География:
- Место исходного события определяется только подтверждёнными фактами.
- Не упоминай Владикавказ, Северную Осетию или любой другой регион,
  если его нет в подтверждённых фактах или в явно разрешённом
  сервисном контексте задания.
- Не переноси событие во Владикавказ или Северную Осетию ради SEO.
- Не добавляй эту географию в заголовок, slug или meta description,
  если её нет в подтверждённых фактах.
- Разрешён отдельный практический контекст для аудитории автосервиса,
  но он не должен создавать впечатление, что исходное событие произошло
  во Владикавказе.

SEO:
- Используй главную и дополнительные SEO-фразы естественно.
- Не повторяй одну фразу механически.
- Не подменяй факты рекламным текстом.
- Заголовок должен описывать исходное событие, а не рекламировать сервис.
- Meta description должна быть информативной и не длиннее 300 символов.

Структура:
- Для обычной автомобильной новости допустим связный текст
  примерно на 700–1800 знаков.
- Если подтверждённых фактов мало, используй 2–3 коротких раздела
  и объём ближе к нижней границе.
- Не увеличивай объём перечислением неизвестных сведений,
  предположениями, корпоративными выводами или прогнозами.
- Markdown без HTML.
- Не используй H1.
- Используй 2–4 смысловых раздела с заголовками второго уровня: ##.
- Допустим короткий маркированный список.
- Не вставляй URL в основной текст: ссылки система добавляет отдельно.
- Не вставляй Markdown-кодовые блоки.
- Не добавляй отдельный дисклеймер: система добавит его сама.

Стиль:
- Спокойный, профессиональный и понятный автовладельцу.
- Без кликбейта, запугивания и рекламной агрессии.
- Без прямой речи и длинных цитат.
- Без канцелярского перегруза.
- Перед ответом исправь опечатки и неудачные формулировки.

Допустимые значения image_topic:
- diagnostics
- engine
- auto_electrics
- suspension
- brakes
- tires
- transmission
- maintenance
- safety
- legislation
- automotive_market
- general_automotive_news

Формат ответа:
Верни строго JSON без Markdown-обёртки.

Схема:
{
  "image_topic": "одно допустимое значение",
  "title": "заголовок",
  "slug": "latin-slug",
  "meta_description": "SEO description до 300 символов",
  "body_markdown": "полный текст в Markdown",
  "source_note": "краткое описание основания и источников",
  "quality_score": 0
}
""".strip()


def parse_brief_ids(
    raw_value: str,
) -> list[int]:
    if not raw_value:
        return []

    result: list[int] = []

    for token in re.split(
        r"[\s,;]+",
        raw_value.strip(),
    ):
        if not token:
            continue

        try:
            value = int(token)
        except ValueError as exc:
            raise CommandError(
                "Некорректный AutomotiveBrief "
                f"ID: {token}"
            ) from exc

        if value <= 0:
            raise CommandError(
                "Некорректный AutomotiveBrief "
                f"ID: {token}"
            )

        result.append(value)

    return list(
        dict.fromkeys(result)
    )


def build_user_prompt(
    brief: AutomotiveBrief,
) -> str:
    source_urls = normalize_source_urls(
        brief.source_urls
    )

    source_block = (
        "\n".join(
            f"- {url}"
            for url in source_urls
        )
        or "- ссылки не указаны"
    )

    secondary_keywords = (
        str(
            brief.secondary_keywords
            or ""
        ).strip()
        or "не указаны"
    )

    facts = (
        str(
            brief.facts
            or ""
        ).strip()
        or "Факты не указаны."
    )

    event = getattr(
        brief,
        "event",
        None,
    )

    event_context = ""

    if event:
        event_context = (
            f"\nЗаголовок Event: {event.title}\n"
            f"Summary Event: {event.summary}\n"
            f"Регион Event: {event.region}\n"
            f"Evidence level: {event.evidence_level}\n"
        )

    service_mode = bool(
        brief.disclaimer_required
    )

    content_mode = (
        "сервисный материал"
        if service_mode
        else "обычная автомобильная новость"
    )

    disclaimer_mode = (
        "да — система добавит его автоматически"
        if service_mode
        else "нет — не добавляй диагностический дисклеймер"
    )

    return f"""
Подготовь автомобильную новость по редакционному заданию.

ID задания: {brief.pk}
Тип материала: {content_mode}
Диагностический дисклеймер требуется: {disclaimer_mode}
Исходный заголовок: {brief.title}
{event_context}
Подтверждённые факты:
{facts}

Редакционный угол:
{brief.angle or "не указан"}

Главная SEO-фраза:
{brief.target_keyword or "не указана"}

Дополнительные SEO-фразы:
{secondary_keywords}

Аудитория:
{brief.audience}

Локальный контекст аудитории:
{brief.region_text}

Ограничения:
{brief.safety_notes or "Не добавлять неподтверждённые факты."}

Источники:
{source_block}

Дополнительные требования:
- Не выдавай контекст аудитории за географию события.
- Не добавляй причины изменения рыночного показателя,
  если причины отсутствуют в подтверждённых фактах.
- Не добавляй прогнозы о ценах, поставках, доступности,
  спросе или региональной динамике.
- Для обычной автомобильной новости не добавляй
  диагностику, неисправности, ремонтные рекомендации,
  автосервис и диагностический дисклеймер.
- Для новости о новой модели не добавляй её класс,
  позиционирование, стратегию бренда, целевой рынок,
  детали презентации и корпоративные намерения,
  если их нет в подтверждённых фактах.
- Не делай утверждений об отсутствии экспортных планов,
  региональной доступности или сроков выпуска,
  если источник прямо этого не сообщает.
- Не перечисляй отсутствующие сведения о характеристиках,
  экспорте, других рынках и начале продаж.
- Не добавляй обстоятельства презентации или анонса,
  если их нет в подтверждённых фактах.
- Не делай из производства по полному циклу вывод
  о корпоративной стратегии или планах локализации.
- Не заменяй фразу «будут известны позднее»
  придуманными установленными или ожидаемыми сроками.
- Не придумывай неисправность, если во входных фактах её нет.
- Не советуй замену деталей без диагностики.
- Не вставляй ссылки в body_markdown.
- Верни quality_score от 0 до 100 как внутреннюю оценку
  полноты, связности и соответствия входным фактам.
""".strip()


def preferred_image_topic_for_brief(
    brief: AutomotiveBrief,
    fallback: object,
) -> str:
    context = " ".join(
        [
            str(brief.title or ""),
            str(brief.target_keyword or ""),
            str(brief.angle or ""),
            str(brief.facts or ""),
        ]
    ).lower().replace("ё", "е")

    if any(
        marker in context
        for marker in (
            "автомобильный рынок",
            "продажи автомобилей",
            "рынок новых",
            "импорт автомобилей",
            "доля рынка",
        )
    ):
        return "automotive_market"

    if any(
        marker in context
        for marker in (
            "закон",
            "осаго",
            "права автовладель",
            "правила ремонта",
            "данные для ремонта",
        )
    ):
        return "legislation"

    if bool(
        brief.disclaimer_required
    ):
        return normalize_image_topic(
            fallback
        )

    return "general_automotive_news"


def extract_news_payload(
    llm_text: str,
    brief_id: int,
) -> dict[str, Any]:
    data = parse_json_response(
        llm_text
    )

    title = str(
        data.get("title")
        or ""
    ).strip()

    body = str(
        data.get("body_markdown")
        or data.get("body")
        or ""
    ).strip()

    if not title:
        raise OllamaError(
            "LLM JSON does not contain title"
        )

    if not body:
        raise OllamaError(
            "LLM JSON does not contain body_markdown"
        )

    meta_description = str(
        data.get("meta_description")
        or ""
    ).strip()

    if len(meta_description) > 320:
        meta_description = (
            meta_description[:317]
            .rstrip()
            + "..."
        )

    return {
        "image_topic": normalize_image_topic(
            data.get("image_topic")
        ),
        "title": title[:300],
        "slug": make_automotive_slug(
            data.get("slug") or title,
            brief_id,
        ),
        "meta_description": (
            meta_description
        ),
        "body": body,
        "source_note": str(
            data.get("source_note")
            or ""
        ).strip(),
        "quality_score": (
            normalize_quality_score(
                data.get("quality_score")
            )
        ),
    }


def save_generation_error(
    *,
    brief: AutomotiveBrief,
    prompt: str,
    model: str,
    error: Exception,
) -> None:
    close_old_connections()

    GeneratedAutomotiveNews.objects.create(
        brief_id=brief.pk,
        title=(
            "Ошибка генерации: "
            f"{brief.title[:220]}"
        ),
        slug=make_automotive_slug(
            f"automotive-generation-error-{brief.pk}",
            brief.pk,
        ),
        meta_description="",
        body="",
        source_note="",
        source_urls=normalize_source_urls(
            brief.source_urls
        ),
        image_topic=(
            "general_automotive_news"
        ),
        disclaimer=(
            get_automotive_disclaimer()
            if brief.disclaimer_required
            else ""
        ),
        quality_score=0,
        status=AutomotiveNewsStatus.ERROR,
        llm_model=model,
        llm_prompt=prompt,
        llm_response_raw="",
        llm_elapsed_ms=None,
        llm_error=str(error),
    )


class Command(BaseCommand):
    help = (
        "Генерирует автомобильные новости "
        "из готовых AutomotiveBrief."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--limit",
            type=int,
            default=3,
        )

        parser.add_argument(
            "--status",
            default=(
                AutomotiveBriefStatus.READY
            ),
            choices=[
                AutomotiveBriefStatus.DRAFT,
                AutomotiveBriefStatus.READY,
            ],
        )

        parser.add_argument(
            "--brief-ids",
            default="",
            help=(
                "ID AutomotiveBrief через "
                "запятую или пробел."
            ),
        )

        parser.add_argument(
            "--model",
            default="",
        )

        parser.add_argument(
            "--force",
            action="store_true",
        )

        parser.add_argument(
            "--skip-safety-check",
            action="store_true",
        )

        parser.add_argument(
            "--dry-run",
            action="store_true",
            help=(
                "Показать prompt без вызова LLM "
                "и без записи в БД."
            ),
        )

    def handle(self, *args, **options):
        limit = max(
            int(options["limit"]),
            1,
        )

        status = options["status"]
        force = bool(options["force"])

        dry_run = bool(
            options["dry_run"]
        )

        skip_safety_check = bool(
            options["skip_safety_check"]
        )

        requested_ids = parse_brief_ids(
            options["brief_ids"]
        )

        selected_model = (
            options["model"]
            or getattr(
                settings,
                "OLLAMA_MODEL",
                "",
            )
        )

        queryset = (
            AutomotiveBrief.objects
            .filter(status=status)
            .select_related("event")
            .order_by("id")
        )

        if requested_ids:
            queryset = queryset.filter(
                id__in=requested_ids,
            )

        if not force:
            queryset = queryset.exclude(
                generated_news__status__in=[
                    AutomotiveNewsStatus.DRAFT,
                    AutomotiveNewsStatus.REVIEW,
                    AutomotiveNewsStatus.APPROVED,
                    AutomotiveNewsStatus.PUBLISHED,
                ]
            )

        briefs = list(
            queryset.distinct()[:limit]
        )

        generated = 0
        failed = 0

        if not briefs:
            self.stdout.write(
                self.style.WARNING(
                    "Нет AutomotiveBrief "
                    "для генерации."
                )
            )

        for brief in briefs:
            prompt = build_user_prompt(
                brief
            )

            self.stdout.write("")
            self.stdout.write(
                self.style.NOTICE(
                    f"AutomotiveBrief #{brief.pk}: "
                    f"{brief.title[:120]}"
                )
            )

            if dry_run:
                self.stdout.write(
                    "-" * 80
                )
                self.stdout.write(prompt)
                self.stdout.write(
                    "-" * 80
                )
                continue

            try:
                close_old_connections()

                result = generate_with_ollama(
                    prompt=prompt,
                    system=SYSTEM_PROMPT,
                    json_mode=True,
                    model=selected_model,
                )

                payload = extract_news_payload(
                    result.text,
                    brief.pk,
                )

                payload["image_topic"] = (
                    preferred_image_topic_for_brief(
                        brief,
                        payload["image_topic"],
                    )
                )

                if brief.disclaimer_required:
                    payload["body"] = (
                        apply_automotive_disclaimer(
                            payload["body"]
                        )
                    )

                    disclaimer_text = (
                        get_automotive_disclaimer()
                    )
                else:
                    from intel.automotive_editorial_validation import (
                        strip_automotive_disclaimer,
                    )

                    payload["body"] = (
                        strip_automotive_disclaimer(
                            payload["body"]
                        )
                    )

                    disclaimer_text = ""

                source_urls = (
                    normalize_source_urls(
                        brief.source_urls
                    )
                )

                safety_errors: list[str] = []

                if not skip_safety_check:
                    safety_errors = (
                        validate_automotive_news(
                            brief=brief,
                            title=payload["title"],
                            body=payload["body"],
                            meta_description=(
                                payload[
                                    "meta_description"
                                ]
                            ),
                            image_topic=(
                                payload[
                                    "image_topic"
                                ]
                            ),
                            source_urls=source_urls,
                            min_body_chars=(
                                minimum_body_chars_for_brief(
                                    brief
                                )
                            ),
                        )
                    )

                if safety_errors:
                    raise OllamaError(
                        "Automotive safety check "
                        "failed: "
                        + "; ".join(
                            safety_errors[:8]
                        )
                    )

                close_old_connections()

                with transaction.atomic():
                    news = (
                        GeneratedAutomotiveNews
                        .objects.create(
                            brief_id=brief.pk,
                            title=payload["title"],
                            slug=payload["slug"],
                            meta_description=(
                                payload[
                                    "meta_description"
                                ]
                            ),
                            body=payload["body"],
                            source_note=(
                                payload[
                                    "source_note"
                                ]
                            ),
                            source_urls=source_urls,
                            image_topic=(
                                payload[
                                    "image_topic"
                                ]
                            ),
                            disclaimer=(
                                disclaimer_text
                            ),
                            quality_score=(
                                payload[
                                    "quality_score"
                                ]
                            ),
                            status=(
                                AutomotiveNewsStatus.REVIEW
                            ),
                            llm_model=result.model,
                            llm_prompt=prompt,
                            llm_response_raw=(
                                json.dumps(
                                    result.raw,
                                    ensure_ascii=False,
                                    indent=2,
                                )
                            ),
                            llm_elapsed_ms=(
                                result.elapsed_ms
                            ),
                            llm_error="",
                        )
                    )

                    (
                        AutomotiveBrief.objects
                        .filter(pk=brief.pk)
                        .update(
                            status=(
                                AutomotiveBriefStatus.USED
                            ),
                            used_at=timezone.now(),
                        )
                    )

                generated += 1

                self.stdout.write(
                    self.style.SUCCESS(
                        f"Создана автомобильная "
                        f"новость #{news.pk}: "
                        f"{news.title[:120]} "
                        f"({result.elapsed_ms} ms)"
                    )
                )

            except Exception as exc:
                failed += 1

                try:
                    save_generation_error(
                        brief=brief,
                        prompt=prompt,
                        model=selected_model,
                        error=exc,
                    )
                except Exception as save_exc:
                    self.stdout.write(
                        self.style.ERROR(
                            "Не удалось сохранить "
                            "ошибку генерации: "
                            f"{save_exc}"
                        )
                    )

                self.stdout.write(
                    self.style.ERROR(
                        "Ошибка генерации "
                        f"AutomotiveBrief "
                        f"#{brief.pk}: {exc}"
                    )
                )

        if dry_run:
            self.stdout.write(
                self.style.SUCCESS(
                    "Dry-run завершён. "
                    f"Заданий: {len(briefs)}"
                )
            )
        else:
            self.stdout.write(
                self.style.SUCCESS(
                    "Генерация завершена: "
                    f"created={generated}, "
                    f"failed={failed}"
                )
            )
