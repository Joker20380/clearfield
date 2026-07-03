from __future__ import annotations

import json
import re

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import close_old_connections

from intel.llm.ollama_client import (
    generate_with_ollama,
    parse_json_response,
)
from intel.management.commands.generate_medical_news import (
    IMAGE_TOPICS,
    normalize_image_topic,
)
from intel.models import GeneratedMedicalNews


SYSTEM_PROMPT = """
Ты классификатор визуальных тем медицинских новостей.

Определи одну основную тему изображения только по исходному событию
и подтверждённым фактам.

Не ориентируйся на:
- SEO-фразы;
- обязательные упоминания лабораторной диагностики;
- название сайта;
- регион публикации сам по себе;
- редакционную обвязку готовой статьи.

Ключевые разграничения:

laboratory_diagnostics:
только если исходное событие действительно посвящено анализам,
лабораторным исследованиям, крови, биоматериалу, результатам исследований
или работе лаборатории.

medical_technologies:
электронные документы, цифровые сервисы, порталы, искусственный интеллект,
медицинское оборудование, автоматизация и телемедицина.

education_staff:
студенты, выпускники, обучение, кадровая политика, награды медицинским
работникам, просветительские и образовательные проекты.

healthcare_region:
работа министерств, совещания руководителей, развитие больниц и поликлиник,
организация и управление системой здравоохранения.

north_ossetia_news:
только если сама Северная Осетия, её региональное событие или инфраструктура
являются центральной темой, а более конкретной медицинской темы нет.

general_medical_news:
международные встречи, соглашения и общие события, для которых нет более
точной медицинской категории.

Причину пиши только по-русски.

Верни строго JSON без Markdown:
{
  "image_topic": "одно разрешённое значение",
  "reason": "краткое объяснение на русском языке"
}
""".strip()


STRONG_TOPIC_RULES = (
    (
        "diabetes",
        (
            r"\bдиабет",
            r"глюкоз",
            r"сахар(?:а|ом|ный)?\s+(?:в\s+)?кров",
        ),
    ),
    (
        "oncology",
        (
            r"онколог",
            r"онкозаболев",
            r"злокачествен",
            r"\bрак(?:а|ом|е)?\b",
            r"опухол",
        ),
    ),
    (
        "cardiology",
        (
            r"кардиолог",
            r"сердечно-сосуд",
            r"инфаркт",
            r"инсульт",
            r"болезн.{0,15}сердц",
        ),
    ),
    (
        "rehabilitation",
        (
            r"реабилитац",
            r"восстановительн.{0,15}лечен",
        ),
    ),
    (
        "immunity_infections",
        (
            r"\bгрипп",
            r"\bорви\b",
            r"иммунитет",
            r"вакцинац",
            r"инфекционн.{0,15}заболев",
            r"вирусн.{0,15}инфекц",
            r"коронавирус",
            r"\bковид",
            r"\bвич\b",
            r"гепатит",
        ),
    ),
    (
        "maternal_child_health",
        (
            r"материнств",
            r"детств",
            r"роддом",
            r"беременн",
            r"новорожден",
            r"педиатр",
        ),
    ),
    (
        "pharma",
        (
            r"лекарственн.{0,20}обеспеч",
            r"производств.{0,20}лекарств",
            r"дефицит.{0,15}лекарств",
            r"регистрац.{0,20}препарат",
            r"фармацевтическ.{0,20}производств",
            r"фармацевтическ.{0,20}рынок",
            r"фармацевтическ.{0,20}препарат",
            r"фармацевтическ.{0,20}обеспеч",
            r"аптечн.{0,15}сеть",
            r"аптечн.{0,15}организац",
        ),
    ),
    (
        "emergency_care",
        (
            r"скорая помощ",
            r"экстренн",
            r"неотложн",
            r"реанимац",
            r"катастроф",
            r"\bдтп\b",
        ),
    ),
    (
        "medical_technologies",
        (
            r"электронн.{0,20}документ",
            r"цифров",
            r"медицинск.{0,20}портал",
            r"искусственн.{0,15}интеллект",
            r"\bии\b",
            r"телемедицин",
            r"автоматизац",
            r"новое оборудование",
            r"медицинск.{0,20}оборудован",
        ),
    ),
    (
        "education_staff",
        (
            r"просветительск",
            r"образовательн",
            r"обучени",
            r"студент",
            r"выпускник",
            r"ординатор",
            r"кадров.{0,15}политик",
            r"молод.{0,10}специалист",
            r"награжд",
            r"лауреат",
            r"преми.{0,20}медик",
        ),
    ),
    (
        "laboratory_diagnostics",
        (
            r"лабораторн.{0,20}исследован",
            r"лабораторн.{0,20}диагностик",
            r"подготовк.{0,15}анализ",
            r"результат.{0,15}анализ",
            r"забор.{0,15}кров",
            r"биоматериал",
            r"медицинск.{0,10}лаборатор",
        ),
    ),
    (
        "prevention_health",
        (
            r"профилактик",
            r"диспансеризац",
            r"профосмотр",
            r"скрининг",
            r"ранн.{0,15}выявлен",
        ),
    ),
    (
        "healthy_lifestyle",
        (
            r"здоровый образ жизни",
            r"\bзож\b",
            r"физическ.{0,10}активност",
            r"здоров.{0,10}питани",
        ),
    ),
    (
        "medical_science",
        (
            r"научн.{0,20}исследован",
            r"медицинск.{0,15}наук",
            r"ученые",
            r"исследователь",
            r"академическ",
        ),
    ),
    (
        "healthcare_region",
        (
            r"совещани.{0,30}министр.{0,20}здравоохран",
            r"руководител.{0,20}здравоохран",
            r"управлени.{0,20}отрасл",
            r"система здравоохранения",
            r"министерств.{0,15}здравоохран",
            r"развити.{0,20}поликлиник",
            r"развити.{0,20}больниц",
            r"открыт.{0,20}поликлиник",
            r"открыт.{0,20}больниц",
        ),
    ),
)


def normalize_text(value: str) -> str:
    value = str(value or "").lower().replace("ё", "е")
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def source_fields(news: GeneratedMedicalNews) -> dict[str, str]:
    brief = news.brief
    event = brief.event if brief else None

    event_title = str(getattr(event, "title", "") or "")
    event_summary = str(getattr(event, "summary", "") or "")
    brief_facts = str(getattr(brief, "facts", "") or "")
    brief_title = str(getattr(brief, "title", "") or "")

    # При наличии исходного события не используем готовый заголовок новости,
    # SEO-ключ и редакционный угол: они системно смещены к лабораторной теме.
    if event_title or event_summary:
        classification_text = " ".join(
            [
                event_title,
                event_summary,
                brief_facts,
            ]
        )
    else:
        classification_text = " ".join(
            [
                brief_title,
                brief_facts,
            ]
        )

    return {
        "event_title": event_title,
        "event_summary": event_summary,
        "brief_title": brief_title,
        "brief_facts": brief_facts,
        "classification_text": classification_text,
    }


def detect_strong_topic(news: GeneratedMedicalNews) -> tuple[str, str]:
    fields = source_fields(news)
    text = normalize_text(fields["classification_text"])

    for topic, patterns in STRONG_TOPIC_RULES:
        for pattern in patterns:
            if re.search(pattern, text, flags=re.I):
                return topic, pattern

    return "", ""


def build_prompt(news: GeneratedMedicalNews) -> str:
    fields = source_fields(news)
    topics = "\n".join(f"- {topic}" for topic in IMAGE_TOPICS)

    return f"""
Разрешённые значения image_topic:
{topics}

Исходный заголовок события:
{fields["event_title"] or "нет"}

Краткое содержание исходного события:
{fields["event_summary"] or "нет"}

Подтверждённые факты:
{fields["brief_facts"] or "нет"}

Заголовок редакционного задания:
{fields["brief_title"] or "нет"}

Классифицируй только основное исходное событие.

Не учитывай готовый SEO-заголовок статьи, обязательный блок о лабораторной
диагностике и целевую SEO-фразу.
""".strip()


class Command(BaseCommand):
    help = "Заполняет image_topic существующих медицинских новостей через Ollama."

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=20)
        parser.add_argument(
            "--statuses",
            default="published,review,approved",
        )
        parser.add_argument("--model", default="")
        parser.add_argument("--force", action="store_true")
        parser.add_argument("--dry-run", action="store_true")

    def handle(self, *args, **options):
        limit = max(options["limit"], 1)

        statuses = [
            value.strip()
            for value in options["statuses"].split(",")
            if value.strip()
        ]

        model = options["model"] or getattr(settings, "OLLAMA_MODEL", "")
        force = options["force"]
        dry_run = options["dry_run"]

        queryset = (
            GeneratedMedicalNews.objects
            .filter(status__in=statuses)
            .select_related("brief__event")
            .order_by("-id")
        )

        if not force:
            queryset = queryset.filter(image_topic="")

        news_ids = list(
            queryset.values_list("id", flat=True)[:limit]
        )

        if not news_ids:
            self.stdout.write(
                self.style.WARNING("Нет новостей для классификации.")
            )
            return

        updated = 0
        failed = 0

        for news_id in news_ids:
            close_old_connections()

            try:
                news = (
                    GeneratedMedicalNews.objects
                    .select_related("brief__event")
                    .get(pk=news_id)
                )

                prompt = build_prompt(news)

                result = generate_with_ollama(
                    prompt=prompt,
                    system=SYSTEM_PROMPT,
                    json_mode=True,
                    model=model or None,
                )

                payload = parse_json_response(result.text)

                raw_topic = payload.get("image_topic")
                llm_topic = normalize_image_topic(raw_topic)
                reason = str(payload.get("reason") or "").strip()

                strong_topic, matched_pattern = detect_strong_topic(news)

                final_topic = strong_topic or llm_topic

                self.stdout.write("")
                self.stdout.write(
                    f"#{news.pk} | {news.title[:110]}"
                )
                self.stdout.write(
                    f"  ollama: {raw_topic!r} -> {llm_topic}"
                )

                if strong_topic:
                    self.stdout.write(
                        f"  guard:  {strong_topic} "
                        f"(pattern={matched_pattern!r})"
                    )

                self.stdout.write(
                    f"  final:  {final_topic}"
                )

                if reason:
                    self.stdout.write(
                        f"  reason: {reason[:300]}"
                    )

                if dry_run:
                    continue

                GeneratedMedicalNews.objects.filter(
                    pk=news.pk
                ).update(
                    image_topic=final_topic
                )

                updated += 1

            except Exception as exc:
                failed += 1

                self.stdout.write(
                    self.style.ERROR(
                        f"ERROR #{news_id}: {exc}"
                    )
                )

        self.stdout.write("")

        self.stdout.write(
            json.dumps(
                {
                    "processed": len(news_ids),
                    "updated": updated,
                    "failed": failed,
                    "dry_run": dry_run,
                },
                ensure_ascii=False,
            )
        )
