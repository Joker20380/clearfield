from __future__ import annotations

import json
import os
import re
from pathlib import Path

from django.core.management.base import (
    BaseCommand,
    CommandError,
)
from django.db import transaction
from django.utils import timezone

from intel.automotive_editorial_validation import (
    normalize_source_urls,
)
from intel.models import (
    AutomotiveNewsStatus,
    GeneratedAutomotiveNews,
)


FEED_SOURCE = (
    "clearfield_generated_automotive_news"
)

DEFAULT_FILENAME = (
    "generated_automotive_news_feed.json"
)

EXPORTABLE_STATUSES = {
    AutomotiveNewsStatus.APPROVED,
    AutomotiveNewsStatus.PUBLISHED,
}


def default_output_path() -> Path:
    return (
        Path.cwd().parent
        / "generated-news"
        / DEFAULT_FILENAME
    )


def parse_statuses(
    raw_value: str,
) -> list[str]:
    tokens = [
        token.strip().lower()
        for token in re.split(
            r"[\s,;]+",
            str(raw_value or ""),
        )
        if token.strip()
    ]

    if not tokens:
        tokens = [
            AutomotiveNewsStatus.APPROVED,
            AutomotiveNewsStatus.PUBLISHED,
        ]

    invalid = sorted(
        set(tokens) - EXPORTABLE_STATUSES
    )

    if invalid:
        raise CommandError(
            "Недопустимые статусы экспорта: "
            + ", ".join(invalid)
        )

    return list(
        dict.fromkeys(tokens)
    )


def isoformat_or_none(value):
    if value is None:
        return None

    return value.isoformat()


def build_item(
    news: GeneratedAutomotiveNews,
) -> dict:
    effective_published_at = (
        news.published_at
        or news.updated_at
        or news.created_at
    )

    body = str(
        news.body or ""
    ).strip()

    return {
        "source_id": (
            f"automotive-news-{news.pk}"
        ),
        "title": news.title,
        "slug": news.slug,
        "meta_description": (
            news.meta_description
        ),
        "body_markdown": body,
        "body": body,
        "source_note": news.source_note,
        "source_urls": (
            normalize_source_urls(
                news.source_urls
            )
        ),
        "image_topic": news.image_topic,
        "created_at": (
            isoformat_or_none(
                news.created_at
            )
        ),
        "updated_at": (
            isoformat_or_none(
                news.updated_at
            )
        ),
        "published_at": (
            isoformat_or_none(
                effective_published_at
            )
        ),
    }


def write_atomic_json(
    output_path: Path,
    payload: dict,
) -> None:
    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp_path = output_path.with_name(
        f".{output_path.name}."
        f"{os.getpid()}.tmp"
    )

    try:
        temp_path.write_text(
            json.dumps(
                payload,
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

        os.chmod(
            temp_path,
            0o640,
        )

        os.replace(
            temp_path,
            output_path,
        )

    finally:
        temp_path.unlink(
            missing_ok=True,
        )


class Command(BaseCommand):
    help = (
        "Экспортирует одобренные автомобильные "
        "новости в отдельный JSON feed."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--output",
            default="",
            help=(
                "Путь к JSON-файлу. "
                "По умолчанию используется "
                "../generated-news/"
                f"{DEFAULT_FILENAME}"
            ),
        )

        parser.add_argument(
            "--statuses",
            default="approved,published",
            help=(
                "Экспортируемые статусы: "
                "approved,published."
            ),
        )

        parser.add_argument(
            "--limit",
            type=int,
            default=100,
        )

        parser.add_argument(
            "--mark-published",
            action="store_true",
            help=(
                "После успешной записи feed перевести "
                "экспортированные approved-новости "
                "в published."
            ),
        )

        parser.add_argument(
            "--dry-run",
            action="store_true",
            help=(
                "Вывести JSON в stdout без записи "
                "файла и изменения статусов."
            ),
        )

    def handle(self, *args, **options):
        statuses = parse_statuses(
            options["statuses"]
        )

        limit = max(
            int(options["limit"]),
            1,
        )

        output_value = str(
            options["output"] or ""
        ).strip()

        output_path = (
            Path(output_value)
            .expanduser()
            .resolve()
            if output_value
            else default_output_path().resolve()
        )

        queryset = (
            GeneratedAutomotiveNews.objects
            .filter(
                status__in=statuses,
            )
            .select_related(
                "brief",
            )
            .order_by(
                "-published_at",
                "-created_at",
                "-id",
            )
        )

        news_items = list(
            queryset[:limit]
        )

        items = [
            build_item(news)
            for news in news_items
        ]

        payload = {
            "source": FEED_SOURCE,
            "created_at": (
                timezone.now().isoformat()
            ),
            "items": items,
        }

        if options["dry_run"]:
            self.stdout.write(
                json.dumps(
                    payload,
                    ensure_ascii=False,
                    indent=2,
                )
            )

            return

        write_atomic_json(
            output_path,
            payload,
        )

        if options["mark_published"]:
            approved_ids = [
                news.pk
                for news in news_items
                if (
                    news.status
                    == AutomotiveNewsStatus.APPROVED
                )
            ]

            if approved_ids:
                now = timezone.now()

                with transaction.atomic():
                    (
                        GeneratedAutomotiveNews
                        .objects
                        .filter(
                            id__in=approved_ids,
                            status=(
                                AutomotiveNewsStatus.APPROVED
                            ),
                        )
                        .update(
                            status=(
                                AutomotiveNewsStatus.PUBLISHED
                            ),
                            published_at=now,
                        )
                    )

        self.stdout.write(
            self.style.SUCCESS(
                f"Automotive feed written: "
                f"{output_path}"
            )
        )

        self.stdout.write(
            f"Items: {len(items)}"
        )

        self.stdout.write(
            "Statuses: "
            + ", ".join(statuses)
        )

        if options["mark_published"]:
            self.stdout.write(
                "Exported approved items were "
                "marked as published."
            )
