from __future__ import annotations

import re

from django.core.management.base import (
    BaseCommand,
    CommandError,
)
from django.db import transaction

from intel.automotive_editorial_validation import (
    minimum_body_chars_for_brief,
    validate_automotive_news,
)
from intel.models import (
    AutomotiveNewsStatus,
    GeneratedAutomotiveNews,
)


def parse_ids(raw_value: str) -> list[int]:
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
                "Некорректный GeneratedAutomotiveNews "
                f"ID: {token}"
            ) from exc

        if value <= 0:
            raise CommandError(
                "Некорректный GeneratedAutomotiveNews "
                f"ID: {token}"
            )

        result.append(value)

    return list(
        dict.fromkeys(result)
    )


def approval_errors(
    news: GeneratedAutomotiveNews,
    *,
    min_score: int,
    min_body_chars: int,
) -> list[str]:
    errors: list[str] = []

    if not news.brief_id:
        errors.append(
            "missing-brief"
        )
        return errors

    effective_min_body_chars = (
        min_body_chars
        if min_body_chars > 0
        else minimum_body_chars_for_brief(
            news.brief
        )
    )

    if news.quality_score < min_score:
        errors.append(
            f"low-quality-score:{news.quality_score}"
        )

    if len(
        str(
            news.source_note or ""
        ).strip()
    ) < 20:
        errors.append(
            "short-source-note"
        )

    errors.extend(
        validate_automotive_news(
            brief=news.brief,
            title=news.title,
            body=news.body,
            meta_description=(
                news.meta_description
            ),
            image_topic=news.image_topic,
            source_urls=news.source_urls,
            min_body_chars=(
                effective_min_body_chars
            ),
        )
    )

    return list(
        dict.fromkeys(errors)
    )


class Command(BaseCommand):
    help = (
        "Автоматически одобряет качественные "
        "GeneratedAutomotiveNews из статуса review."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--limit",
            type=int,
            default=100,
        )

        parser.add_argument(
            "--min-score",
            type=int,
            default=70,
        )

        parser.add_argument(
            "--min-body-chars",
            type=int,
            default=0,
            help=(
                "Явный минимальный объём. "
                "Значение 0 использует динамический "
                "порог конкретного AutomotiveBrief."
            ),
        )

        parser.add_argument(
            "--news-ids",
            default="",
            help=(
                "ID GeneratedAutomotiveNews "
                "через запятую или пробел."
            ),
        )

        parser.add_argument(
            "--dry-run",
            action="store_true",
        )

        parser.add_argument(
            "--show-approved",
            action="store_true",
        )

        parser.add_argument(
            "--show-skipped",
            action="store_true",
        )

    def handle(self, *args, **options):
        limit = max(
            int(options["limit"]),
            1,
        )

        min_score = min(
            max(
                int(options["min_score"]),
                0,
            ),
            100,
        )

        min_body_chars = max(
            int(options["min_body_chars"]),
            0,
        )

        requested_ids = parse_ids(
            options["news_ids"]
        )

        queryset = (
            GeneratedAutomotiveNews.objects
            .filter(
                status=(
                    AutomotiveNewsStatus.REVIEW
                ),
            )
            .select_related(
                "brief",
                "brief__event",
            )
            .order_by("id")
        )

        if requested_ids:
            queryset = queryset.filter(
                id__in=requested_ids,
            )

        news_items = list(
            queryset[:limit]
        )

        checked = 0
        approved = 0
        skipped = 0

        for news in news_items:
            checked += 1

            errors = approval_errors(
                news,
                min_score=min_score,
                min_body_chars=(
                    min_body_chars
                ),
            )

            if errors:
                skipped += 1

                if options["show_skipped"]:
                    self.stdout.write(
                        self.style.WARNING(
                            f"SKIP #{news.pk}: "
                            f"{', '.join(errors[:10])} | "
                            f"{news.title[:120]}"
                        )
                    )

                continue

            approved += 1

            if options["dry_run"]:
                if options["show_approved"]:
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"WOULD APPROVE #{news.pk}: "
                            f"score={news.quality_score} | "
                            f"{news.title[:120]}"
                        )
                    )

                continue

            with transaction.atomic():
                updated = (
                    GeneratedAutomotiveNews
                    .objects
                    .filter(
                        pk=news.pk,
                        status=(
                            AutomotiveNewsStatus.REVIEW
                        ),
                    )
                    .update(
                        status=(
                            AutomotiveNewsStatus.APPROVED
                        ),
                    )
                )

            if updated and options["show_approved"]:
                self.stdout.write(
                    self.style.SUCCESS(
                        f"APPROVED #{news.pk}: "
                        f"score={news.quality_score} | "
                        f"{news.title[:120]}"
                    )
                )

        if not news_items:
            self.stdout.write(
                self.style.WARNING(
                    "Нет автомобильных новостей "
                    "в статусе review."
                )
            )

        self.stdout.write("")
        self.stdout.write(
            f"Checked: {checked}"
        )
        self.stdout.write(
            f"Approved: {approved}"
        )
        self.stdout.write(
            f"Skipped: {skipped}"
        )

        if options["dry_run"]:
            self.stdout.write(
                self.style.NOTICE(
                    "Dry-run: статусы не изменены."
                )
            )
