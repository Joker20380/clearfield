from __future__ import annotations

import re
from pathlib import Path

from django.core.management.base import (
    BaseCommand,
    CommandError,
)
from django.utils import timezone

from intel.medical_semantic_matching import (
    SemanticCatalog,
    load_semantic_feed,
)
from intel.models import MedicalBrief


DEFAULT_FEED = "var/medical-semantic-feed.json"


def parse_ids(raw_value: str) -> list[int]:
    values = []
    seen = set()

    for token in re.split(
        r"[\s,;]+",
        str(raw_value or "").strip(),
    ):
        if not token:
            continue

        try:
            value = int(token)
        except ValueError as exc:
            raise CommandError(
                f"Некорректный ID brief: {token!r}"
            ) from exc

        if value <= 0:
            raise CommandError(
                f"ID brief должен быть положительным: {value}"
            )

        if value in seen:
            continue

        seen.add(value)
        values.append(value)

    return values


class Command(BaseCommand):
    help = (
        "Подбирает для MedicalBrief релевантную карточку "
        "анализа из semantic feed КДЛ «Дзагуров»."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--feed",
            default=DEFAULT_FEED,
            help=(
                "Путь к semantic feed. "
                f"По умолчанию: {DEFAULT_FEED}"
            ),
        )
        parser.add_argument(
            "--status",
            default="ready",
            help=(
                "Статус MedicalBrief для обработки. "
                "Игнорируется при использовании --brief-ids."
            ),
        )
        parser.add_argument(
            "--brief-ids",
            default="",
            help=(
                "ID MedicalBrief через запятую, "
                "пробел или точку с запятой."
            ),
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=20,
            help="Максимальное число заданий. По умолчанию: 20.",
        )
        parser.add_argument(
            "--top",
            type=int,
            default=5,
            help="Число выводимых кандидатов. По умолчанию: 5.",
        )
        parser.add_argument(
            "--min-score",
            type=int,
            default=230,
            help=(
                "Минимальная оценка для назначения. "
                "По умолчанию: 230."
            ),
        )
        parser.add_argument(
            "--min-margin",
            type=int,
            default=20,
            help=(
                "Минимальный отрыв от второго кандидата. "
                "По умолчанию: 20."
            ),
        )
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Записать принятые назначения в базу.",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help=(
                "Повторно оценивать brief, у которых "
                "уже назначена semantic-панель."
            ),
        )

    def handle(self, *args, **options):
        feed_path = Path(
            options["feed"]
        ).expanduser()

        if not feed_path.is_absolute():
            feed_path = Path.cwd() / feed_path

        try:
            payload = load_semantic_feed(
                feed_path
            )
        except ValueError as exc:
            raise CommandError(str(exc)) from exc

        catalog = SemanticCatalog(payload)

        requested_ids = parse_ids(
            options["brief_ids"]
        )

        limit = options["limit"]
        top_count = options["top"]
        min_score = options["min_score"]
        min_margin = options["min_margin"]
        apply_changes = options["apply"]
        force = options["force"]

        if limit <= 0:
            raise CommandError(
                "--limit должен быть больше нуля."
            )

        if top_count <= 0:
            raise CommandError(
                "--top должен быть больше нуля."
            )

        queryset = MedicalBrief.objects.all()

        if requested_ids:
            queryset = queryset.filter(
                id__in=requested_ids
            )
        else:
            queryset = queryset.filter(
                status=options["status"]
            )

        if not force:
            queryset = queryset.filter(
                semantic_panel_id__isnull=True
            )

        briefs = list(
            queryset
            .order_by("-created_at", "-id")[:limit]
        )

        self.stdout.write(
            f"Feed: {feed_path}"
        )
        self.stdout.write(
            f"Feed items: {payload['item_count']}"
        )
        self.stdout.write(
            f"Feed SHA256: {payload['content_sha256']}"
        )
        self.stdout.write(
            f"Briefs selected: {len(briefs)}"
        )
        self.stdout.write(
            "Mode: APPLY"
            if apply_changes
            else "Mode: DRY RUN"
        )

        checked = 0
        accepted = 0
        ambiguous = 0
        weak = 0
        updated = 0

        for brief in briefs:
            checked += 1

            profiles = catalog.detect_profiles(
                brief
            )

            if not profiles:
                weak += 1

                self.stdout.write("")
                self.stdout.write("=" * 100)
                self.stdout.write(
                    f"Brief #{brief.id} [{brief.status}]"
                )
                self.stdout.write(
                    f"Title: {brief.title}"
                )
                self.stdout.write(
                    f"Target: "
                    f"{brief.target_keyword or '—'}"
                )
                self.stdout.write(
                    self.style.WARNING(
                        "NO SUPPORTED CLINICAL PROFILE"
                    )
                )
                self.stdout.write(
                    "Общие слова и target_keyword "
                    "не используются как основание "
                    "для назначения ссылки."
                )
                continue

            ranked = catalog.rank(
                brief,
                top_n=top_count,
            )

            if not ranked:
                weak += 1

                self.stdout.write("")
                self.stdout.write("=" * 100)
                self.stdout.write(
                    f"Brief #{brief.id} [{brief.status}]"
                )
                self.stdout.write(
                    f"Title: {brief.title}"
                )
                self.stdout.write(
                    self.style.WARNING(
                        "PROFILE FOUND, "
                        "BUT NO ALLOWED PANEL CANDIDATE"
                    )
                )
                continue

            winner = ranked[0]

            runner_up_score = (
                ranked[1].score
                if len(ranked) > 1
                else 0
            )

            margin = (
                winner.score
                - runner_up_score
            )

            score_ok = (
                winner.score >= min_score
            )
            margin_ok = (
                margin >= min_margin
            )

            is_accepted = (
                score_ok and margin_ok
            )

            if is_accepted:
                accepted += 1
                status_label = "ACCEPT"
                style = self.style.SUCCESS
            elif score_ok:
                ambiguous += 1
                status_label = "AMBIGUOUS"
                style = self.style.WARNING
            else:
                weak += 1
                status_label = "WEAK"
                style = self.style.WARNING

            self.stdout.write("")
            self.stdout.write("=" * 100)
            self.stdout.write(
                f"Brief #{brief.id} [{brief.status}]"
            )
            self.stdout.write(
                f"Title: {brief.title}"
            )
            self.stdout.write(
                f"Target: {brief.target_keyword or '—'}"
            )
            self.stdout.write(
                style(
                    f"{status_label}: "
                    f"score={winner.score}, "
                    f"margin={margin}, "
                    f"profiles="
                    f"{', '.join(winner.profiles) or '—'}"
                )
            )

            for position, result in enumerate(
                ranked,
                start=1,
            ):
                item = result.item

                self.stdout.write(
                    f"{position}. "
                    f"score={result.score} | "
                    f"{item.get('code')} | "
                    f"{item.get('title')}"
                )
                self.stdout.write(
                    f"   {item.get('url')}"
                )

                for reason in result.reasons:
                    self.stdout.write(
                        f"   - {reason}"
                    )

            if not (
                apply_changes
                and is_accepted
            ):
                continue

            item = winner.item

            brief.semantic_panel_id = item[
                "panel_id"
            ]
            brief.semantic_panel_code = str(
                item.get("code") or ""
            )
            brief.semantic_panel_title = str(
                item.get("title") or ""
            )
            brief.semantic_panel_url = str(
                item.get("url") or ""
            )
            brief.semantic_anchor = str(
                item.get("canonical_anchor")
                or item.get("title")
                or ""
            )
            brief.semantic_score = (
                winner.score
            )
            brief.semantic_match_details = {
                "algorithm": (
                    "medical-semantic-deterministic-v1"
                ),
                "score": winner.score,
                "margin": margin,
                "profiles": list(
                    winner.profiles
                ),
                "reasons": list(
                    winner.reasons
                ),
                "runner_up": (
                    {
                        "panel_id": (
                            ranked[1].item.get(
                                "panel_id"
                            )
                        ),
                        "code": (
                            ranked[1].item.get(
                                "code"
                            )
                        ),
                        "title": (
                            ranked[1].item.get(
                                "title"
                            )
                        ),
                        "score": ranked[1].score,
                    }
                    if len(ranked) > 1
                    else None
                ),
                "feed_generated_at": payload.get(
                    "generated_at"
                ),
            }
            brief.semantic_feed_sha256 = str(
                payload["content_sha256"]
            )
            brief.semantic_assigned_at = (
                timezone.now()
            )

            brief.save(
                update_fields=[
                    "semantic_panel_id",
                    "semantic_panel_code",
                    "semantic_panel_title",
                    "semantic_panel_url",
                    "semantic_anchor",
                    "semantic_score",
                    "semantic_match_details",
                    "semantic_feed_sha256",
                    "semantic_assigned_at",
                    "updated_at",
                ]
            )

            updated += 1

        self.stdout.write("")
        self.stdout.write("=" * 100)
        self.stdout.write(
            f"Checked: {checked}"
        )
        self.stdout.write(
            f"Accepted: {accepted}"
        )
        self.stdout.write(
            f"Ambiguous: {ambiguous}"
        )
        self.stdout.write(
            f"Weak: {weak}"
        )
        self.stdout.write(
            f"Updated: {updated}"
        )

        if not apply_changes:
            self.stdout.write(
                self.style.NOTICE(
                    "DRY RUN: база данных не изменялась."
                )
            )
