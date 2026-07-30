import re
from urllib.parse import urlsplit

from django.core.management.base import (
    BaseCommand,
)
from django.db import transaction

from intel.models import (
    AutomotiveBrief,
    AutomotiveBriefStatus,
    Topic,
)


NOISE_TITLES = (
    "все новости",
    "главная",
    "архив",
    "контакты",
    "мероприятия",
    "подписаться",
)


DANGEROUS_PROMISES = (
    "гарантированно устранит",
    "точно определит неисправность",
    "ремонт точно не требуется",
    "можно продолжать движение",
    "безопасно продолжать движение",
    "деталь необходимо заменить",
)

SERVICE_SEARCH_TERMS = (
    "ремонт",
    "обслуживан",
    "диагност",
    "неисправ",
    "код ошиб",
    "автосервис",
    "замена",
    "износ",
    "поломк",
    "стук",
    "люфт",
    "вибрац",
    "перегрев",
    "утечк",
    "техосмотр",
    "тормоз",
    "подвес",
    "двигател",
    "автоэлект",
)


def normalize(value):
    text = str(value or "").lower()
    text = text.replace("ё", "е")
    text = re.sub(
        r"\s+",
        " ",
        text,
    )
    return text.strip()


def split_lines(value):
    result = []

    for line in str(
        value or ""
    ).splitlines():
        clean = line.strip()

        if clean.startswith("-"):
            clean = clean[1:].strip()

        if clean:
            result.append(clean)

    return result


def valid_http_url(value):
    try:
        parsed = urlsplit(
            str(value or "").strip()
        )
    except ValueError:
        return False

    return (
        parsed.scheme in {
            "http",
            "https",
        }
        and bool(parsed.netloc)
    )


def audit_brief(
    brief,
    min_title_chars,
    min_facts_chars,
    allow_unlinked,
):
    reasons = []

    title = str(
        brief.title or ""
    ).strip()

    facts = str(
        brief.facts or ""
    ).strip()

    target_keyword = str(
        brief.target_keyword or ""
    ).strip()

    angle = str(
        brief.angle or ""
    ).strip()

    source_urls = split_lines(
        brief.source_urls
    )

    normalized_title = normalize(title)

    if len(title) < min_title_chars:
        reasons.append(
            f"short-title:{len(title)}"
        )

    if any(
        noise == normalized_title
        or normalized_title.startswith(
            noise + " "
        )
        for noise in NOISE_TITLES
    ):
        reasons.append(
            "navigation-title"
        )

    if len(facts) < min_facts_chars:
        reasons.append(
            f"short-facts:{len(facts)}"
        )

    if len(target_keyword) < 8:
        reasons.append(
            "missing-target-keyword"
        )

    if len(angle) < 40:
        reasons.append(
            "weak-angle"
        )

    if not brief.secondary_keywords.strip():
        reasons.append(
            "missing-secondary-keywords"
        )

    if not brief.region_text.strip():
        reasons.append(
            "missing-region-context"
        )

    if not brief.safety_notes.strip():
        reasons.append(
            "missing-safety-notes"
        )

    if not source_urls:
        reasons.append(
            "missing-source-urls"
        )
    else:
        invalid_urls = [
            url
            for url in source_urls
            if not valid_http_url(url)
        ]

        if invalid_urls:
            reasons.append(
                "invalid-source-url"
            )

    if brief.event_id:
        if brief.event.topic != Topic.AUTO:
            reasons.append(
                f"wrong-event-topic:{brief.event.topic}"
            )
    elif not allow_unlinked:
        reasons.append(
            "missing-event"
        )

    generated_text = normalize(
        "\n".join(
            [
                title,
                angle,
                facts,
            ]
        )
    )

    if not any(
        term in generated_text
        for term in SERVICE_SEARCH_TERMS
    ):
        reasons.append(
            "no-service-search-intent"
        )

    for phrase in DANGEROUS_PROMISES:
        if normalize(phrase) in generated_text:
            reasons.append(
                "unsafe-promise"
            )
            break

    return reasons


class Command(BaseCommand):
    help = (
        "Audit AutomotiveBrief quality before "
        "automotive news generation."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--status",
            default=AutomotiveBriefStatus.READY,
            choices=[
                AutomotiveBriefStatus.DRAFT,
                AutomotiveBriefStatus.READY,
                AutomotiveBriefStatus.USED,
                AutomotiveBriefStatus.REJECTED,
            ],
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=100,
        )
        parser.add_argument(
            "--min-title-chars",
            type=int,
            default=20,
        )
        parser.add_argument(
            "--min-facts-chars",
            type=int,
            default=80,
        )
        parser.add_argument(
            "--allow-unlinked",
            action="store_true",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
        )
        parser.add_argument(
            "--show-accepted",
            action="store_true",
        )
        parser.add_argument(
            "--show-rejected",
            action="store_true",
        )

    def handle(self, *args, **options):
        limit = max(
            int(options["limit"]),
            1,
        )

        min_title_chars = max(
            int(options["min_title_chars"]),
            1,
        )

        min_facts_chars = max(
            int(options["min_facts_chars"]),
            1,
        )

        queryset = (
            AutomotiveBrief.objects
            .filter(
                status=options["status"],
            )
            .select_related("event")
            .order_by("id")[:limit]
        )

        briefs = list(queryset)

        checked = 0
        accepted = 0
        rejected = 0

        for brief in briefs:
            checked += 1

            reasons = audit_brief(
                brief,
                min_title_chars=(
                    min_title_chars
                ),
                min_facts_chars=(
                    min_facts_chars
                ),
                allow_unlinked=options[
                    "allow_unlinked"
                ],
            )

            if reasons:
                rejected += 1

                if options["show_rejected"]:
                    self.stdout.write(
                        self.style.WARNING(
                            f"REJECT #{brief.id}: "
                            f"{', '.join(reasons)} | "
                            f"{brief.title[:130]}"
                        )
                    )

                if not options["dry_run"]:
                    with transaction.atomic():
                        AutomotiveBrief.objects.filter(
                            pk=brief.pk,
                        ).update(
                            status=(
                                AutomotiveBriefStatus.REJECTED
                            ),
                            used_at=None,
                        )

                continue

            accepted += 1

            if options["show_accepted"]:
                self.stdout.write(
                    self.style.SUCCESS(
                        f"ACCEPT #{brief.id}: "
                        f"{brief.title[:130]}"
                    )
                )

        self.stdout.write("")
        self.stdout.write(
            f"Checked: {checked}"
        )
        self.stdout.write(
            f"Accepted: {accepted}"
        )
        self.stdout.write(
            f"Rejected: {rejected}"
        )

        if options["dry_run"]:
            self.stdout.write(
                self.style.NOTICE(
                    "Dry-run: статусы не изменены."
                )
            )
