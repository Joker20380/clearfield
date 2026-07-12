from __future__ import annotations

from dataclasses import dataclass

from django.core.management.base import (
    BaseCommand,
    CommandError,
)
from django.db import transaction

from intel.models import (
    Cadence,
    Region,
    Source,
    SourceClass,
    Topic,
)


@dataclass(frozen=True)
class AutomotiveSourceSpec:
    name: str
    url: str
    source_class: str
    cadence: str


AUTOMOTIVE_SOURCES = (
    AutomotiveSourceSpec(
        name="АВТОСТАТ — Новости",
        url=(
            "https://www.autostat.ru/"
            "news/rss/3/"
        ),
        source_class=SourceClass.INDUSTRY,
        cadence=Cadence.MEDIUM,
    ),
    AutomotiveSourceSpec(
        name="Drom.ru — Новости",
        url=(
            "https://www.drom.ru/"
            "export/xml/news.rss"
        ),
        source_class=SourceClass.INDUSTRY,
        cadence=Cadence.MEDIUM,
    ),
)


def expected_values(
    spec: AutomotiveSourceSpec,
) -> dict:
    return {
        "name": spec.name,
        "region": Region.RU,
        "topic": Topic.AUTO,
        "source_class": spec.source_class,
        "cadence": spec.cadence,
        "is_enabled": True,
    }


def changed_fields(
    source: Source,
    expected: dict,
) -> dict:
    changes = {}

    for field_name, expected_value in (
        expected.items()
    ):
        current_value = getattr(
            source,
            field_name,
        )

        if current_value != expected_value:
            changes[field_name] = (
                expected_value
            )

    return changes


def verify_no_name_conflict(
    spec: AutomotiveSourceSpec,
) -> None:
    conflict = (
        Source.objects
        .filter(name=spec.name)
        .exclude(url=spec.url)
        .first()
    )

    if conflict:
        raise CommandError(
            "Source name conflict: "
            f"{spec.name!r} already belongs "
            f"to Source #{conflict.pk} "
            f"with URL {conflict.url!r}"
        )


class Command(BaseCommand):
    help = (
        "Создаёт или обновляет проверенные "
        "российские автомобильные RSS-источники."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help=(
                "Показать изменения без записи "
                "в базу данных."
            ),
        )

    def handle(self, *args, **options):
        dry_run = bool(
            options["dry_run"]
        )

        created = 0
        updated = 0
        unchanged = 0

        for spec in AUTOMOTIVE_SOURCES:
            verify_no_name_conflict(spec)

            expected = expected_values(
                spec
            )

            existing = (
                Source.objects
                .filter(url=spec.url)
                .first()
            )

            if existing is None:
                created += 1

                prefix = (
                    "WOULD CREATE"
                    if dry_run
                    else "CREATE"
                )

                self.stdout.write(
                    self.style.SUCCESS(
                        f"{prefix}: "
                        f"{spec.name} | "
                        f"{spec.url}"
                    )
                )

                if not dry_run:
                    with transaction.atomic():
                        Source.objects.create(
                            url=spec.url,
                            **expected,
                        )

                continue

            if (
                existing.topic
                not in {
                    "",
                    Topic.AUTO,
                }
            ):
                raise CommandError(
                    "Refusing to change topic of "
                    f"Source #{existing.pk}: "
                    f"{existing.topic!r} -> "
                    f"{Topic.AUTO!r}"
                )

            changes = changed_fields(
                existing,
                expected,
            )

            if not changes:
                unchanged += 1

                self.stdout.write(
                    f"UNCHANGED: "
                    f"{spec.name} | "
                    f"{spec.url}"
                )

                continue

            updated += 1

            changed_names = ", ".join(
                sorted(changes)
            )

            prefix = (
                "WOULD UPDATE"
                if dry_run
                else "UPDATE"
            )

            self.stdout.write(
                self.style.WARNING(
                    f"{prefix}: "
                    f"Source #{existing.pk} | "
                    f"fields={changed_names}"
                )
            )

            if not dry_run:
                with transaction.atomic():
                    (
                        Source.objects
                        .filter(pk=existing.pk)
                        .update(**changes)
                    )

        self.stdout.write("")
        self.stdout.write(
            f"Created: {created}"
        )
        self.stdout.write(
            f"Updated: {updated}"
        )
        self.stdout.write(
            f"Unchanged: {unchanged}"
        )

        if dry_run:
            self.stdout.write(
                self.style.NOTICE(
                    "Dry-run: база данных "
                    "не изменена."
                )
            )
