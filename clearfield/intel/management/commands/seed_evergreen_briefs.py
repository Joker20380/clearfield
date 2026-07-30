from __future__ import annotations

import re

from django.core.management.base import BaseCommand
from django.db import transaction

from intel.evergreen_catalog import (
    AUTOMOTIVE_TOPICS,
    COMMON_SAFETY_NOTES,
    MEDICAL_TOPICS,
)
from intel.models import (
    AutomotiveBrief,
    AutomotiveBriefStatus,
    MedicalBrief,
    MedicalBriefStatus,
)


def normalize_cluster(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().casefold())


def seed_topics(*, model, topics, status, defaults, apply_changes):
    existing = {
        normalize_cluster(value)
        for value in model.objects.values_list("target_keyword", flat=True)
        if normalize_cluster(value)
    }
    created_ids = []
    skipped = 0

    for topic in topics:
        cluster = normalize_cluster(topic["target_keyword"])
        if cluster in existing:
            skipped += 1
            continue

        if apply_changes:
            payload = {**defaults, **topic, "status": status}
            obj = model.objects.create(**payload)
            created_ids.append(obj.pk)

        existing.add(cluster)

    return created_ids, skipped, len(topics) - skipped


class Command(BaseCommand):
    help = (
        "Создаёт идемпотентные evergreen SEO-задания. "
        "Команда никогда не генерирует и не публикует материалы."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--kind",
            choices=("all", "medical", "automotive"),
            default="all",
        )
        parser.add_argument(
            "--status",
            choices=("draft", "ready"),
            default="draft",
            help="Безопасный статус по умолчанию: draft.",
        )
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Записать задания в БД; без флага выполняется dry-run.",
        )

    def handle(self, *args, **options):
        apply_changes = bool(options["apply"])
        kind = options["kind"]
        status = options["status"]
        results = []

        with transaction.atomic():
            if kind in ("all", "medical"):
                results.append(
                    (
                        "medical",
                        *seed_topics(
                            model=MedicalBrief,
                            topics=MEDICAL_TOPICS,
                            status=MedicalBriefStatus(status),
                            defaults={
                                "audience": "пациенты медицинской лаборатории",
                                "region_text": "Владикавказ и Северная Осетия",
                                "safety_notes": COMMON_SAFETY_NOTES,
                                "disclaimer_required": True,
                            },
                            apply_changes=apply_changes,
                        ),
                    )
                )

            if kind in ("all", "automotive"):
                results.append(
                    (
                        "automotive",
                        *seed_topics(
                            model=AutomotiveBrief,
                            topics=AUTOMOTIVE_TOPICS,
                            status=AutomotiveBriefStatus(status),
                            defaults={
                                "source_urls": "https://diagnost-rso.ru/",
                                "audience": "автовладельцы и клиенты автосервиса",
                                "region_text": "Владикавказ и Северная Осетия",
                                "safety_notes": COMMON_SAFETY_NOTES,
                                "disclaimer_required": True,
                            },
                            apply_changes=apply_changes,
                        ),
                    )
                )

        mode = "APPLY" if apply_changes else "DRY RUN"
        self.stdout.write(f"Mode: {mode}; target status: {status}")
        for label, ids, skipped, available in results:
            self.stdout.write(
                f"{label}: new={available}, skipped={skipped}, "
                f"created_ids={','.join(map(str, ids)) or '-'}"
            )
