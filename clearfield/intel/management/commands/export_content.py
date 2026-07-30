from __future__ import annotations

import json
import os
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from intel.models import GeneratedContent, GeneratedContentStatus


class Command(BaseCommand):
    help = "Экспортирует одобренные материалы универсального движка в JSON."

    def add_arguments(self, parser):
        parser.add_argument("--project", required=True)
        parser.add_argument("--output", required=True)
        parser.add_argument("--limit", type=int, default=100)
        parser.add_argument("--dry-run", action="store_true")

    def handle(self, *args, **options):
        project_key = options["project"].strip()
        if not project_key:
            raise CommandError("--project не может быть пустым")

        queryset = (
            GeneratedContent.objects
            .filter(
                brief__project__key=project_key,
                status__in=[
                    GeneratedContentStatus.APPROVED,
                    GeneratedContentStatus.PUBLISHED,
                ],
            )
            .select_related("brief", "brief__project", "brief__template")
            .order_by("-created_at")[: max(options["limit"], 1)]
        )
        items = []

        for item in queryset:
            template = item.brief.template
            if template.expert_review_required and not (
                item.reviewer_name and item.reviewed_at
            ):
                self.stdout.write(
                    self.style.WARNING(
                        f"SKIP #{item.pk}: expert-review-metadata-required"
                    )
                )
                continue

            items.append(
                {
                    "source_id": f"content-{item.pk}",
                    "project": project_key,
                    "content_type": template.content_type,
                    "title": item.title,
                    "slug": item.slug,
                    "meta_description": item.meta_description,
                    "body_markdown": item.body,
                    "primary_keyword": item.brief.primary_keyword,
                    "search_intent": item.brief.search_intent,
                    "source_urls": item.source_urls,
                    "quality_score": item.quality_score,
                    "reviewer_name": item.reviewer_name,
                    "reviewed_at": (
                        item.reviewed_at.isoformat()
                        if item.reviewed_at
                        else None
                    ),
                    "created_at": item.created_at.isoformat(),
                    "updated_at": item.updated_at.isoformat(),
                }
            )

        payload = {
            "schema_version": 1,
            "project": project_key,
            "item_count": len(items),
            "items": items,
        }
        encoded = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"

        if options["dry_run"]:
            self.stdout.write(encoded)
            return

        output = Path(options["output"]).expanduser().resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_name(f".{output.name}.{os.getpid()}.tmp")
        try:
            temporary.write_text(encoded, encoding="utf-8")
            os.chmod(temporary, 0o640)
            os.replace(temporary, output)
        finally:
            temporary.unlink(missing_ok=True)

        self.stdout.write(
            self.style.SUCCESS(f"Exported: {len(items)} -> {output}")
        )
