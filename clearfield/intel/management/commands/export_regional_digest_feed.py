import json
import os
import re
import secrets
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from intel.models import RegionalDigest


SOURCE_ID_OFFSET = 1_000_000_000
MAX_SOURCE_ID = 2_147_483_647


def normalize_title(value):
    return " ".join(str(value or "").lower().split())


def get_or_create_token(public_dir):
    token_file = public_dir / ".regional_digest_feed_token"

    if token_file.exists():
        token = token_file.read_text(
            encoding="utf-8"
        ).strip()

        if token:
            return token

    token = secrets.token_hex(16)

    token_file.write_text(
        token,
        encoding="utf-8",
    )

    token_file.chmod(0o600)

    return token


def local_date_label(value):
    if value is None:
        return ""

    if timezone.is_naive(value):
        value = timezone.make_aware(
            value,
            timezone.get_current_timezone(),
        )

    return timezone.localtime(value).strftime(
        "%d.%m.%Y"
    )


def build_export_title(digest, seen_titles):
    base = str(digest.title or "").strip()

    date_value = (
        digest.period_end
        or digest.published_at
        or digest.created_at
    )

    date_label = local_date_label(date_value)

    if (
        date_label
        and not re.search(
            r"\b\d{2}\.\d{2}\.\d{4}\b",
            base,
        )
    ):
        candidate = f"{base} — {date_label}"
    else:
        candidate = base

    key = normalize_title(candidate)

    if key in seen_titles:
        candidate = (
            f"{candidate} — выпуск {digest.id}"
        )
        key = normalize_title(candidate)

    seen_titles.add(key)

    return candidate


def get_image_topic(digest):
    for payload in (
        digest.source_map,
        digest.criteria,
        digest.evidence_pack,
    ):
        if not isinstance(payload, dict):
            continue

        value = str(
            payload.get("image_topic") or ""
        ).strip()

        if value:
            return value

    if digest.topic == "medicine":
        return "healthcare_region"

    return "general_medical_news"


class Command(BaseCommand):
    help = (
        "Export RegionalDigest records to a separate "
        "public JSON feed."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--status",
            default="published",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=365,
        )
        parser.add_argument(
            "--public-dir",
            default="../generated-news",
        )
        parser.add_argument(
            "--filename",
            default="",
        )
        parser.add_argument(
            "--source-id-offset",
            type=int,
            default=SOURCE_ID_OFFSET,
        )
        parser.add_argument(
            "--show-content-size",
            action="store_true",
        )

    def handle(self, *args, **options):
        status = str(
            options["status"] or ""
        ).strip()

        limit = int(options["limit"])
        offset = int(options["source_id_offset"])

        if not status:
            raise CommandError(
                "Status must not be empty."
            )

        if limit <= 0:
            raise CommandError(
                "Limit must be greater than zero."
            )

        if offset <= 0:
            raise CommandError(
                "Source ID offset must be positive."
            )

        public_dir = Path(
            options["public_dir"]
        ).resolve()

        public_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        filename = str(
            options["filename"] or ""
        ).strip()

        if not filename:
            token = get_or_create_token(
                public_dir
            )

            filename = (
                f"regional-digest-feed-"
                f"{token}.json"
            )

        output_path = public_dir / filename

        queryset = (
            RegionalDigest.objects
            .filter(status=status)
            .order_by("-period_end", "-id")
        )

        # Рабочий feed не должен публиковать запись,
        # для которой не зафиксирован момент публикации.
        if status == "published":
            queryset = queryset.exclude(
                published_at__isnull=True
            )

        queryset = queryset[:limit]

        items = []
        seen_titles = set()

        for digest in queryset:
            title = str(
                digest.title or ""
            ).strip()

            content = str(
                digest.body or ""
            ).strip()

            if not title or not content:
                self.stdout.write(
                    self.style.WARNING(
                        f"SKIP RegionalDigest "
                        f"#{digest.id}: "
                        f"empty title/body"
                    )
                )
                continue

            source_id = offset + digest.id

            if source_id > MAX_SOURCE_ID:
                raise CommandError(
                    f"source_id={source_id} exceeds "
                    f"PostgreSQL integer limit"
                )

            export_title = build_export_title(
                digest,
                seen_titles,
            )

            created_at = (
                digest.published_at
                or digest.created_at
                or timezone.now()
            )

            region_label = str(
                digest.region_label or ""
            ).strip()

            topic = str(
                digest.topic or ""
            ).strip()

            items.append({
                "source_id": source_id,
                "regional_digest_id": digest.id,
                "title": export_title,
                "content": content,
                "slug": str(
                    digest.slug or ""
                ).strip(),
                "meta_description": str(
                    digest.meta_description or ""
                ).strip(),
                "target_keyword": (
                    f"медицинские новости "
                    f"{region_label}"
                ).strip(),
                "theme": (
                    f"regional_{topic}"
                    if topic
                    else "regional_digest"
                ),
                "image_topic": get_image_topic(
                    digest
                ),
                "status": digest.status,
                "region_code": str(
                    digest.region_code or ""
                ).strip(),
                "region_label": region_label,
                "topic": topic,
                "period_start": (
                    digest.period_start.isoformat()
                    if digest.period_start
                    else None
                ),
                "period_end": (
                    digest.period_end.isoformat()
                    if digest.period_end
                    else None
                ),
                "created_at": (
                    created_at.isoformat()
                ),
            })

        payload = {
            "source": (
                "clearfield_regional_digest"
            ),
            "status": status,
            "created_at": (
                timezone.now().isoformat()
            ),
            "source_id_offset": offset,
            "items": items,
        }

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
                ),
                encoding="utf-8",
            )

            temp_path.chmod(0o644)
            temp_path.replace(output_path)
        finally:
            if temp_path.exists():
                temp_path.unlink()

        self.stdout.write(
            self.style.SUCCESS(
                f"Exported: {len(items)}"
            )
        )

        self.stdout.write(
            f"Status: {status}"
        )

        self.stdout.write(
            f"Path: {output_path}"
        )

        self.stdout.write(
            f"Filename: {filename}"
        )

        if options["show_content_size"]:
            self.stdout.write(
                f"Size: "
                f"{output_path.stat().st_size} bytes"
            )
