from django.core.management.base import (
    BaseCommand,
    CommandError,
)
from django.db import transaction
from django.utils import timezone

from intel.models import (
    RegionalDigest,
    RegionalDigestStatus,
)


def text(value):
    return str(value or "").strip()


def validate_digest(digest):
    errors = []

    if digest.status != RegionalDigestStatus.REVIEW:
        errors.append(
            f"status must be review, got {digest.status!r}"
        )

    if digest.published_at is not None:
        errors.append("published_at must be empty")

    required_text = {
        "title": digest.title,
        "slug": digest.slug,
        "meta_description": digest.meta_description,
        "body": digest.body,
    }

    for field_name, value in required_text.items():
        if not text(value):
            errors.append(f"{field_name} is empty")

    if len(text(digest.body)) < 500:
        errors.append("body is shorter than 500 characters")

    if text(digest.llm_error):
        errors.append(
            f"llm_error is not empty: {digest.llm_error!r}"
        )

    criteria = (
        digest.criteria
        if isinstance(digest.criteria, dict)
        else {}
    )

    if (
        criteria.get("semantic_screening_confirmed")
        is not True
    ):
        errors.append(
            "semantic_screening_confirmed is not true"
        )

    source_map = (
        digest.source_map
        if isinstance(digest.source_map, dict)
        else {}
    )

    fact_pack = source_map.get(
        "grounded_fact_pack"
    )

    if not isinstance(fact_pack, dict):
        errors.append("grounded_fact_pack is missing")
        fact_pack = {}

    if fact_pack.get("source_sufficient") is not True:
        errors.append("source_sufficient is not true")

    facts = fact_pack.get("facts")

    if not isinstance(facts, list) or not facts:
        errors.append("grounded facts are missing")
        facts = []

    fact_ids = set()

    for index, fact in enumerate(facts, start=1):
        if not isinstance(fact, dict):
            errors.append(
                f"fact #{index} is not an object"
            )
            continue

        fact_id = text(fact.get("fact_id"))

        if not fact_id:
            errors.append(
                f"fact #{index} has no fact_id"
            )
        else:
            fact_ids.add(fact_id)

        if not text(fact.get("statement")):
            errors.append(
                f"{fact_id or index}: empty statement"
            )

        if not text(fact.get("evidence_quote")):
            errors.append(
                f"{fact_id or index}: empty evidence_quote"
            )

        source_ids = fact.get("source_ids")

        if (
            not isinstance(source_ids, list)
            or not source_ids
        ):
            errors.append(
                f"{fact_id or index}: no source_ids"
            )

    blocks = source_map.get("blocks")

    if not isinstance(blocks, list) or not blocks:
        errors.append("validated blocks are missing")
        blocks = []

    for index, block in enumerate(blocks, start=1):
        if not isinstance(block, dict):
            errors.append(
                f"block #{index} is not an object"
            )
            continue

        block_fact_ids = block.get("fact_ids")

        if (
            not isinstance(block_fact_ids, list)
            or not block_fact_ids
        ):
            errors.append(
                f"block #{index} has no fact_ids"
            )
            continue

        unknown = {
            text(value)
            for value in block_fact_ids
            if text(value)
        } - fact_ids

        if unknown:
            errors.append(
                f"block #{index} has unknown fact_ids: "
                + ", ".join(sorted(unknown))
            )

    used_fact_ids = {
        text(value)
        for value in (
            source_map.get("used_fact_ids") or []
        )
        if text(value)
    }

    if used_fact_ids != fact_ids:
        missing = fact_ids - used_fact_ids
        unknown = used_fact_ids - fact_ids

        if missing:
            errors.append(
                "unused grounded facts: "
                + ", ".join(sorted(missing))
            )

        if unknown:
            errors.append(
                "unknown used_fact_ids: "
                + ", ".join(sorted(unknown))
            )

    used_event_ids = {
        text(value)
        for value in (
            source_map.get("used_event_ids") or []
        )
        if text(value)
    }

    digest_item_count = digest.digest_items.count()

    if digest_item_count < 1:
        errors.append("digest has no selected events")

    if len(used_event_ids) != digest_item_count:
        errors.append(
            "used_event_ids count does not match "
            f"digest items: {len(used_event_ids)} "
            f"!= {digest_item_count}"
        )

    return errors, {
        "facts": len(facts),
        "blocks": len(blocks),
        "events": digest_item_count,
        "body_chars": len(text(digest.body)),
    }


class Command(BaseCommand):
    help = (
        "Publishes a strictly validated RegionalDigest "
        "currently in review status."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--digest-id",
            type=int,
            default=None,
        )
        parser.add_argument(
            "--latest-review",
            action="store_true",
        )
        parser.add_argument(
            "--region-label",
            default="Северная Осетия",
        )
        parser.add_argument(
            "--topic",
            default="medicine",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
        )

    def handle(self, *args, **options):
        digest_id = options["digest_id"]
        latest_review = options["latest_review"]
        region_label = text(options["region_label"])
        topic = text(options["topic"])
        dry_run = options["dry_run"]

        if not digest_id and not latest_review:
            raise CommandError(
                "Use --digest-id or --latest-review."
            )

        with transaction.atomic():
            queryset = (
                RegionalDigest.objects
                .select_for_update()
            )

            if digest_id:
                digest = queryset.filter(
                    id=digest_id
                ).first()

                if digest is None:
                    raise CommandError(
                        f"RegionalDigest #{digest_id} "
                        "was not found."
                    )
            else:
                digest = (
                    queryset
                    .filter(
                        status=RegionalDigestStatus.REVIEW,
                        region_label=region_label,
                        topic=topic,
                    )
                    .order_by("-updated_at", "-id")
                    .first()
                )

                if digest is None:
                    self.stdout.write(
                        "No pending review digest found."
                    )
                    return

            if (
                digest.status
                == RegionalDigestStatus.PUBLISHED
                and digest.published_at is not None
            ):
                self.stdout.write(
                    self.style.WARNING(
                        f"RegionalDigest #{digest.id} "
                        "is already published."
                    )
                )
                return

            errors, stats = validate_digest(digest)

            self.stdout.write(
                f"RegionalDigest #{digest.id}"
            )
            self.stdout.write(
                f"Title: {digest.title}"
            )
            self.stdout.write(
                f"Facts: {stats['facts']}"
            )
            self.stdout.write(
                f"Blocks: {stats['blocks']}"
            )
            self.stdout.write(
                f"Events: {stats['events']}"
            )
            self.stdout.write(
                f"Body chars: {stats['body_chars']}"
            )

            if errors:
                for error in errors:
                    self.stderr.write(
                        self.style.ERROR(
                            f"VALIDATION ERROR: {error}"
                        )
                    )

                raise CommandError(
                    f"RegionalDigest #{digest.id} "
                    "failed publication validation."
                )

            if dry_run:
                self.stdout.write(
                    self.style.SUCCESS(
                        "Dry-run validation passed. "
                        "Database was not changed."
                    )
                )
                return

            digest.status = (
                RegionalDigestStatus.PUBLISHED
            )
            digest.published_at = timezone.now()

            digest.save(
                update_fields=[
                    "status",
                    "published_at",
                    "updated_at",
                ]
            )

            self.stdout.write(
                self.style.SUCCESS(
                    f"Published RegionalDigest "
                    f"#{digest.id} at "
                    f"{digest.published_at.isoformat()}"
                )
            )
