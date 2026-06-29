import hashlib

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from intel.models import Event, EventItem, RawItem, SourceClass


def sha1(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8", errors="ignore")).hexdigest()


def build_safe_cluster_key(raw: RawItem) -> str:
    """
    Event.cluster_key max_length=64.

    raw.item_hash обычно SHA256 = 64 символа.
    Нельзя делать "ih:" + item_hash, потому что получится 67 символов.
    Поэтому хэшируем item_hash через SHA1.
    """

    item_hash = (raw.item_hash or "").strip()
    if item_hash:
        return f"ih:{sha1(item_hash)}"

    url = (raw.url or "").strip()
    if url:
        return f"url:{sha1(url)}"

    guid = (raw.guid or "").strip()
    if guid:
        return f"gid:{sha1(guid)}"

    base = f"{raw.title or ''}|{raw.published_at or ''}|{raw.source_id or ''}"
    return f"fb:{sha1(base)}"


def detect_evidence_level(raw: RawItem) -> int:
    source = getattr(raw, "source", None)

    if not source:
        return 1

    if source.source_class in [SourceClass.OFFICIAL, SourceClass.STATS]:
        return 2

    return 1


class Command(BaseCommand):
    help = "Repair missing EventItem links for RawItem."

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=1000)
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument(
            "--delete-empty-events",
            action="store_true",
            help="Delete Events without EventItem before repair.",
        )

    def handle(self, *args, **options):
        limit = options["limit"]
        dry_run = options["dry_run"]
        delete_empty_events = options["delete_empty_events"]

        if delete_empty_events:
            empty_qs = Event.objects.filter(items__isnull=True)
            empty_count = empty_qs.count()

            self.stdout.write(f"[repair_eventitems] empty events = {empty_count}")

            if not dry_run:
                empty_qs.delete()
                self.stdout.write(self.style.WARNING(f"[repair_eventitems] deleted empty events = {empty_count}"))

        raw_qs = (
            RawItem.objects
            .filter(event_item__isnull=True)
            .select_related("source")
            .order_by("id")[:limit]
        )

        raws = list(raw_qs)

        self.stdout.write(f"[repair_eventitems] unlinked RawItem = {len(raws)}")

        if not raws:
            self.stdout.write(self.style.SUCCESS("[repair_eventitems] nothing to repair"))
            return

        created_events = 0
        reused_events = 0
        created_links = 0

        for raw in raws:
            source = raw.source

            cluster_key = build_safe_cluster_key(raw)

            title = (raw.title or "").strip()
            summary = (raw.summary or "").strip()

            region = (source.region if source else "") or ""
            topic = (source.topic if source else "") or ""
            evidence_level = detect_evidence_level(raw)

            self.stdout.write(
                f"[repair_eventitems] raw={raw.id} key={cluster_key} topic={topic} title={title[:80]}"
            )

            if dry_run:
                continue

            with transaction.atomic():
                event, was_created = Event.objects.get_or_create(
                    cluster_key=cluster_key,
                    defaults={
                        "title": title,
                        "summary": summary,
                        "region": region,
                        "topic": topic,
                        "evidence_level": evidence_level,
                        "created_at": timezone.now(),
                    },
                )

                if was_created:
                    created_events += 1
                else:
                    reused_events += 1

                    changed = False

                    if not event.title and title:
                        event.title = title
                        changed = True

                    if not event.summary and summary:
                        event.summary = summary
                        changed = True

                    if not event.region and region:
                        event.region = region
                        changed = True

                    if not event.topic and topic:
                        event.topic = topic
                        changed = True

                    if evidence_level > event.evidence_level:
                        event.evidence_level = evidence_level
                        changed = True

                    if changed:
                        event.save(update_fields=[
                            "title",
                            "summary",
                            "region",
                            "topic",
                            "evidence_level",
                            "updated_at",
                        ])

                _, link_created = EventItem.objects.get_or_create(
                    item=raw,
                    defaults={
                        "event": event,
                        "created_at": timezone.now(),
                    },
                )

                if link_created:
                    created_links += 1

        if dry_run:
            self.stdout.write(self.style.SUCCESS("[repair_eventitems] dry-run complete"))
        else:
            self.stdout.write(self.style.SUCCESS(f"[repair_eventitems] created events = {created_events}"))
            self.stdout.write(self.style.SUCCESS(f"[repair_eventitems] reused events = {reused_events}"))
            self.stdout.write(self.style.SUCCESS(f"[repair_eventitems] created EventItems = {created_links}"))

