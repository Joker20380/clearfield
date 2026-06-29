# clearfield/intel/management/commands/cluster_events.py

import hashlib
import time
from dataclasses import dataclass
from typing import Dict, List

from django.core.management.base import BaseCommand
from django.db import IntegrityError, OperationalError, close_old_connections, transaction
from django.db.transaction import TransactionManagementError
from django.utils import timezone

from intel.models import Event, EventItem, RawItem, SourceClass


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()


def build_cluster_key(raw: RawItem) -> str:
    """
    Stable cluster_key.
    На текущем этапе 1 RawItem ≈ 1 Event.
    Более глубокую склейку можно делать командой recluster_events.
    """
    if getattr(raw, "item_hash", None):
        return f"ih:{raw.item_hash}"

    url = (raw.url or "").strip()

    if url:
        return f"url:{_sha1(url)}"

    guid = (raw.guid or "").strip()

    if guid:
        return f"gid:{_sha1(guid)}"

    base = f"{(raw.title or '').strip()}|{raw.published_at or ''}|{raw.source_id or ''}"

    return f"fb:{_sha1(base)}"


def detect_evidence_level(raw: RawItem) -> int:
    """
    Уровень доказательности на основе класса источника.

    OFFICIAL/STATS — 2.
    Остальные — 1.
    Уровень 3 позже можно давать событиям вручную или отдельной командой,
    когда событие подтверждено несколькими классами источников.
    """
    source = getattr(raw, "source", None)

    if not source:
        return 1

    if source.source_class in [SourceClass.OFFICIAL, SourceClass.STATS]:
        return 2

    return 1


def db_retry(fn, label: str, stdout=None, retries: int = 3, base_sleep: float = 0.6):
    last_exc = None

    for i in range(retries):
        try:
            close_old_connections()
            return fn()
        except OperationalError as e:
            last_exc = e

            if stdout:
                stdout.write(f"[cluster_events] {label} OperationalError: {e} (retry {i + 1}/{retries})")

            time.sleep(base_sleep * (i + 1))

    if last_exc:
        raise last_exc

    return fn()


def atomic_retry(fn, label: str, stdout=None, retries: int = 3, base_sleep: float = 0.8):
    last_exc = None

    for i in range(retries):
        try:
            close_old_connections()

            with transaction.atomic():
                return fn()

        except OperationalError as e:
            last_exc = e
            close_old_connections()

            if stdout:
                stdout.write(f"[cluster_events] {label} OperationalError: {e} (retry {i + 1}/{retries})")

            time.sleep(base_sleep * (i + 1))

        except TransactionManagementError as e:
            last_exc = e
            close_old_connections()

            if stdout:
                stdout.write(f"[cluster_events] {label} TransactionManagementError: {e} (retry {i + 1}/{retries})")

            time.sleep(base_sleep * (i + 1))

    close_old_connections()

    with transaction.atomic():
        return fn()


def isolate_inserts(model_cls, objs: List, label: str, stdout=None, stderr=None):
    if stdout:
        stdout.write(f"[cluster_events] ISOLATE {label}: inserting one-by-one ({len(objs)} rows)")

    for idx, obj in enumerate(objs):
        try:
            close_old_connections()
            obj.save(force_insert=True)
        except Exception as e:
            if stderr:
                stderr.write(f"[cluster_events] ISOLATE {label} FAIL at idx={idx}: {type(e).__name__}: {e}")
                stderr.write(f"[cluster_events] offending {label} __dict__: {getattr(obj, '__dict__', {})}")

            raise

    if stdout:
        stdout.write(f"[cluster_events] ISOLATE {label}: OK")


@dataclass
class ClusterRow:
    raw_id: int
    cluster_key: str
    title: str
    summary: str
    region: str
    topic: str
    evidence_level: int


class Command(BaseCommand):
    help = "Cluster unlinked RawItem into Events and create EventItems."

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true", help="No writes, only counts.")
        parser.add_argument("--batch-size", type=int, default=500)

        parser.add_argument("--debug-insert", action="store_true", help="Try inserting a sample EventItem.")
        parser.add_argument("--debug-event-insert", action="store_true", help="Try inserting a sample Event.")
        parser.add_argument("--isolate-event", action="store_true", help="Insert Events one-by-one to debug invalid rows.")

        parser.add_argument("--limit", type=int, default=None, help="Limit number of unlinked RawItems processed.")

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        batch_size = options["batch_size"]
        limit = options.get("limit")

        unlinked_qs = (
            RawItem.objects
            .filter(event_item__isnull=True)
            .select_related("source")
            .order_by("id")
        )

        if limit:
            unlinked_ids = list(unlinked_qs.values_list("id", flat=True)[:limit])
            unlinked_qs = (
                RawItem.objects
                .filter(id__in=unlinked_ids)
                .select_related("source")
                .order_by("id")
            )

        scope_count = db_retry(unlinked_qs.count, label="count unlinked", stdout=self.stdout)
        self.stdout.write(f"[cluster_events] unlinked scope = {scope_count}")

        if scope_count == 0:
            self.stdout.write("[cluster_events] nothing to do")
            return

        def load_raws():
            return list(
                unlinked_qs.only(
                    "id",
                    "title",
                    "summary",
                    "published_at",
                    "guid",
                    "url",
                    "item_hash",
                    "source_id",
                    "source__region",
                    "source__topic",
                    "source__source_class",
                    "source__name",
                )
            )

        raws: List[RawItem] = db_retry(load_raws, label="load raws", stdout=self.stdout)

        rows: List[ClusterRow] = []

        for raw in raws:
            source = getattr(raw, "source", None)

            rows.append(
                ClusterRow(
                    raw_id=raw.id,
                    cluster_key=build_cluster_key(raw),
                    title=(raw.title or "").strip(),
                    summary=(raw.summary or "").strip(),
                    region=(source.region if source else "") or "",
                    topic=(source.topic if source else "") or "",
                    evidence_level=detect_evidence_level(raw),
                )
            )

        cluster_keys = sorted({x.cluster_key for x in rows})
        self.stdout.write(f"[cluster_events] clusters = {len(cluster_keys)}")

        if options["debug_insert"]:
            self.stdout.write("[cluster_events] DEBUG INSERT MODE")
            sample = EventItem(event_id=None, item_id=None, created_at=timezone.now())

            try:
                sample.save(force_insert=True)
            except Exception as e:
                self.stderr.write(f"[cluster_events] ROOT ERROR: {type(e).__name__}: {e}")
                self.stderr.write(f"[cluster_events] sample __dict__: {sample.__dict__}")
                raise

            return

        if options["debug_event_insert"]:
            self.stdout.write("[cluster_events] DEBUG EVENT INSERT")

            sample_ev = Event(
                title="debug event insert",
                summary="",
                region="",
                topic="",
                evidence_level=1,
                cluster_key=f"debug:{_sha1(str(time.time()))}",
            )
            sample_ev.save(force_insert=True)

            self.stdout.write("[cluster_events] sample Event insert OK")
            return

        if dry_run:
            self.stdout.write("[cluster_events] DRY RUN — no writes")
            self.stdout.write(f"[cluster_events] would upsert Events: ~{len(cluster_keys)}")
            self.stdout.write(f"[cluster_events] would create EventItems: {len(rows)}")
            return

        raw_to_key: Dict[int, str] = {x.raw_id: x.cluster_key for x in rows}

        uniq_events: Dict[str, Event] = {}

        for row in rows:
            if row.cluster_key in uniq_events:
                continue

            uniq_events[row.cluster_key] = Event(
                title=row.title or "",
                summary=row.summary or "",
                region=row.region or "",
                topic=row.topic or "",
                evidence_level=row.evidence_level,
                cluster_key=row.cluster_key,
            )

        events_to_create = list(uniq_events.values())

        if options["isolate_event"]:
            isolate_inserts(Event, events_to_create, label="Event", stdout=self.stdout, stderr=self.stderr)
            return

        def unit_of_work():
            existing = {
                event.cluster_key: event
                for event in Event.objects.filter(cluster_key__in=cluster_keys)
            }

            to_create = [
                event for event in events_to_create
                if event.cluster_key not in existing
            ]

            if to_create:
                Event.objects.bulk_create(
                    to_create,
                    batch_size=batch_size,
                    ignore_conflicts=True,
                )

            events_by_key = {
                event.cluster_key: event
                for event in Event.objects.filter(cluster_key__in=cluster_keys)
            }

            existing_item_ids = set(
                EventItem.objects
                .filter(item_id__in=list(raw_to_key.keys()))
                .values_list("item_id", flat=True)
            )

            items_to_create = []
            now = timezone.now()

            for raw_id, cluster_key in raw_to_key.items():
                if raw_id in existing_item_ids:
                    continue

                event = events_by_key.get(cluster_key)

                if not event:
                    continue

                items_to_create.append(
                    EventItem(
                        event_id=event.id,
                        item_id=raw_id,
                        created_at=now,
                    )
                )

            if items_to_create:
                EventItem.objects.bulk_create(
                    items_to_create,
                    batch_size=batch_size,
                    ignore_conflicts=True,
                )

            return True

        try:
            atomic_retry(unit_of_work, label="atomic unit", stdout=self.stdout)

        except IntegrityError as e:
            self.stderr.write(f"[cluster_events] FAILED IntegrityError: {e}")
            raise

        except Exception as e:
            self.stderr.write(f"[cluster_events] FAILED: {type(e).__name__}: {e}")
            raise

        def remaining_unlinked():
            return RawItem.objects.filter(event_item__isnull=True).count()

        remaining = db_retry(remaining_unlinked, label="remaining unlinked", stdout=self.stdout)

        self.stdout.write(
            f"[cluster_events] DONE | eventitems_attempted={len(rows)} | unlinked(after)={remaining}"
        )
