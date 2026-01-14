# clearfield/intel/management/commands/cluster_events.py

import hashlib
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

from django.core.management.base import BaseCommand
from django.db import IntegrityError, OperationalError, transaction
from django.db.transaction import TransactionManagementError
from django.db import close_old_connections
from django.utils import timezone

from intel.models import Event, EventItem, RawItem


# -----------------------------
# Helpers
# -----------------------------

def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()


def build_cluster_key(raw: RawItem) -> str:
    """
    Stable cluster_key. Prefer item_hash if you already compute it in ingest.
    Fallback to url; final fallback to guid/title/published_at.
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


def db_retry(fn, label: str, stdout=None, retries: int = 3, base_sleep: float = 0.6):
    """
    Retry for standalone DB operations (NO atomic inside fn).
    """
    last_exc = None
    for i in range(retries):
        try:
            close_old_connections()
            return fn()
        except OperationalError as e:
            last_exc = e
            if stdout:
                stdout.write(f"[cluster_events] {label} OperationalError: {e} (retry {i+1}/{retries})")
            time.sleep(base_sleep * (i + 1))
    if last_exc:
        raise last_exc
    return fn()


def atomic_retry(fn, label: str, stdout=None, retries: int = 3, base_sleep: float = 0.8):
    """
    Retry around a FULL atomic block. If connection drops inside, the transaction is broken.
    We must exit atomic, close connections, and retry whole block.
    """
    last_exc = None
    for i in range(retries):
        try:
            close_old_connections()
            with transaction.atomic():
                return fn()
        except (OperationalError,) as e:
            last_exc = e
            try:
                close_old_connections()
            except Exception:
                pass
            if stdout:
                stdout.write(f"[cluster_events] {label} OperationalError: {e} (retry {i+1}/{retries})")
            time.sleep(base_sleep * (i + 1))
        except TransactionManagementError as e:
            last_exc = e
            try:
                close_old_connections()
            except Exception:
                pass
            if stdout:
                stdout.write(f"[cluster_events] {label} TransactionManagementError: {e} (retry {i+1}/{retries})")
            time.sleep(base_sleep * (i + 1))

    # last attempt без проглатывания
    close_old_connections()
    with transaction.atomic():
        return fn()


def isolate_inserts(model_cls, objs: List, label: str, stdout=None, stderr=None):
    """
    Debug helper: insert one-by-one to find offending row.
    """
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


# -----------------------------
# Command
# -----------------------------

class Command(BaseCommand):
    help = "Cluster unlinked RawItem into Events and create EventItems."

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true", help="No writes, only counts.")
        parser.add_argument("--batch-size", type=int, default=500)

        # debug switches used in your console logs
        parser.add_argument("--debug-insert", action="store_true", help="Try inserting a sample EventItem (no atomic).")
        parser.add_argument("--debug-event-insert", action="store_true", help="Try inserting a sample Event (no atomic).")
        parser.add_argument("--isolate-event", action="store_true", help="Insert Events one-by-one to find duplicates/invalid rows.")

        # optional scope control
        parser.add_argument("--limit", type=int, default=None, help="Limit number of unlinked RawItems processed (for stability).")

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        batch_size = options["batch_size"]
        limit = options.get("limit")

        # scope: only unlinked RawItem (OneToOne reverse name: raw.event_item)
        unlinked_qs = RawItem.objects.filter(event_item__isnull=True).order_by("id")
        if limit:
            unlinked_ids = list(unlinked_qs.values_list("id", flat=True)[:limit])
            unlinked_qs = RawItem.objects.filter(id__in=unlinked_ids).order_by("id")

        scope_count = db_retry(unlinked_qs.count, label="count unlinked", stdout=self.stdout)
        self.stdout.write(f"[cluster_events] unlinked scope = {scope_count}")

        if scope_count == 0:
            self.stdout.write("[cluster_events] nothing to do")
            return

        # materialize raw items (avoid long server-side cursor)
        def load_raws():
            # only the fields we need; keep objects to access choices easily
            return list(unlinked_qs.select_related().only(
                "id", "title", "summary", "region", "topic", "published_at", "guid", "url", "item_hash"
            ))

        raws: List[RawItem] = db_retry(load_raws, label="load raws", stdout=self.stdout)

        # build clusters (here 1 raw => 1 cluster_key; if you want real merging later, do it in recluster_events)
        rows: List[ClusterRow] = []
        for r in raws:
            ck = build_cluster_key(r)
            rows.append(
                ClusterRow(
                    raw_id=r.id,
                    cluster_key=ck,
                    title=(r.title or "").strip(),
                    summary=(r.summary or "").strip(),
                    region=(getattr(r, "region", "") or "").strip(),
                    topic=(getattr(r, "topic", "") or "").strip(),
                    evidence_level=1,
                )
            )

        cluster_keys = sorted({x.cluster_key for x in rows})
        self.stdout.write(f"[cluster_events] clusters = {len(cluster_keys)}")

        # ---- debug modes ----
        if options["debug_insert"]:
            self.stdout.write("[cluster_events] DEBUG INSERT MODE (no atomic)")
            sample = EventItem(event_id=None, item_id=None, created_at=timezone.now())
            try:
                sample.save(force_insert=True)
            except Exception as e:
                self.stderr.write(f"[cluster_events] ROOT ERROR: {type(e).__name__}: {e}")
                self.stderr.write(f"[cluster_events] sample __dict__: {sample.__dict__}")
                raise
            return

        if options["debug_event_insert"]:
            self.stdout.write("[cluster_events] DEBUG EVENT INSERT (no atomic)")
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

        # ---- DRY RUN ----
        if dry_run:
            self.stdout.write("[cluster_events] DRY RUN — no writes")
            self.stdout.write(f"[cluster_events] would upsert Events: ~{len(cluster_keys)}")
            self.stdout.write(f"[cluster_events] would create EventItems: {len(rows)}")
            return

        # ---- Real work (atomic-with-retry) ----

        # Build minimal lookup map raw_id -> cluster_key once
        raw_to_key: Dict[int, str] = {x.raw_id: x.cluster_key for x in rows}
        # Prepare Event candidates
        events_to_create = [
            Event(
                title=x.title or "",         # allow empty, but prefer not
                summary=x.summary or "",
                region=x.region or "",
                topic=x.topic or "",
                evidence_level=x.evidence_level,
                cluster_key=x.cluster_key,
            )
            for x in rows
        ]

        # Deduplicate Event objects by cluster_key (bulk_create would still be ok with ignore_conflicts,
        # but we avoid sending duplicates in one batch)
        uniq_events: Dict[str, Event] = {}
        for ev in events_to_create:
            if ev.cluster_key not in uniq_events:
                uniq_events[ev.cluster_key] = ev
        events_to_create = list(uniq_events.values())

        if options["isolate_event"]:
            # useful if you're hunting unique constraint collisions / invalid rows
            isolate_inserts(Event, events_to_create, label="Event", stdout=self.stdout, stderr=self.stderr)
            return

        def unit_of_work():
            # Step 1: Upsert Events by cluster_key
            existing = {e.cluster_key: e for e in Event.objects.filter(cluster_key__in=cluster_keys)}

            to_create = [ev for ev in events_to_create if ev.cluster_key not in existing]
            if to_create:
                # ignore_conflicts to handle races / pre-existing keys
                Event.objects.bulk_create(to_create, batch_size=batch_size, ignore_conflicts=True)

            # Re-fetch to get IDs for all keys
            events_by_key = {e.cluster_key: e for e in Event.objects.filter(cluster_key__in=cluster_keys)}

            # Step 2: Create EventItems (link RawItem -> Event)
            # IMPORTANT: Do NOT bulk_update RawItem.event_item (it's reverse O2O and not concrete).
            # Creating EventItem rows is the correct linkage.
            existing_item_ids = set(
                EventItem.objects.filter(item_id__in=list(raw_to_key.keys()))
                .values_list("item_id", flat=True)
            )

            items_to_create = []
            now = timezone.now()
            for raw_id, ck in raw_to_key.items():
                if raw_id in existing_item_ids:
                    continue
                ev = events_by_key.get(ck)
                if not ev:
                    # should not happen; but keep resilient
                    continue
                items_to_create.append(
                    EventItem(event_id=ev.id, item_id=raw_id, created_at=now)
                )

            if items_to_create:
                EventItem.objects.bulk_create(items_to_create, batch_size=batch_size, ignore_conflicts=True)

            return True

        try:
            atomic_retry(unit_of_work, label="atomic unit", stdout=self.stdout)
        except IntegrityError as e:
            self.stderr.write(f"[cluster_events] FAILED IntegrityError: {e}")
            raise
        except Exception as e:
            self.stderr.write(f"[cluster_events] FAILED: {type(e).__name__}: {e}")
            raise

        # Post-check (outside atomic): how many left
        def remaining_unlinked():
            return RawItem.objects.filter(event_item__isnull=True).count()

        remaining = db_retry(remaining_unlinked, label="remaining unlinked", stdout=self.stdout)
        self.stdout.write(f"[cluster_events] DONE | eventitems_attempted={len(rows)} | unlinked(after)={remaining}")
