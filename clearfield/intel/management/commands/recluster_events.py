# clearfield/intel/management/commands/recluster_events.py

import hashlib
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import timedelta
from typing import Dict, List

from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Count

from intel.models import Event, EventItem


TRACKING_QUERY_KEYS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "fbclid", "gclid", "yclid", "igshid", "mc_cid", "mc_eid",
    "ref", "cmpid", "cmp", "spm", "mkt_tok"
}


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()


def normalize_whitespace(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def canonicalize_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    try:
        from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
        parts = urlsplit(url)
        scheme = (parts.scheme or "https").lower()
        netloc = (parts.netloc or "").lower()
        path = parts.path or ""
        fragment = ""
        pairs = parse_qsl(parts.query, keep_blank_values=True)
        cleaned = []
        for k, v in pairs:
            if (k or "").lower() in TRACKING_QUERY_KEYS:
                continue
            cleaned.append((k, v))
        query = urlencode(cleaned, doseq=True)
        out = urlunsplit((scheme, netloc, path, query, fragment))
        return out.rstrip("/")
    except Exception:
        return url.rstrip("/")


_STOP_WORDS = {
    "the", "a", "an", "and", "or", "of", "to", "in", "on", "for", "with", "at",
    "from", "by", "as", "is", "are", "was", "were", "be", "been", "it", "this",
    "that", "these", "those", "about", "into", "over", "after", "before", "vs"
}


def normalize_title(title: str) -> str:
    t = (title or "").lower()
    t = re.sub(r"[^\w\s]", " ", t, flags=re.UNICODE)
    t = normalize_whitespace(t)
    if not t:
        return ""
    words = [w for w in t.split(" ") if w and w not in _STOP_WORDS]
    return " ".join(words)


def make_cluster_key_from_item(item) -> str:
    cu = canonicalize_url(getattr(item, "url", "") or "")
    if cu:
        return "url:" + sha1(cu)

    nt = normalize_title(getattr(item, "title", "") or "")
    pub = getattr(item, "published_at", None)
    bucket = pub.date().isoformat() if pub else "nodate"
    if nt:
        return f"t:{bucket}:" + sha1(nt)

    ih = getattr(item, "item_hash", "") or ""
    if ih:
        return "ih:" + ih
    return "misc:" + sha1(str(getattr(item, "pk", "")))


class Command(BaseCommand):
    help = "Recompute Event cluster_key (v2) and move EventItems accordingly"

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--cleanup-empty-events", action="store_true")

    def handle(self, *args, **opts):
        dry = opts["dry_run"]
        cleanup = opts["cleanup_empty_events"]

        qs = EventItem.objects.select_related("event", "item").all()
        total = qs.count()
        self.stdout.write(f"[recluster_events] scope(EventItem) = {total}")

        # target keys
        # We'll count moves first
        moves = []
        key_to_items = defaultdict(list)

        for ei in qs.iterator(chunk_size=1000):
            new_key = make_cluster_key_from_item(ei.item)
            if ei.event.cluster_key != new_key:
                moves.append((ei.id, ei.event_id, new_key))
            key_to_items[new_key].append(ei.id)

        self.stdout.write(f"[recluster_events] target clusters = {len(key_to_items)}")
        self.stdout.write(f"[recluster_events] moves_needed = {len(moves)}")

        if dry:
            self.stdout.write("[recluster_events] DRY RUN — no writes")
            return

        moved = 0
        scanned = 0

        with transaction.atomic():
            # preload existing events for keys
            all_keys = list(key_to_items.keys())
            existing = {e.cluster_key: e for e in Event.objects.filter(cluster_key__in=all_keys)}

            # create missing
            to_create = []
            for k in all_keys:
                if k in existing:
                    continue
                # minimal event; title/summary will be refined later by your summary job
                to_create.append(Event(cluster_key=k, title="", summary="", region="", topic="", evidence_level=1))
            if to_create:
                Event.objects.bulk_create(to_create, batch_size=500, ignore_conflicts=True)
                existing = {e.cluster_key: e for e in Event.objects.filter(cluster_key__in=all_keys)}

            # apply moves
            for ei in qs.iterator(chunk_size=1000):
                scanned += 1
                new_key = make_cluster_key_from_item(ei.item)
                target = existing.get(new_key)
                if not target:
                    continue
                if ei.event_id != target.id:
                    EventItem.objects.filter(id=ei.id).update(event_id=target.id)
                    moved += 1

            self.stdout.write(f"[recluster_events] moved={moved} scanned={scanned}")

            if cleanup:
                # delete empty events
                empty = (Event.objects.annotate(n=Count("items"))
                         .filter(n=0))
                deleted = empty.count()
                empty.delete()
                self.stdout.write(f"[recluster_events] deleted empty Events: {deleted}")

        # distribution after
        dist = list(
            Event.objects.annotate(n=Count("items", distinct=True))
            .values("n")
            .annotate(c=Count("id"))
            .order_by("n")[:50]
        )
        self.stdout.write(f"[recluster_events] dist(n->count): {dist}")
