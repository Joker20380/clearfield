# clearfield/intel/management/commands/cluster_events.py
from __future__ import annotations

import hashlib
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import timedelta
from typing import Dict, Iterable, List, Optional, Tuple

from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from intel.models import Article, Event, EventItem, RawItem


# -----------------------------
# Text cleanup / tokenization
# -----------------------------
NOISE_PHRASES = [
    "One of your browser extensions seems to be blocking the video player",
    "To watch this content, you may need to disable it on this site",
    "Follow our liveblog",
    "for all the latest developments.",
    "for all the latest updates.",
    "from loading.",
    "from loading. .",
    "from loading. . from loading.",
]

NOISE_RE = [
    r"\bLive:\s*",
    r"\bFollow (our )?liveblog.*$",
    r"\bfrom loading\.(\s*\.)*",
]

WORD_RE = re.compile(r"[a-zA-Z0-9]+", re.UNICODE)


def sanitize(text: str) -> str:
    t = (text or "").strip()
    for p in NOISE_PHRASES:
        t = t.replace(p, " ")
    for rx in NOISE_RE:
        t = re.sub(rx, " ", t, flags=re.IGNORECASE)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def tokenize(text: str) -> List[str]:
    # Lower + keep only word-ish tokens
    t = sanitize(text).lower()
    return WORD_RE.findall(t)


# -----------------------------
# SimHash (64-bit)
# -----------------------------
def _hash64(token: str) -> int:
    # Stable 64-bit from md5 (fast + stable)
    h = hashlib.md5(token.encode("utf-8")).digest()
    return int.from_bytes(h[:8], byteorder="big", signed=False)


def simhash64(tokens: Iterable[str]) -> int:
    # Classic SimHash: signed bit weights
    v = [0] * 64
    for tok in tokens:
        x = _hash64(tok)
        for i in range(64):
            bit = (x >> i) & 1
            v[i] += 1 if bit else -1
    out = 0
    for i in range(64):
        if v[i] >= 0:
            out |= (1 << i)
    return out


def hamming64(a: int, b: int) -> int:
    return (a ^ b).bit_count()


def sh64_key(h: int) -> str:
    # compact stable cluster key
    return f"sh64:{h:016x}"


# -----------------------------
# Helpers
# -----------------------------
def pick_title(raw: RawItem, art: Optional[Article]) -> str:
    # Prefer article title, fallback raw title
    t = (art.title if art else "") or raw.title or ""
    t = sanitize(t)
    return t[:300]


def pick_region_topic(raw: RawItem) -> Tuple[str, str]:
    # If you have source.region/topic in your Source model, try it.
    # Otherwise keep blanks.
    region = ""
    topic = ""
    src = getattr(raw, "source", None)
    if src is not None:
        region = getattr(src, "region", "") or ""
        topic = getattr(src, "topic", "") or ""
    return (region[:16], topic[:16])


def best_text(raw: RawItem, art: Optional[Article]) -> str:
    # Prefer extracted article text, fallback raw summary/title/url
    t = (art.text if art else "") or ""
    if t:
        return t
    return " ".join([raw.title or "", raw.summary or "", raw.url or ""]).strip()


@dataclass
class Candidate:
    raw_id: int
    simh: int
    title: str
    region: str
    topic: str


class Command(BaseCommand):
    help = "Cluster extracted articles into Events (SimHash MVP, sanitized)"

    def add_arguments(self, parser):
        parser.add_argument("--since-hours", type=int, default=24)
        parser.add_argument("--limit", type=int, default=500)
        parser.add_argument("--max-dist", type=int, default=3)
        # Compatibility alias (optional UX): allow --hours same as --since-hours
        parser.add_argument("--hours", type=int, default=None)

    def handle(self, *args, **opts):
        since_hours = opts["since_hours"]
        if opts.get("hours") is not None:
            since_hours = int(opts["hours"])

        limit = int(opts["limit"])
        max_dist = int(opts["max_dist"])

        since = timezone.now() - timedelta(hours=since_hours)

        # IMPORTANT: window by published_at (fallback to created_at if published_at is null)
        raw_qs = (
            RawItem.objects.filter(
                Q(published_at__gte=since)
                | Q(published_at__isnull=True, created_at__gte=since)
            )
            .order_by("-published_at", "-created_at")
        )

        if limit:
            raw_qs = raw_qs[:limit]

        raw_items: List[RawItem] = list(raw_qs)
        if not raw_items:
            self.stdout.write("Events upserted: 0, items linked: 0")
            return

        raw_ids = [r.id for r in raw_items]

        # Pull Article for these items (1:1 by item)
        arts = Article.objects.filter(item_id__in=raw_ids)
        art_by_item: Dict[int, Article] = {a.item_id: a for a in arts}

        # Build candidates
        cands: List[Candidate] = []
        for r in raw_items:
            a = art_by_item.get(r.id)
            txt = best_text(r, a)
            toks = tokenize(txt)
            if len(toks) < 30:
                # Too little signal; skip
                continue
            h = simhash64(toks)
            region, topic = pick_region_topic(r)
            cands.append(
                Candidate(
                    raw_id=r.id,
                    simh=h,
                    title=pick_title(r, a),
                    region=region,
                    topic=topic,
                )
            )

        if not cands:
            self.stdout.write("Events upserted: 0, items linked: 0")
            return

        # Naive clustering: bucket by prefix to reduce comparisons
        # (prefix = top 16 bits)
        buckets: Dict[int, List[Candidate]] = defaultdict(list)
        for c in cands:
            prefix = (c.simh >> 48) & 0xFFFF
            buckets[prefix].append(c)

        # Fetch existing EventItem links to avoid relinking / integrity errors
        already_linked = set(
            EventItem.objects.filter(item_id__in=[c.raw_id for c in cands])
            .values_list("item_id", flat=True)
        )

        events_upserted = 0
        items_linked = 0

        # Load existing events for potential matches inside same prefix
        # We match by hamming distance on simhash embedded in cluster_key "sh64:..."
        # Note: This uses deterministic cluster keys (exact simhash). We still allow "near" match.
        existing_events = list(Event.objects.all().only("id", "cluster_key", "title", "region", "topic", "evidence_level"))
        existing_by_prefix: Dict[int, List[Tuple[Event, int]]] = defaultdict(list)
        for ev in existing_events:
            ck = ev.cluster_key or ""
            if ck.startswith("sh64:"):
                try:
                    h = int(ck.split(":", 1)[1], 16)
                except Exception:
                    continue
                prefix = (h >> 48) & 0xFFFF
                existing_by_prefix[prefix].append((ev, h))

        @transaction.atomic
        def upsert_one(c: Candidate):
            nonlocal events_upserted, items_linked

            if c.raw_id in already_linked:
                return

            prefix = (c.simh >> 48) & 0xFFFF

            # Find nearest existing event in same prefix by hamming distance
            best_ev = None
            best_d = 10**9
            for ev, h in existing_by_prefix.get(prefix, []):
                d = hamming64(c.simh, h)
                if d < best_d:
                    best_d = d
                    best_ev = ev

            if best_ev is not None and best_d <= max_dist:
                ev = best_ev
                created = False
            else:
                # Create new event with deterministic key = exact simhash
                key = sh64_key(c.simh)
                ev, created = Event.objects.get_or_create(
                    cluster_key=key,
                    defaults=dict(
                        title=c.title,
                        summary="",
                        region=c.region,
                        topic=c.topic,
                        evidence_level=1,
                    ),
                )
                if created:
                    events_upserted += 1
                    existing_by_prefix[prefix].append((ev, c.simh))

            # Lightweight enrichment (don’t thrash fields)
            changed = False
            if c.region and not ev.region:
                ev.region = c.region
                changed = True
            if c.topic and not ev.topic:
                ev.topic = c.topic
                changed = True
            if c.title and (not ev.title or len(ev.title) < 20) and len(c.title) > len(ev.title or ""):
                ev.title = c.title
                changed = True
            if changed:
                ev.save(update_fields=["title", "region", "topic", "updated_at"])

            # Link item to event (1:1 on item)
            EventItem.objects.create(event=ev, item_id=c.raw_id)
            items_linked += 1

        for c in cands:
            upsert_one(c)

        self.stdout.write(f"Events upserted: {events_upserted}, items linked: {items_linked}")
