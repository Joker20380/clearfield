import re
from datetime import timedelta

from django.core.management.base import BaseCommand
from django.db.models import Q
from django.utils import timezone

from intel.models import Event, EventItem, RawItem, Article


# =========================
# Noise cleanup
# =========================

NOISE_PHRASES = [
    "One of your browser extensions seems to be blocking the video player",
    "To watch this content, you may need to disable it on this site",
    "Follow our liveblog",
    "for all the latest developments.",
    "for all the latest updates.",
]

NOISE_RE = [
    r"\bLive:\s*",                      # "Live:"
    r"\bFollow (our )?liveblog.*$",      # tail like "Follow our liveblog ..."
    r"\bfrom loading(?:\s*\.)*\b",       # "from loading." / "from loading. ."
    r"\bblocking the video player from loading\b",
    r"\bOne of your browser extensions seems to be blocking the video player\b",
    r"\bTo watch this content, you may need to disable it on this site\b",
]


WORD_RE = re.compile(r"[A-Za-z0-9]+", re.UNICODE)


def sanitize(text: str) -> str:
    t = (text or "").strip()

    for p in NOISE_PHRASES:
        t = t.replace(p, " ")

    for rx in NOISE_RE:
        t = re.sub(rx, " ", t, flags=re.IGNORECASE | re.MULTILINE)

    t = re.sub(r"\s+", " ", t).strip()
    return t


def token_count(text: str) -> int:
    return len(WORD_RE.findall((text or "").lower()))


def is_placeholder(clean_text: str, min_len: int = 140, min_tokens: int = 30) -> bool:
    """Heuristic: detect empty/placeholder/blocked extracts."""
    if not clean_text:
        return True
    if re.search(r"\bfrom loading\b", clean_text, flags=re.IGNORECASE):
        return True
    if re.search(r"\bblocking the video player\b", clean_text, flags=re.IGNORECASE):
        return True
    if len(clean_text) < min_len:
        return True
    if token_count(clean_text) < min_tokens:
        return True
    return False


def pick_summary(text: str, title: str = "") -> str:
    t = sanitize(text)

    tt = (title or "").strip()
    if tt and t.lower().startswith(tt.lower()):
        t = t[len(tt):].lstrip(" -:—–\n\t")

    parts = re.split(r"(?<=[.!?])\s+", t)
    return " ".join(parts[:3])[:1200].strip()


class Command(BaseCommand):
    help = "Rebuild Event.summary for recent events using sanitized Article.text (with fallbacks)."

    def add_arguments(self, parser):
        parser.add_argument("--hours", type=int, default=168, help="Window by Event.updated_at (hours)")
        parser.add_argument(
            "--touch-updated-at",
            action="store_true",
            help="If set, updates Event.updated_at as well. Default: keep updated_at intact.",
        )
        parser.add_argument("--min-clean-len", type=int, default=140)
        parser.add_argument("--min-tokens", type=int, default=30)

    def handle(self, *args, **opts):
        hours = int(opts["hours"])
        touch_updated_at = bool(opts["touch_updated_at"])
        min_clean_len = int(opts["min_clean_len"])
        min_tokens = int(opts["min_tokens"])
        verbosity = int(opts.get("verbosity") or 1)

        since = timezone.now() - timedelta(hours=hours)
        qs = Event.objects.filter(updated_at__gte=since).order_by("-updated_at")

        events = list(qs)
        if verbosity >= 2:
            self.stdout.write(f"Found events in window: {len(events)} (since={since.isoformat()})")

        if not events:
            self.stdout.write(self.style.SUCCESS("Updated summaries: 0"))
            return

        event_ids = [e.id for e in events]

        # Pull event-item links
        ev_items = list(
            EventItem.objects.filter(event_id__in=event_ids).only("event_id", "item_id")
        )
        if not ev_items:
            self.stdout.write(self.style.SUCCESS("Updated summaries: 0"))
            return

        item_ids = list({ei.item_id for ei in ev_items})

        # Bulk load RawItem & Article
        raw_by_id = {
            r.id: r for r in RawItem.objects.filter(id__in=item_ids).only("id", "title", "summary", "url")
        }
        art_by_item = {
            a.item_id: a for a in Article.objects.filter(item_id__in=item_ids).only("item_id", "title", "text")
        }

        # Group EventItem by event
        items_by_event = {}
        for ei in ev_items:
            items_by_event.setdefault(ei.event_id, []).append(ei.item_id)

        updated = 0
        skipped_no_good_text = 0
        skipped_unchanged = 0

        for ev in events:
            ids = items_by_event.get(ev.id) or []
            if not ids:
                skipped_no_good_text += 1
                continue

            best_text = ""
            best_clean = ""
            best_src = None

            for item_id in ids:
                raw = raw_by_id.get(item_id)
                art = art_by_item.get(item_id)

                # Candidate chain (ordered):
                candidates = []
                if art and (art.text or "").strip():
                    candidates.append(("article", art.text))
                if raw and (raw.summary or "").strip():
                    candidates.append(("raw_summary", raw.summary))
                if raw and (raw.title or "").strip():
                    candidates.append(("raw_title", raw.title))

                for src, txt in candidates:
                    clean = sanitize(txt)

                    if is_placeholder(clean, min_len=min_clean_len, min_tokens=min_tokens):
                        continue

                    # choose most informative clean text
                    if len(clean) > len(best_clean):
                        best_clean = clean
                        best_text = txt
                        best_src = (src, item_id)

            if not best_text:
                skipped_no_good_text += 1
                if verbosity >= 2:
                    self.stdout.write(f"Event {ev.id}: skip (no good text after sanitation)")
                continue

            new_summary = pick_summary(best_text, ev.title)

            if not new_summary:
                skipped_no_good_text += 1
                if verbosity >= 2:
                    self.stdout.write(f"Event {ev.id}: skip (empty summary after pick_summary)")
                continue

            if new_summary == (ev.summary or ""):
                skipped_unchanged += 1
                if verbosity >= 3 and best_src:
                    self.stdout.write(f"Event {ev.id}: unchanged (best={best_src[0]} item={best_src[1]})")
                continue

            # IMPORTANT: by default do NOT touch updated_at (keeps windows meaningful)
            if touch_updated_at:
                ev.summary = new_summary
                ev.save(update_fields=["summary", "updated_at"])
            else:
                Event.objects.filter(pk=ev.pk).update(summary=new_summary)

            updated += 1
            if verbosity >= 2 and best_src:
                self.stdout.write(f"Event {ev.id}: updated (best={best_src[0]} item={best_src[1]})")

        if verbosity >= 2:
            self.stdout.write(f"Skipped (no good text): {skipped_no_good_text}")
            self.stdout.write(f"Skipped (unchanged): {skipped_unchanged}")

        self.stdout.write(self.style.SUCCESS(f"Updated summaries: {updated}"))
