import re
from datetime import timedelta

from django.core.management.base import BaseCommand
from django.utils import timezone

from intel.models import Event, EventItem


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
]


def sanitize(text: str) -> str:
    t = (text or "").strip()

    for p in NOISE_PHRASES:
        t = t.replace(p, " ")

    for rx in NOISE_RE:
        t = re.sub(rx, " ", t, flags=re.IGNORECASE)

    t = re.sub(r"\s+", " ", t).strip()
    return t


def pick_summary(text: str, title: str = "") -> str:
    t = sanitize(text)

    tt = (title or "").strip()
    if tt and t.lower().startswith(tt.lower()):
        t = t[len(tt):].lstrip(" -:—–\n\t")

    parts = re.split(r"(?<=[.!?])\s+", t)
    return " ".join(parts[:3])[:1200]


class Command(BaseCommand):
    help = "Rebuild Event.summary for recent events using sanitized Article.text"

    def add_arguments(self, parser):
        parser.add_argument("--hours", type=int, default=168)  # 7 days

    def handle(self, *args, **opts):
        since = timezone.now() - timedelta(hours=opts["hours"])
        qs = Event.objects.filter(updated_at__gte=since).order_by("-updated_at")

        updated = 0

        for ev in qs:
            # берём самый длинный (самый информативный) текст из привязанных items
            items = (
                EventItem.objects
                .filter(event=ev)
                .select_related("item", "item__article")
            )

            best_text = ""
            for it in items:
                art = getattr(it.item, "article", None)
                if art and art.text and len(art.text) > len(best_text):
                    best_text = art.text

            if not best_text:
                continue

            new_summary = pick_summary(best_text, ev.title)

            if new_summary and new_summary != (ev.summary or ""):
                ev.summary = new_summary
                ev.save(update_fields=["summary", "updated_at"])
                updated += 1

        self.stdout.write(self.style.SUCCESS(f"Updated summaries: {updated}"))
