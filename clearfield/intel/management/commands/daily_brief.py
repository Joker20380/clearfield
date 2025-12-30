from __future__ import annotations

import signal
import re
from datetime import timedelta

from django.core.management.base import BaseCommand
from django.utils import timezone

from intel.models import Event


# корректно завершаемся при пайпах в head|tail
signal.signal(signal.SIGPIPE, signal.SIG_DFL)


TITLE_CLEAN_RE = [
    (re.compile(r"^\s*Live:\s*", re.IGNORECASE), ""),  # "Live: ..."
    (re.compile(r"\s+", re.UNICODE), " "),             # collapse spaces
]

SUMMARY_CLEAN_RE = [
    (re.compile(r"\bfrom loading(?:\s*\.)*\b", re.IGNORECASE), ""),  # last-resort cleanup
    (re.compile(r"\s+", re.UNICODE), " "),
]


def clean_title(t: str) -> str:
    t = (t or "").strip()
    for rx, repl in TITLE_CLEAN_RE:
        t = rx.sub(repl, t)
    return t.strip()


def clean_summary(s: str) -> str:
    s = (s or "").strip()
    for rx, repl in SUMMARY_CLEAN_RE:
        s = rx.sub(repl, s)
    return s.strip()


def brief_header(hours: int) -> str:
    return f"# CLEARFIELD Brief — last {hours}h"


class Command(BaseCommand):
    help = "Print daily brief (Markdown) from Events"

    def add_arguments(self, parser):
        parser.add_argument("--hours", type=int, default=72)
        parser.add_argument("--min-evidence", type=int, default=1)

    def handle(self, *args, **opts):
        hours = int(opts["hours"])
        min_evidence = int(opts["min_evidence"])
        since = timezone.now() - timedelta(hours=hours)

        qs = (
            Event.objects
            .filter(updated_at__gte=since, evidence_level__gte=min_evidence)
            .order_by("evidence_level", "-updated_at")
        )

        self.stdout.write(brief_header(hours))

        for ev in qs:
            title = clean_title(ev.title or "")
            summary = clean_summary(ev.summary or "")

            # политика вывода: без summary — не показываем в брифе
            if not summary:
                continue

            if not title:
                title = f"Event {ev.id}"

            self.stdout.write(f"## L{ev.evidence_level} — {title}")
            self.stdout.write(summary)
            self.stdout.write("")
            self.stdout.write(f"- Region: `{ev.region or ''}`  Topic: `{ev.topic or ''}`")
            self.stdout.write(f"- Cluster: `{ev.cluster_key or ''}`")
            self.stdout.write("")
