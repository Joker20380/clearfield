from datetime import timedelta
from django.core.management.base import BaseCommand
from django.utils import timezone

from intel.models import Event


class Command(BaseCommand):
    help = "Print daily brief (Markdown) from Events"

    def add_arguments(self, parser):
        parser.add_argument("--hours", type=int, default=24)
        parser.add_argument("--min-evidence", type=int, default=1)

    def handle(self, *args, **opts):
        since = timezone.now() - timedelta(hours=opts["hours"])
        qs = (
            Event.objects
            .filter(updated_at__gte=since, evidence_level__gte=opts["min_evidence"])
            .order_by("-evidence_level", "-updated_at")[:30]
        )

        self.stdout.write(f"# CLEARFIELD Brief — last {opts['hours']}h\n")
        for ev in qs:
            self.stdout.write(f"## L{ev.evidence_level} — {ev.title}\n")
            if ev.summary:
                self.stdout.write(f"{ev.summary}\n\n")
            self.stdout.write(f"- Region: `{ev.region}`  Topic: `{ev.topic}`\n")
            self.stdout.write(f"- Cluster: `{ev.cluster_key}`\n")
            self.stdout.write("\n")
