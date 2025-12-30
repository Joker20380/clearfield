import asyncio
import re
from dataclasses import dataclass
from datetime import timedelta
from hashlib import md5
from urllib.parse import urlparse

from asgiref.sync import sync_to_async
from django.core.management.base import BaseCommand
from django.utils import timezone

from intel.models import Article, RawItem, Event, EventItem


WORD_RE = re.compile(r"[A-Za-zА-Яа-я0-9]{3,}")

STOP = {
    # EN
    "this","that","with","from","into","over","after","before","about","their","there",
    "says","said","say","will","would","could","should","also","more","most","less",
    "new","news","live","latest","update","updates",
    # RU
    "что","это","как","для","или","при","после","перед","также","ещё","еще",
    "котор","которые","который","которого","которым","которых",
}

NOISE_PHRASES = [
    "One of your browser extensions seems to be blocking the video player",
    "To watch this content, you may need to disable it on this site",
    "Follow our liveblog",
    "Live:",
]


def sanitize(text: str) -> str:
    t = (text or "").strip()
    for p in NOISE_PHRASES:
        t = t.replace(p, " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t


def tokenize(text: str):
    words = [w.lower() for w in WORD_RE.findall(text)]
    return [w for w in words if w not in STOP]


def simhash(tokens, bits: int = 64) -> int:
    v = [0] * bits
    for t in tokens:
        h = int(md5(t.encode("utf-8")).hexdigest(), 16)
        for i in range(bits):
            bit = (h >> i) & 1
            v[i] += 1 if bit else -1
    out = 0
    for i in range(bits):
        if v[i] > 0:
            out |= (1 << i)
    return out


def hamming(a: int, b: int) -> int:
    return (a ^ b).bit_count()


def domain(url: str) -> str:
    try:
        return (urlparse(url).netloc or "").lower()
    except Exception:
        return ""


@dataclass
class Candidate:
    item_id: int
    url: str
    dom: str
    title: str
    text: str
    region: str
    topic: str
    source_class: str
    sh: int


@sync_to_async
def load_candidates(since_hours: int, limit: int):
    """
    Грузим кандидатов из Article (+RawItem+Source), считаем simhash по (title + cleaned text snippet).
    """
    since = timezone.now() - timedelta(hours=since_hours)
    qs = (
        Article.objects
        .select_related("item", "item__source")
        .exclude(text="")
        .filter(extracted_at__gte=since)
        .order_by("-extracted_at")[:limit]
    )

    out: list[Candidate] = []

    for a in qs:
        item: RawItem = a.item
        src = item.source

        # region/topic/source_class — берём максимально мягко (не падаем, если поля нет)
        region = (getattr(src, "region", "") or "")
        topic = (getattr(src, "topic", "") or "")
        source_class = (getattr(src, "source_class", "") or getattr(src, "kind", "") or "")

        clean_text = sanitize(a.text)[:2500]
        merged = (a.title or item.title or "") + "\n" + clean_text

        toks = tokenize(merged)
        # ограничим токены, чтобы CPU не рвало
        toks = toks[:500]
        sh = simhash(toks)

        out.append(
            Candidate(
                item_id=item.id,
                url=item.url,
                dom=domain(item.url),
                title=(a.title or item.title or ""),
                text=a.text,
                region=region,
                topic=topic,
                source_class=source_class,
                sh=sh,
            )
        )

    return out


@sync_to_async
def item_already_linked(item_id: int) -> bool:
    return EventItem.objects.filter(item_id=item_id).exists()


@sync_to_async
def upsert_event(cluster_key: str, title: str, summary: str, region: str, topic: str, evidence_level: int):
    ev, _ = Event.objects.update_or_create(
        cluster_key=cluster_key,
        defaults={
            "title": title[:300],
            "summary": summary[:2000],
            "region": region[:16],
            "topic": topic[:16],
            "evidence_level": evidence_level,
        },
    )
    return ev.id


@sync_to_async
def link_item(event_id: int, item_id: int):
    EventItem.objects.get_or_create(event_id=event_id, item_id=item_id)


def pick_event_title(c: Candidate) -> str:
    return (c.title or "").strip()[:200]


def pick_summary(c: Candidate) -> str:
    text = sanitize(c.text)
    parts = re.split(r"(?<=[.!?])\s+", text)
    return " ".join(parts[:3])[:1200]


def evidence_from_cluster(cluster: list[Candidate]) -> int:
    """
    MVP-приближение к твоим уровням 0–3, без внешних источников.

    L1: >=2 разных домена ИЛИ >=2 items
    L2: есть source_class=official ИЛИ >=2 домена + >=2 items
    L3: >=2 разных классов источников (agency+official/local/osint) И >=3 items
    """
    if not cluster:
        return 0

    doms = {c.dom for c in cluster if c.dom}
    classes = {c.source_class for c in cluster if c.source_class}

    n = len(cluster)

    if n >= 3 and len(classes) >= 2:
        return 3

    if ("official" in classes) or (n >= 2 and len(doms) >= 2):
        return 2

    if n >= 2 or len(doms) >= 2:
        return 1

    return 0


class Command(BaseCommand):
    help = "Cluster extracted articles into Events (SimHash MVP, sanitized)"

    def add_arguments(self, parser):
        parser.add_argument("--since-hours", type=int, default=72)
        parser.add_argument("--limit", type=int, default=500)
        parser.add_argument("--max-dist", type=int, default=10)

    def handle(self, *args, **opts):
        asyncio.run(self.run(**opts))

    async def run(self, since_hours: int, limit: int, max_dist: int, **_):
        candidates = await load_candidates(since_hours, limit)
        if not candidates:
            self.stdout.write(self.style.WARNING("No candidates."))
            return

        clusters: list[list[Candidate]] = []

        for c in candidates:
            if await item_already_linked(c.item_id):
                continue

            placed = False
            for cl in clusters:
                # сравниваем с "головой" кластера
                if hamming(c.sh, cl[0].sh) <= max_dist:
                    cl.append(c)
                    placed = True
                    break

            if not placed:
                clusters.append([c])

        created = 0
        linked = 0

        for cl in clusters:
            # выбираем наиболее "мясной" текст как head
            cl.sort(key=lambda x: len(x.text), reverse=True)
            head = cl[0]

            # cluster_key = simhash head (hex)
            cluster_key = f"sh64:{head.sh:x}"

            title = pick_event_title(head) or "Untitled event"
            summary = pick_summary(head)

            region = head.region
            topic = head.topic

            level = evidence_from_cluster(cl)

            event_id = await upsert_event(cluster_key, title, summary, region, topic, level)
            created += 1

            for c in cl:
                await link_item(event_id, c.item_id)
                linked += 1

        self.stdout.write(self.style.SUCCESS(f"Events upserted: {created}, items linked: {linked}"))
