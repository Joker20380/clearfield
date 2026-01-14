import asyncio
import hashlib
import time
from email.utils import parsedate_to_datetime

import aiohttp
import feedparser
from asgiref.sync import sync_to_async
from django.core.management.base import BaseCommand
from django.db import close_old_connections
from django.utils import timezone

from intel.models import Source, FetchLog, RawItem


def make_item_hash(entry) -> str:
    base = (
        entry.get("id")
        or entry.get("guid")
        or (
            (entry.get("link", "") or "")
            + (entry.get("published", "") or "")
            + (entry.get("title", "") or "")
        )
    )
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def parse_source_ids(raw: str | None) -> list[int]:
    raw = (raw or "").strip()
    if not raw:
        return []
    parts = []
    for chunk in raw.replace(",", " ").split():
        try:
            parts.append(int(chunk))
        except Exception:
            continue
    return sorted(set(parts))


@sync_to_async
def get_sources(
    limit: int,
    source_id: int | None = None,
    source_ids: list[int] | None = None,
    only_last: int | None = None,
):
    """
    IMPORTANT: материализуем в список в sync-контексте.

    Scope filters:
      --source-id
      --source-ids
      --only-last
    """
    qs = Source.objects.filter(is_enabled=True)

    if source_id is not None:
        qs = qs.filter(id=source_id)

    if source_ids:
        qs = qs.filter(id__in=source_ids)

    if only_last:
        last_ids = list(
            Source.objects.filter(is_enabled=True)
            .order_by("-id")
            .values_list("id", flat=True)[: int(only_last)]
        )
        qs = qs.filter(id__in=last_ids)

    # Самые давно не опрашиваемые — вперёд (NULL first зависит от СУБД; MySQL обычно NULL first)
    qs = qs.order_by("last_fetch_at", "id")

    if limit:
        qs = qs[: int(limit)]

    return list(qs)


@sync_to_async
def save_fetchlog(
    source: Source,
    status_code,
    elapsed_ms: int,
    bytes_received: int,
    error: str | None,
):
    FetchLog.objects.create(
        source=source,
        status_code=status_code,
        elapsed_ms=elapsed_ms,
        bytes_received=bytes_received,
        error=error,
    )


@sync_to_async
def update_source_after_fetch(source_id: int, etag: str | None, last_modified: str | None):
    Source.objects.filter(id=source_id).update(
        last_fetch_at=timezone.now(),
        etag=etag,
        last_modified=last_modified,
    )


@sync_to_async
def upsert_items(source_id: int, items: list[dict], since_dt=None) -> int:
    """
    Возвращает кол-во НОВЫХ RawItem (created=True).
    Это полезно для метрик прогона и контроля качества источников.
    """
    source = Source.objects.get(id=source_id)
    created_count = 0

    for it in items:
        published_at = it.get("published_at")

        # опциональный срез по времени (если дата распарсилась)
        if since_dt and published_at and published_at < since_dt:
            continue

        _, created = RawItem.objects.get_or_create(
            source=source,
            item_hash=it["item_hash"],
            defaults={
                "guid": it.get("guid", ""),
                "url": it.get("url", ""),
                "title": it.get("title", ""),
                "summary": it.get("summary", ""),
                "published_at": published_at,
            },
        )
        if created:
            created_count += 1

    return created_count


class Command(BaseCommand):
    help = "Fetch RSS/Atom feeds and store raw items"

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=50)

        # NEW: scoped runs
        parser.add_argument("--source-id", type=int, default=None, help="Ingest only one Source by id")
        parser.add_argument("--source-ids", type=str, default=None, help="Comma/space separated list of Source ids")
        parser.add_argument("--only-last", type=int, default=None, help="Ingest only last N enabled Sources (by id desc)")

        # NEW: volume control
        parser.add_argument("--since-hours", type=int, default=None, help="Only store items newer than N hours (if published_at parsed)")

        # NEW: stability knob
        parser.add_argument("--concurrency", type=int, default=10, help="How many sources to fetch concurrently")

    def handle(self, *args, **options):
        asyncio.run(
            self.run(
                limit=options["limit"],
                source_id=options.get("source_id"),
                source_ids=parse_source_ids(options.get("source_ids")),
                only_last=options.get("only_last"),
                since_hours=options.get("since_hours"),
                concurrency=options.get("concurrency") or 10,
            )
        )

    async def run(
        self,
        limit: int,
        source_id: int | None,
        source_ids: list[int],
        only_last: int | None,
        since_hours: int | None,
        concurrency: int,
    ):
        close_old_connections()

        sources = await get_sources(
            limit=limit,
            source_id=source_id,
            source_ids=source_ids,
            only_last=only_last,
        )

        self.stdout.write(
            f"[ingest_feeds] scope sources={len(sources)} "
            f"(limit={limit} source_id={source_id} source_ids={len(source_ids)} only_last={only_last})"
        )

        if not sources:
            self.stdout.write("[ingest_feeds] nothing to do")
            return

        since_dt = None
        if since_hours:
            since_dt = timezone.now() - timezone.timedelta(hours=int(since_hours))
            self.stdout.write(f"[ingest_feeds] since >= {since_dt.isoformat()}")

        timeout = aiohttp.ClientTimeout(total=30)
        connector = aiohttp.TCPConnector(limit=50)
        sem = asyncio.Semaphore(int(concurrency))

        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            tasks = [self.fetch_one(session, src, sem, since_dt) for src in sources]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        ok = 0
        fail = 0
        created_total = 0

        for r in results:
            if isinstance(r, Exception):
                fail += 1
                continue
            ok += 1
            created_total += int(r or 0)

        self.stdout.write(f"[ingest_feeds] done ok={ok} fail={fail} created={created_total}")

    async def fetch_one(
        self,
        session: aiohttp.ClientSession,
        source: Source,
        sem: asyncio.Semaphore,
        since_dt,
    ) -> int:
        headers = {}
        if source.etag:
            headers["If-None-Match"] = source.etag
        if source.last_modified:
            headers["If-Modified-Since"] = source.last_modified

        started = time.monotonic()
        status = None
        size = 0
        error = None

        # ВАЖНЫЙ фикс: надёжнее хранить ETag/Last-Modified из HTTP headers ответа
        new_etag = source.etag
        new_last_modified = source.last_modified

        created_count = 0

        async with sem:
            try:
                async with session.get(source.url, headers=headers) as resp:
                    status = resp.status

                    # даже при 304 сервер может прислать актуальные метки
                    new_etag = resp.headers.get("ETag") or new_etag
                    new_last_modified = resp.headers.get("Last-Modified") or new_last_modified

                    if status == 304:
                        await update_source_after_fetch(source.id, new_etag, new_last_modified)
                        return 0

                    data = await resp.read()
                    size = len(data)

                    feed = feedparser.parse(data)

                    items_payload = []
                    for entry in feed.entries:
                        item_hash = make_item_hash(entry)

                        published_at = None
                        if entry.get("published"):
                            try:
                                published_at = parsedate_to_datetime(entry.get("published"))
                            except Exception:
                                published_at = None

                        items_payload.append(
                            {
                                "item_hash": item_hash,
                                "guid": entry.get("id") or entry.get("guid", ""),
                                "url": entry.get("link", ""),
                                "title": entry.get("title", ""),
                                "summary": entry.get("summary", ""),
                                "published_at": published_at,
                            }
                        )

                    created_count = await upsert_items(source.id, items_payload, since_dt=since_dt)
                    await update_source_after_fetch(source.id, new_etag, new_last_modified)

            except Exception as e:
                error = str(e)
                # last_fetch_at тоже обновим, чтобы не долбить источник бесконечно
                await update_source_after_fetch(source.id, new_etag, new_last_modified)

            finally:
                elapsed = int((time.monotonic() - started) * 1000)
                await save_fetchlog(source, status, elapsed, size, error)

        return created_count
