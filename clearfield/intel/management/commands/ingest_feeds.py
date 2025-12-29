import asyncio
import hashlib
import time
from email.utils import parsedate_to_datetime

import aiohttp
import feedparser
from asgiref.sync import sync_to_async
from django.core.management.base import BaseCommand
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


@sync_to_async
def get_sources(limit: int):
    # Важно: материализуем в список в sync-контексте
    return list(
        Source.objects.filter(is_enabled=True).order_by("last_fetch_at")[:limit]
    )


@sync_to_async
def save_fetchlog(source: Source, status_code, elapsed_ms: int, bytes_received: int, error: str | None):
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
def upsert_items(source_id: int, items: list[dict]):
    source = Source.objects.get(id=source_id)
    for it in items:
        RawItem.objects.get_or_create(
            source=source,
            item_hash=it["item_hash"],
            defaults={
                "guid": it.get("guid", ""),
                "url": it.get("url", ""),
                "title": it.get("title", ""),
                "summary": it.get("summary", ""),
                "published_at": it.get("published_at"),
            },
        )


class Command(BaseCommand):
    help = "Fetch RSS/Atom feeds and store raw items"

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=50)

    def handle(self, *args, **options):
        asyncio.run(self.run(limit=options["limit"]))

    async def run(self, limit: int):
        sources = await get_sources(limit)

        timeout = aiohttp.ClientTimeout(total=30)
        connector = aiohttp.TCPConnector(limit=50)

        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            for source in sources:
                await self.fetch_one(session, source)

    async def fetch_one(self, session: aiohttp.ClientSession, source: Source):
        headers = {}
        if source.etag:
            headers["If-None-Match"] = source.etag
        if source.last_modified:
            headers["If-Modified-Since"] = source.last_modified

        started = time.monotonic()
        status = None
        size = 0
        error = None
        new_etag = None
        new_last_modified = None

        try:
            async with session.get(source.url, headers=headers) as resp:
                status = resp.status
                data = await resp.read()
                size = len(data)

                if status == 304:
                    # всё равно обновим last_fetch_at
                    await update_source_after_fetch(source.id, source.etag, source.last_modified)
                    return

                feed = feedparser.parse(data)
                new_etag = feed.get("etag")
                new_last_modified = feed.get("modified")

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

                await upsert_items(source.id, items_payload)
                await update_source_after_fetch(source.id, new_etag, new_last_modified)

        except Exception as e:
            error = str(e)
            # last_fetch_at тоже обновим, чтобы не долбить источник бесконечно
            await update_source_after_fetch(source.id, source.etag, source.last_modified)

        finally:
            elapsed = int((time.monotonic() - started) * 1000)
            await save_fetchlog(source, status, elapsed, size, error)
