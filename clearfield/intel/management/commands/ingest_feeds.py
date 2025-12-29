import asyncio
import hashlib
import time
from email.utils import parsedate_to_datetime

import aiohttp
import feedparser
from django.core.management.base import BaseCommand
from django.utils import timezone

from intel.models import Source, FetchLog, RawItem


def make_item_hash(entry):
    base = (
        entry.get("id")
        or entry.get("guid")
        or entry.get("link", "")
        + (entry.get("published", "") or "")
        + (entry.get("title", "") or "")
    )
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


class Command(BaseCommand):
    help = "Fetch RSS/Atom feeds and store raw items"

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=50)

    def handle(self, *args, **options):
        asyncio.run(self.run(limit=options["limit"]))

    async def run(self, limit: int):
        sources = (
            Source.objects
            .filter(is_enabled=True)
            .order_by("last_fetch_at")[:limit]
        )

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

        try:
            async with session.get(source.url, headers=headers) as resp:
                status = resp.status
                data = await resp.read()
                size = len(data)

                if status == 304:
                    return

                feed = feedparser.parse(data)

                source.etag = feed.get("etag")
                source.last_modified = feed.get("modified")

                for entry in feed.entries:
                    h = make_item_hash(entry)
                    RawItem.objects.get_or_create(
                        source=source,
                        item_hash=h,
                        defaults={
                            "guid": entry.get("id") or entry.get("guid", ""),
                            "url": entry.get("link", ""),
                            "title": entry.get("title", ""),
                            "summary": entry.get("summary", ""),
                            "published_at": (
                                parsedate_to_datetime(entry.get("published"))
                                if entry.get("published") else None
                            ),
                        },
                    )

        except Exception as e:
            error = str(e)

        finally:
            elapsed = int((time.monotonic() - started) * 1000)

            FetchLog.objects.create(
                source=source,
                status_code=status,
                elapsed_ms=elapsed,
                bytes_received=size,
                error=error,
            )

            source.last_fetch_at = timezone.now()
            source.save(update_fields=["last_fetch_at", "etag", "last_modified"])
