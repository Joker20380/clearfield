import asyncio
import time
from dataclasses import dataclass

import aiohttp
import trafilatura
from trafilatura.core import bare_extraction
from asgiref.sync import sync_to_async
from django.core.management.base import BaseCommand
from django.utils import timezone

from intel.models import RawItem, Article


# =========================
# HTTP defaults
# =========================
UA = "CLEARFIELD/0.1 (+https://github.com/Joker20380/clearfield)"
ACCEPT = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
ACCEPT_LANG = "en-US,en;q=0.9,ru;q=0.8"


# =========================
# DTO
# =========================
@dataclass
class ExtractResult:
    ok: bool
    final_url: str = ""
    title: str = ""
    text: str = ""
    lang: str = ""
    error: str = ""


# =========================
# DB helpers (sync ORM)
# =========================
@sync_to_async
def pick_items(limit: int):
    """
    Берём RawItem без Article, свежие сначала
    """
    qs = (
        RawItem.objects
        .filter(article__isnull=True)
        .exclude(url="")
        .order_by("-published_at", "-created_at")[:limit]
    )
    return list(qs)


@sync_to_async
def save_article(item_id: int, res: ExtractResult):
    """
    Создаём или обновляем Article
    """
    Article.objects.update_or_create(
        item_id=item_id,
        defaults={
            "final_url": res.final_url,
            "title": res.title,
            "text": res.text,
            "lang": res.lang,
            "extracted_at": timezone.now(),
            "extract_error": "" if res.ok else res.error,
        },
    )


# =========================
# Network + extraction
# =========================
async def fetch_html(session: aiohttp.ClientSession, url: str) -> tuple[str, str]:
    async with session.get(url, allow_redirects=True) as resp:
        if resp.status >= 400:
            raise RuntimeError(f"HTTP {resp.status}")
        final_url = str(resp.url)
        html = await resp.text(errors="ignore")
        return final_url, html


def extract_from_html(final_url: str, html: str) -> ExtractResult:
    """
    Универсально для разных версий trafilatura:
    bare_extraction может вернуть dict или Document-like объект.
    """
    if not html or len(html) < 200:
        return ExtractResult(ok=False, final_url=final_url, error="Empty or too short HTML")

    data = bare_extraction(html, url=final_url, favor_precision=True)
    if not data:
        return ExtractResult(ok=False, final_url=final_url, error="bare_extraction returned None")

    # --- normalize getters (dict vs object) ---
    def pick(field: str) -> str:
        if isinstance(data, dict):
            val = data.get(field)
        else:
            val = getattr(data, field, None)
        return (val or "").strip() if isinstance(val, str) or val is None else str(val).strip()

    text = pick("text")
    if len(text) < 200:
        return ExtractResult(ok=False, final_url=final_url, error="No meaningful text extracted")

    title = pick("title")
    lang = pick("language")

    return ExtractResult(ok=True, final_url=final_url, title=title, text=text, lang=lang)


async def process_one(
    session: aiohttp.ClientSession,
    item: RawItem,
    retries: int,
) -> ExtractResult:
    delay = 1.0
    last_error = None

    for attempt in range(retries + 1):
        try:
            final_url, html = await fetch_html(session, item.url)
            return extract_from_html(final_url, html)
        except Exception as e:
            last_error = str(e)
            if attempt < retries:
                await asyncio.sleep(delay)
                delay = min(delay * 2, 10.0)

    return ExtractResult(
        ok=False,
        final_url=item.url,
        error=last_error or "Unknown error",
    )


# =========================
# Django command
# =========================
class Command(BaseCommand):
    help = "Download articles and extract full text using trafilatura (bare_extraction)"

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=50)
        parser.add_argument("--concurrency", type=int, default=10)
        parser.add_argument("--retries", type=int, default=2)
        parser.add_argument("--timeout", type=int, default=40)

    def handle(self, *args, **options):
        asyncio.run(self.run(**options))

    async def run(
        self,
        limit: int,
        concurrency: int,
        retries: int,
        timeout: int,
        **_,
    ):
        items = await pick_items(limit)
        if not items:
            self.stdout.write(self.style.SUCCESS("No items to extract"))
            return

        self.stdout.write(
            f"Extracting {len(items)} items "
            f"(concurrency={concurrency}, retries={retries})"
        )

        client_timeout = aiohttp.ClientTimeout(total=timeout)
        connector = aiohttp.TCPConnector(limit=concurrency * 2)

        headers = {
            "User-Agent": UA,
            "Accept": ACCEPT,
            "Accept-Language": ACCEPT_LANG,
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }

        sem = asyncio.Semaphore(concurrency)

        async with aiohttp.ClientSession(
            timeout=client_timeout,
            connector=connector,
            headers=headers,
        ) as session:

            async def bounded(item: RawItem):
                async with sem:
                    start = time.monotonic()
                    result = await process_one(session, item, retries)
                    await save_article(item.id, result)
                    elapsed = int((time.monotonic() - start) * 1000)

                    status = "OK" if result.ok else "FAIL"
                    title = (result.title or item.title or "")[:80]

                    if result.ok:
                        self.stdout.write(f"[{status}] {elapsed}ms item={item.id} {title}")
                    else:
                        err = (result.error or "unknown")[:140]
                        self.stdout.write(f"[{status}] {elapsed}ms item={item.id} {title} | {err}")

            await asyncio.gather(*(bounded(it) for it in items))

        self.stdout.write(self.style.SUCCESS("Done"))
