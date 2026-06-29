import asyncio
import ssl
import time
from dataclasses import dataclass

import aiohttp
from asgiref.sync import sync_to_async
from django.core.management.base import BaseCommand
from django.db.models import Q
from django.utils import timezone
from trafilatura.core import bare_extraction

from intel.models import Article, RawItem


# =============================================================================
# HTTP DEFAULTS
# =============================================================================

UA = "CLEARFIELD/0.2 (+medical-news-pipeline)"
ACCEPT = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
ACCEPT_LANG = "ru-RU,ru;q=0.9,en-US;q=0.7,en;q=0.6"


# =============================================================================
# DTO
# =============================================================================

@dataclass
class ExtractResult:
    ok: bool
    final_url: str = ""
    title: str = ""
    text: str = ""
    lang: str = ""
    error: str = ""


# =============================================================================
# HELPERS
# =============================================================================

def build_ssl_context(allow_insecure_ssl: bool = False):
    """
    По умолчанию используем нормальную SSL-проверку.

    --allow-insecure-ssl нужен только для публичного чтения проблемных
    государственных сайтов со сломанной цепочкой сертификатов.
    """

    if allow_insecure_ssl:
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        return context

    return ssl.create_default_context()


def compact(value: str, limit: int = 500) -> str:
    value = (value or "").strip()
    value = " ".join(value.split())

    if len(value) <= limit:
        return value

    return value[:limit].rstrip() + "..."


# =============================================================================
# DB HELPERS
# =============================================================================

@sync_to_async
def pick_items(limit: int, retry_failed: bool = False):
    """
    Берём RawItem для извлечения полного текста.

    По умолчанию:
      - только RawItem без Article.

    С --retry-failed:
      - RawItem без Article
      - плюс RawItem, у которых Article есть, но extract_error не пустой.
    """

    if retry_failed:
        qs = (
            RawItem.objects
            .filter(
                Q(article__isnull=True)
                | Q(article__extract_error__isnull=False) & ~Q(article__extract_error="")
            )
            .exclude(url="")
            .select_related("source")
            .order_by("-published_at", "-created_at")[:limit]
        )
    else:
        qs = (
            RawItem.objects
            .filter(article__isnull=True)
            .exclude(url="")
            .select_related("source")
            .order_by("-published_at", "-created_at")[:limit]
        )

    return list(qs)


@sync_to_async
def save_article(item_id: int, res: ExtractResult):
    """
    Создаём или обновляем Article.
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


# =============================================================================
# NETWORK + EXTRACTION
# =============================================================================

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
    bare_extraction может вернуть dict или Document-like object.
    """

    if not html or len(html) < 200:
        return ExtractResult(
            ok=False,
            final_url=final_url,
            error="Empty or too short HTML",
        )

    data = bare_extraction(
        html,
        url=final_url,
        favor_precision=True,
    )

    if not data:
        return ExtractResult(
            ok=False,
            final_url=final_url,
            error="bare_extraction returned None",
        )

    def pick(field: str) -> str:
        if isinstance(data, dict):
            value = data.get(field)
        else:
            value = getattr(data, field, None)

        if value is None:
            return ""

        if isinstance(value, str):
            return value.strip()

        return str(value).strip()

    text = pick("text")

    if len(text) < 200:
        return ExtractResult(
            ok=False,
            final_url=final_url,
            error="No meaningful text extracted",
        )

    title = pick("title")
    lang = pick("language")

    return ExtractResult(
        ok=True,
        final_url=final_url,
        title=title,
        text=text,
        lang=lang,
    )


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

        except Exception as exc:
            last_error = str(exc)

            if attempt < retries:
                await asyncio.sleep(delay)
                delay = min(delay * 2, 10.0)

    return ExtractResult(
        ok=False,
        final_url=item.url,
        error=last_error or "Unknown error",
    )


# =============================================================================
# COMMAND
# =============================================================================

class Command(BaseCommand):
    help = "Download articles and extract full text using trafilatura."

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=50)
        parser.add_argument("--concurrency", type=int, default=10)
        parser.add_argument("--retries", type=int, default=2)
        parser.add_argument("--timeout", type=int, default=40)

        parser.add_argument(
            "--retry-failed",
            action="store_true",
            help="Retry RawItem where Article exists but extract_error is not empty.",
        )

        parser.add_argument(
            "--allow-insecure-ssl",
            action="store_true",
            help=(
                "Disable SSL verification for problematic public sources. "
                "Use only for public news fetching from broken government sites."
            ),
        )

    def handle(self, *args, **options):
        asyncio.run(self.run(**options))

    async def run(
        self,
        limit: int,
        concurrency: int,
        retries: int,
        timeout: int,
        retry_failed: bool,
        allow_insecure_ssl: bool,
        **_,
    ):
        items = await pick_items(
            limit=limit,
            retry_failed=retry_failed,
        )

        if not items:
            self.stdout.write(self.style.SUCCESS("No items to extract"))
            return

        self.stdout.write(
            f"Extracting {len(items)} items "
            f"(concurrency={concurrency}, retries={retries}, "
            f"retry_failed={retry_failed}, insecure_ssl={allow_insecure_ssl})"
        )

        ssl_context = build_ssl_context(
            allow_insecure_ssl=allow_insecure_ssl,
        )

        client_timeout = aiohttp.ClientTimeout(total=timeout)

        connector = aiohttp.TCPConnector(
            limit=concurrency * 2,
            ssl=ssl_context,
        )

        headers = {
            "User-Agent": UA,
            "Accept": ACCEPT,
            "Accept-Language": ACCEPT_LANG,
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
        }

        sem = asyncio.Semaphore(concurrency)

        ok_count = 0
        fail_count = 0

        async with aiohttp.ClientSession(
            timeout=client_timeout,
            connector=connector,
            headers=headers,
        ) as session:

            async def bounded(item: RawItem):
                nonlocal ok_count, fail_count

                async with sem:
                    started = time.monotonic()

                    result = await process_one(
                        session=session,
                        item=item,
                        retries=retries,
                    )

                    await save_article(item.id, result)

                    elapsed = int((time.monotonic() - started) * 1000)

                    status = "OK" if result.ok else "FAIL"
                    title = compact(result.title or item.title or "", 90)

                    if result.ok:
                        ok_count += 1
                        self.stdout.write(
                            f"[{status}] {elapsed}ms item={item.id} {title}"
                        )
                    else:
                        fail_count += 1
                        error = compact(result.error or "unknown", 160)
                        self.stdout.write(
                            f"[{status}] {elapsed}ms item={item.id} {title} | {error}"
                        )

            await asyncio.gather(*(bounded(item) for item in items))

        self.stdout.write(
            self.style.SUCCESS(
                f"Done. OK={ok_count}, FAIL={fail_count}"
            )
        )
