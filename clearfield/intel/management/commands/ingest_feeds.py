import asyncio
import hashlib
import ssl
import time
from dataclasses import dataclass
from datetime import datetime as dt_datetime
from datetime import timezone as dt_timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from urllib.parse import urljoin, urlsplit

import aiohttp
import feedparser
from asgiref.sync import sync_to_async
from django.core.management.base import BaseCommand
from django.db import close_old_connections
from django.utils import timezone

from intel.models import FetchLog, RawItem, Source, Topic


# =============================================================================
# HTTP DEFAULTS
# =============================================================================

UA = "CLEARFIELD/0.2 (+medical-news-pipeline)"
ACCEPT = (
    "text/html,application/xhtml+xml,application/xml;q=0.9,"
    "application/rss+xml;q=0.9,application/atom+xml;q=0.9,*/*;q=0.8"
)
ACCEPT_LANG = "ru-RU,ru;q=0.9,en-US;q=0.7,en;q=0.6"


# =============================================================================
# HTML LINK FILTERING
# =============================================================================

BAD_EXTENSIONS = (
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg",
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    ".zip", ".rar", ".7z", ".mp4", ".mp3", ".avi", ".mov",
)

NEWS_PATH_MARKERS = (
    "/news",
    "/novosti",
    "/press",
    "/pressa",
    "/press-center",
    "/press_center",
    "/presscentre",
    "/press-centre",
    "/about/news",
    "/events",
    "/event",
    "news",
    "novost",
    "press",
)

BAD_LINK_TITLES_EXACT = {
    "пресс-служба",
    "календарь событий",
    "календарь",
    "пресс-релизы",
    "пресс релизы",
    "фотоматериалы",
    "фото",
    "видео",
    "показать ещё",
    "показать еще",
    "подробнее",
    "читать далее",
    "далее",
    "все новости",
    "архив новостей",
    "архив",
    "новости",
    "события",
    "главная",
    "контакты",
    "карта сайта",
    "поиск",
    "версия для слабовидящих",
    "просветительские проекты",
    "разделы сайта",
    "официально",
    "документы",
    "об учреждении",
    "о нас",
    "обратная связь",
    "личный кабинет",
    "rss",
    "подписаться",
    "социальные сети",
    "вконтакте",
    "telegram",
    "одноклассники",
}

BAD_LINK_TITLE_PARTS = (
    "показать ещё",
    "показать еще",
    "читать далее",
    "перейти в раздел",
    "все материалы",
    "все новости",
    "архив новостей",
    "версия для слабовидящих",
    "карта сайта",
    "поделиться",
    "скачать",
    "подписаться",
)

BAD_URL_PARTS = (
    "/contacts",
    "/contact",
    "/search",
    "/map",
    "/sitemap",
    "/photo",
    "/video",
    "/gallery",
    "/rss",
    "/upload/",
    "/documents",
    "/docs",
    "/about",
    "/structure",
    "/reception",
    "/appeals",
    "/anticorruption",
    "/vacancy",
    "/vacancies",
)


# =============================================================================
# DTO
# =============================================================================

@dataclass
class IngestedItem:
    guid: str
    url: str
    title: str
    summary: str
    published_at: object | None
    item_hash: str


# =============================================================================
# HTML PARSER
# =============================================================================

class LinkExtractor(HTMLParser):
    """
    Минимальный HTML-парсер ссылок без внешних зависимостей.

    Используется только как fallback, если URL источника — не RSS/Atom,
    а обычная HTML-страница новостей.
    """

    def __init__(self):
        super().__init__()
        self.links: list[dict] = []
        self._current_href = ""
        self._current_title = ""
        self._buffer: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "a":
            return

        attrs_dict = dict(attrs)
        href = (attrs_dict.get("href") or "").strip()

        if not href:
            return

        self._current_href = href
        self._current_title = (
            attrs_dict.get("title")
            or attrs_dict.get("aria-label")
            or ""
        ).strip()
        self._buffer = []

    def handle_data(self, data):
        if self._current_href:
            self._buffer.append(data)

    def handle_endtag(self, tag):
        if tag.lower() != "a":
            return

        if self._current_href:
            text = " ".join(" ".join(self._buffer).split()).strip()
            title = self._current_title or text

            self.links.append(
                {
                    "href": self._current_href,
                    "title": title,
                    "text": text,
                }
            )

        self._current_href = ""
        self._current_title = ""
        self._buffer = []


# =============================================================================
# HELPERS
# =============================================================================

def compact(value: str, limit: int = 500) -> str:
    value = (value or "").strip()
    value = " ".join(value.split())

    if len(value) <= limit:
        return value

    return value[:limit].rstrip() + "..."


def normalized_title(value: str) -> str:
    value = compact(value, 300).lower()
    value = value.strip(" .,:;!?—-«»\"'()[]{}")
    return value


def is_bad_link_title(title: str) -> bool:
    value = normalized_title(title)

    if not value:
        return True

    if value in BAD_LINK_TITLES_EXACT:
        return True

    if any(part in value for part in BAD_LINK_TITLE_PARTS):
        return True

    # Слишком короткие заголовки почти всегда навигация.
    if len(value) < 12:
        return True

    # Если в заголовке нет букв/цифр, это не новость.
    has_meaningful_char = any(ch.isalnum() for ch in value)
    if not has_meaningful_char:
        return True

    return False


def sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8", errors="ignore")).hexdigest()


def make_item_hash_from_feed(entry) -> str:
    base = (
        entry.get("id")
        or entry.get("guid")
        or entry.get("link")
        or (
            (entry.get("published", "") or "")
            + (entry.get("title", "") or "")
            + (entry.get("summary", "") or "")
        )
    )

    return sha256(base)


def make_item_hash_from_html(url: str, title: str) -> str:
    return sha256(f"{url}|{title}")


def parse_source_ids(raw: str | None) -> list[int]:
    raw = (raw or "").strip()

    if not raw:
        return []

    result = []

    for chunk in raw.replace(",", " ").split():
        try:
            result.append(int(chunk))
        except Exception:
            continue

    return sorted(set(result))


def normalize_published(entry):
    """
    Пытаемся достать дату из RSS/Atom.
    Если дата не распарсилась — возвращаем None.
    """

    for key in ("published", "updated", "created"):
        raw = entry.get(key)

        if raw:
            try:
                parsed = parsedate_to_datetime(raw)

                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=dt_timezone.utc)

                return parsed
            except Exception:
                pass

    for key in ("published_parsed", "updated_parsed", "created_parsed"):
        struct_value = entry.get(key)

        if struct_value:
            try:
                return dt_datetime.fromtimestamp(
                    time.mktime(struct_value),
                    tz=dt_timezone.utc,
                )
            except Exception:
                pass

    return None


def normalize_url(base_url: str, href: str) -> str:
    href = (href or "").strip()

    if not href:
        return ""

    if href.startswith(("mailto:", "tel:", "javascript:", "#")):
        return ""

    return urljoin(base_url, href).split("#", 1)[0].strip()


def is_same_site(source_url: str, candidate_url: str) -> bool:
    try:
        source_host = urlsplit(source_url).netloc.lower().replace("www.", "")
        candidate_host = urlsplit(candidate_url).netloc.lower().replace("www.", "")
        return source_host == candidate_host
    except Exception:
        return False


def has_bad_url_part(candidate_url: str) -> bool:
    url_lower = (candidate_url or "").lower()
    return any(part in url_lower for part in BAD_URL_PARTS)


def looks_like_news_link(source_url: str, candidate_url: str, title: str) -> bool:
    if not candidate_url:
        return False

    if not candidate_url.startswith(("http://", "https://")):
        return False

    if not is_same_site(source_url, candidate_url):
        return False

    parsed = urlsplit(candidate_url)
    path = (parsed.path or "").lower()

    if not path or path == "/":
        return False

    if path.endswith(BAD_EXTENSIONS):
        return False

    if has_bad_url_part(candidate_url):
        return False

    title_clean = compact(title, 200)

    if is_bad_link_title(title_clean):
        return False

    full_url = candidate_url.lower()
    has_news_marker = any(marker in full_url for marker in NEWS_PATH_MARKERS)

    if not has_news_marker:
        return False

    return True


def extract_html_items(source_url: str, html: str, max_items: int) -> list[IngestedItem]:
    parser = LinkExtractor()

    try:
        parser.feed(html)
    except Exception:
        return []

    result: list[IngestedItem] = []
    seen_urls: set[str] = set()

    for link in parser.links:
        title = compact(link.get("title") or link.get("text") or "", 300)
        url = normalize_url(source_url, link.get("href") or "")

        if not looks_like_news_link(source_url, url, title):
            continue

        if url in seen_urls:
            continue

        seen_urls.add(url)

        result.append(
            IngestedItem(
                guid=url,
                url=url,
                title=title,
                summary="",
                published_at=None,
                item_hash=make_item_hash_from_html(url, title),
            )
        )

        if len(result) >= max_items:
            break

    return result


def extract_feed_items(data: bytes, max_items: int) -> list[IngestedItem]:
    feed = feedparser.parse(data)

    result: list[IngestedItem] = []

    for entry in feed.entries[:max_items]:
        url = (entry.get("link") or "").strip()
        title = compact(entry.get("title") or "", 500)
        summary = compact(entry.get("summary") or entry.get("description") or "", 1500)

        if not url and not title:
            continue

        if is_bad_link_title(title):
            continue

        result.append(
            IngestedItem(
                guid=entry.get("id") or entry.get("guid") or url,
                url=url,
                title=title,
                summary=summary,
                published_at=normalize_published(entry),
                item_hash=make_item_hash_from_feed(entry),
            )
        )

    return result


def build_ssl_context(allow_insecure_ssl: bool = False):
    """
    По умолчанию используем нормальную проверку SSL.

    --allow-insecure-ssl нужен только как аварийный режим для проблемных
    государственных сайтов со сломанными цепочками сертификатов.
    """

    if allow_insecure_ssl:
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        return context

    return ssl.create_default_context()


# =============================================================================
# DB HELPERS
# =============================================================================

@sync_to_async
def get_sources(
    limit: int,
    source_id: int | None = None,
    source_ids: list[int] | None = None,
    only_last: int | None = None,
    topic: str | None = None,
):
    base_qs = Source.objects.filter(
        is_enabled=True,
    )

    if topic:
        base_qs = base_qs.filter(
            topic=topic,
        )

    qs = base_qs

    if source_id is not None:
        qs = qs.filter(id=source_id)

    if source_ids:
        qs = qs.filter(id__in=source_ids)

    if only_last:
        last_ids = list(
            base_qs
            .order_by("-id")
            .values_list(
                "id",
                flat=True,
            )[: int(only_last)]
        )

        qs = qs.filter(
            id__in=last_ids,
        )

    # Самые давно не опрашиваемые — вперёд.
    qs = qs.order_by(
        "last_fetch_at",
        "id",
    )

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
def upsert_items(source_id: int, items: list[IngestedItem], since_dt=None) -> int:
    source = Source.objects.get(id=source_id)
    created_count = 0

    for item in items:
        published_at = item.published_at

        if since_dt and published_at and published_at < since_dt:
            continue

        if is_bad_link_title(item.title):
            continue

        _, created = RawItem.objects.get_or_create(
            source=source,
            item_hash=item.item_hash,
            defaults={
                "guid": item.guid or "",
                "url": item.url or "",
                "title": item.title or "",
                "summary": item.summary or "",
                "published_at": published_at,
            },
        )

        if created:
            created_count += 1

    return created_count


# =============================================================================
# COMMAND
# =============================================================================

class Command(BaseCommand):
    help = "Fetch RSS/Atom feeds or HTML news pages and store RawItem."

    def add_arguments(self, parser):
        parser.add_argument(
            "--limit",
            type=int,
            default=50,
        )

        parser.add_argument(
            "--topic",
            choices=[
                value
                for value, _label
                in Topic.choices
            ],
            default=None,
            help=(
                "Ingest only enabled Sources "
                "with this topic."
            ),
        )

        parser.add_argument(
            "--source-id",
            type=int,
            default=None,
            help="Ingest only one Source by id.",
        )
        parser.add_argument(
            "--source-ids",
            type=str,
            default=None,
            help="Comma/space separated list of Source ids.",
        )
        parser.add_argument(
            "--only-last",
            type=int,
            default=None,
            help="Ingest only last N enabled Sources by id desc.",
        )

        parser.add_argument(
            "--since-hours",
            type=int,
            default=None,
            help="Only store feed items newer than N hours if date parsed.",
        )
        parser.add_argument(
            "--concurrency",
            type=int,
            default=5,
            help="How many sources to fetch concurrently.",
        )
        parser.add_argument(
            "--max-items-per-source",
            type=int,
            default=40,
            help="Max items extracted from one source.",
        )
        parser.add_argument(
            "--allow-insecure-ssl",
            action="store_true",
            help=(
                "Disable SSL verification for problematic sources. "
                "Use only for public news fetching from broken government sites."
            ),
        )

    def handle(self, *args, **options):
        asyncio.run(
            self.run(
                limit=options["limit"],
                topic=options.get("topic"),
                source_id=options.get("source_id"),
                source_ids=parse_source_ids(options.get("source_ids")),
                only_last=options.get("only_last"),
                since_hours=options.get("since_hours"),
                concurrency=options.get("concurrency") or 5,
                max_items_per_source=options.get("max_items_per_source") or 40,
                allow_insecure_ssl=options.get("allow_insecure_ssl") or False,
            )
        )

    async def run(
        self,
        limit: int,
        topic: str | None,
        source_id: int | None,
        source_ids: list[int],
        only_last: int | None,
        since_hours: int | None,
        concurrency: int,
        max_items_per_source: int,
        allow_insecure_ssl: bool,
    ):
        close_old_connections()

        sources = await get_sources(
            limit=limit,
            source_id=source_id,
            source_ids=source_ids,
            only_last=only_last,
            topic=topic,
        )

        self.stdout.write(
            f"[ingest_feeds] scope sources={len(sources)} "
            f"(topic={topic} limit={limit} "
            f"source_id={source_id} "
            f"source_ids={len(source_ids)} "
            f"only_last={only_last})"
        )

        if not sources:
            self.stdout.write("[ingest_feeds] nothing to do")
            return

        since_dt = None

        if since_hours:
            since_dt = timezone.now() - timezone.timedelta(hours=int(since_hours))
            self.stdout.write(f"[ingest_feeds] since >= {since_dt.isoformat()}")

        ssl_context = build_ssl_context(allow_insecure_ssl=allow_insecure_ssl)

        timeout = aiohttp.ClientTimeout(total=35)
        connector = aiohttp.TCPConnector(
            limit=max(10, concurrency * 2),
            ssl=ssl_context,
        )
        sem = asyncio.Semaphore(int(concurrency))

        headers = {
            "User-Agent": UA,
            "Accept": ACCEPT,
            "Accept-Language": ACCEPT_LANG,
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
        }

        async with aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers=headers,
        ) as session:
            tasks = [
                self.fetch_one(
                    session=session,
                    source=source,
                    sem=sem,
                    since_dt=since_dt,
                    max_items_per_source=max_items_per_source,
                )
                for source in sources
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        ok = 0
        fail = 0
        created_total = 0

        for result in results:
            if isinstance(result, Exception):
                fail += 1
                continue

            ok += 1
            created_total += int(result or 0)

        self.stdout.write(
            f"[ingest_feeds] done ok={ok} fail={fail} created={created_total}"
        )

    async def fetch_one(
        self,
        session: aiohttp.ClientSession,
        source: Source,
        sem: asyncio.Semaphore,
        since_dt,
        max_items_per_source: int,
    ) -> int:
        request_headers = {}

        if source.etag:
            request_headers["If-None-Match"] = source.etag

        if source.last_modified:
            request_headers["If-Modified-Since"] = source.last_modified

        started = time.monotonic()
        status = None
        size = 0
        error = None

        new_etag = source.etag
        new_last_modified = source.last_modified

        created_count = 0
        parsed_count = 0
        parser_used = "none"

        async with sem:
            try:
                async with session.get(
                    source.url,
                    headers=request_headers,
                    allow_redirects=True,
                ) as resp:
                    status = resp.status

                    new_etag = resp.headers.get("ETag") or new_etag
                    new_last_modified = resp.headers.get("Last-Modified") or new_last_modified

                    if status == 304:
                        await update_source_after_fetch(
                            source.id,
                            new_etag,
                            new_last_modified,
                        )
                        self.stdout.write(
                            f"[ingest_feeds] 304 source={source.id} {source.name}"
                        )
                        return 0

                    if status >= 400:
                        raise RuntimeError(f"HTTP {status}")

                    data = await resp.read()
                    size = len(data)

                    items = extract_feed_items(
                        data,
                        max_items=max_items_per_source,
                    )
                    parser_used = "feed"
                    parsed_count = len(items)

                    if not items:
                        try:
                            charset = resp.charset or resp.get_encoding() or "utf-8"
                        except Exception:
                            charset = "utf-8"

                        html = data.decode(charset, errors="replace")

                        items = extract_html_items(
                            source.url,
                            html,
                            max_items=max_items_per_source,
                        )
                        parser_used = "html"
                        parsed_count = len(items)

                    if not items:
                        error = "No RSS/Atom entries and no valid HTML news links found"
                    else:
                        created_count = await upsert_items(
                            source.id,
                            items,
                            since_dt=since_dt,
                        )

                    await update_source_after_fetch(
                        source.id,
                        new_etag,
                        new_last_modified,
                    )

            except Exception as exc:
                error = str(exc)

                await update_source_after_fetch(
                    source.id,
                    new_etag,
                    new_last_modified,
                )

            finally:
                elapsed = int((time.monotonic() - started) * 1000)

                await save_fetchlog(
                    source=source,
                    status_code=status,
                    elapsed_ms=elapsed,
                    bytes_received=size,
                    error=error,
                )

        if error:
            self.stdout.write(
                f"[ingest_feeds] FAIL source={source.id} parser={parser_used} "
                f"status={status} parsed={parsed_count} created={created_count} | "
                f"{error[:160]}"
            )
        else:
            self.stdout.write(
                f"[ingest_feeds] OK source={source.id} parser={parser_used} "
                f"status={status} parsed={parsed_count} created={created_count}"
            )

        return created_count
