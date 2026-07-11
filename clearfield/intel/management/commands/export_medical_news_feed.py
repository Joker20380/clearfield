import json
import secrets
from pathlib import Path
from urllib.parse import urlparse

from django.core.management.base import BaseCommand
from django.utils import timezone

from intel.models import GeneratedMedicalNews


TITLE_FIELDS = ["title", "headline", "name"]
CONTENT_FIELDS = ["content", "body", "text", "article", "html"]


def first_obj_value(obj, fields):
    for field in fields:
        if hasattr(obj, field):
            value = getattr(obj, field) or ""
            if value:
                return str(value)
    return ""


def get_source_urls(brief):
    raw_value = getattr(brief, "source_urls", "") or ""
    urls = []

    for raw_line in str(raw_value).splitlines():
        url = raw_line.strip()

        if not url:
            continue

        parsed = urlparse(url)

        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.netloc
        ):
            continue

        if url not in urls:
            urls.append(url)

    return urls


def append_source_links(content, urls):
    missing_urls = [
        url
        for url in urls
        if url not in content
    ]

    if not missing_urls:
        return content

    if len(missing_urls) == 1:
        source_block = f"[Источник]({missing_urls[0]})"
    else:
        source_lines = ["## Источники", ""]

        for index, url in enumerate(missing_urls, start=1):
            source_lines.append(
                f"- [Источник {index}]({url})"
            )

        source_block = "\n".join(source_lines)

    return f"{content.rstrip()}\n\n{source_block}\n"


def get_or_create_token(base_dir):
    token_file = base_dir / ".medical_news_feed_token"

    if token_file.exists():
        return token_file.read_text(encoding="utf-8").strip()

    token = secrets.token_hex(16)
    token_file.write_text(token, encoding="utf-8")
    return token


class Command(BaseCommand):
    help = "Export published GeneratedMedicalNews records to public JSON feed."

    def add_arguments(self, parser):
        parser.add_argument("--status", default="published")
        parser.add_argument("--limit", type=int, default=20)
        parser.add_argument("--public-dir", default="../generated-news")
        parser.add_argument("--filename", default="")
        parser.add_argument("--show-content-size", action="store_true")

    def handle(self, *args, **options):
        status = options["status"]
        limit = options["limit"]
        public_dir = Path(options["public_dir"]).resolve()
        filename = options["filename"]

        public_dir.mkdir(parents=True, exist_ok=True)

        token = get_or_create_token(public_dir)

        if not filename:
            filename = f"medical-news-feed-{token}.json"

        output_path = public_dir / filename

        # Newest first. Take extra rows so duplicate titles do not reduce the final feed too much.
        qs_limit = max(limit * 5, limit)
        qs = (
            GeneratedMedicalNews.objects
            .filter(status=status)
            .select_related("brief")
            .order_by("-id")[:qs_limit]
        )

        items = []
        seen_titles = set()

        for item in qs:
            title = first_obj_value(item, TITLE_FIELDS)
            content = first_obj_value(item, CONTENT_FIELDS)
            brief = getattr(item, "brief", None)

            content = append_source_links(
                content,
                get_source_urls(brief),
            )

            if not title or not content:
                self.stdout.write(self.style.WARNING(f"SKIP #{item.id}: empty title/content"))
                continue

            title_key = " ".join(title.lower().split())
            if title_key in seen_titles:
                self.stdout.write(self.style.WARNING(f"SKIP #{item.id}: duplicate title"))
                continue
            seen_titles.add(title_key)

            items.append({
                "source_id": item.id,
                "title": title,
                "content": content,
                "target_keyword": str(
                    getattr(brief, "target_keyword", "") or ""
                ),
                "theme": str(
                    getattr(brief, "angle", "") or ""
                ),
                "image_topic": first_obj_value(item, ["image_topic"]),
                "created_at": item.created_at.isoformat() if getattr(item, "created_at", None) else timezone.now().isoformat(),
            })

            if len(items) >= limit:
                break

        payload = {
            "source": "clearfield_generated_medical_news",
            "created_at": timezone.now().isoformat(),
            "items": items,
        }

        output_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        self.stdout.write(self.style.SUCCESS(f"Exported: {len(items)}"))
        self.stdout.write(f"Path: {output_path}")
        self.stdout.write(f"Filename: {filename}")

        if options["show_content_size"]:
            self.stdout.write(f"Size: {output_path.stat().st_size} bytes")
