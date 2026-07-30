import json
import re
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


SEMANTIC_ANALYSIS_URL_PREFIX = (
    "https://kdl-dzagurov.ru/analysis/"
)

SEMANTIC_ANALYSIS_URL_RE = re.compile(
    r"^https://kdl-dzagurov\.ru/"
    r"analysis/([0-9A-Za-z._-]+)/$"
)

SEMANTIC_MARKDOWN_LINK_RE = re.compile(
    r"\[([^\]\n]+)\]\("
    r"(https://kdl-dzagurov\.ru/"
    r"analysis/[0-9A-Za-z._-]+/)"
    r"\)"
)

SEMANTIC_SECTION_HEADING = (
    "## Связанная информация"
)


def clean_markdown_label(value):
    """
    Готовит безопасный однострочный анкор для простого
    Markdown-конвертера на стороне Дзагурова.
    """

    label = " ".join(
        str(value or "").split()
    )

    label = (
        label
        .replace("[", "(")
        .replace("]", ")")
    )

    return label[:500].strip()


def semantic_landing_from_brief(brief):
    """
    Возвращает проверенную SEO-привязку либо None.

    Привязка считается допустимой только тогда, когда
    matcher записал полный согласованный набор полей.
    """

    if brief is None:
        return None

    panel_id = getattr(
        brief,
        "semantic_panel_id",
        None,
    )

    if panel_id in (None, ""):
        return None

    try:
        panel_id = int(panel_id)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "semantic_panel_id должен быть числом."
        ) from exc

    if panel_id <= 0:
        raise ValueError(
            "semantic_panel_id должен быть положительным."
        )

    code = str(
        getattr(
            brief,
            "semantic_panel_code",
            "",
        )
        or ""
    ).strip()

    url = str(
        getattr(
            brief,
            "semantic_panel_url",
            "",
        )
        or ""
    ).strip()

    anchor = clean_markdown_label(
        getattr(
            brief,
            "semantic_anchor",
            "",
        )
        or getattr(
            brief,
            "semantic_panel_title",
            "",
        )
        or code
    )

    try:
        score = int(
            getattr(
                brief,
                "semantic_score",
                0,
            )
            or 0
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "semantic_score должен быть числом."
        ) from exc

    feed_sha256 = str(
        getattr(
            brief,
            "semantic_feed_sha256",
            "",
        )
        or ""
    ).strip().lower()

    assigned_at = getattr(
        brief,
        "semantic_assigned_at",
        None,
    )

    if not code:
        raise ValueError(
            "Для semantic-привязки не задан код анализа."
        )

    if not re.fullmatch(
        r"[0-9A-Za-z._-]+",
        code,
    ):
        raise ValueError(
            "Код semantic-панели содержит "
            "недопустимые символы."
        )

    expected_url = (
        f"{SEMANTIC_ANALYSIS_URL_PREFIX}"
        f"{code}/"
    )

    if url != expected_url:
        raise ValueError(
            "URL semantic-панели не соответствует "
            f"её коду: expected={expected_url}, "
            f"actual={url}"
        )

    match = SEMANTIC_ANALYSIS_URL_RE.fullmatch(
        url
    )

    if (
        not match
        or match.group(1) != code
    ):
        raise ValueError(
            "Неканонический URL semantic-панели."
        )

    if not anchor:
        raise ValueError(
            "Для semantic-привязки не задан анкор."
        )

    if score <= 0:
        raise ValueError(
            "semantic_score должен быть больше нуля."
        )

    if not assigned_at:
        raise ValueError(
            "Для semantic-привязки не задано "
            "время назначения."
        )

    if not re.fullmatch(
        r"[0-9a-f]{64}",
        feed_sha256,
    ):
        raise ValueError(
            "Некорректный semantic_feed_sha256."
        )

    return {
        "panel_id": panel_id,
        "code": code,
        "url": url,
        "anchor": anchor,
        "score": score,
        "feed_sha256": feed_sha256,
    }


def append_semantic_landing_link(
    content,
    brief,
):
    """
    Добавляет ровно одну нейтральную внутреннюю ссылку.

    Ссылка не является частью текста LLM и не содержит
    медицинских назначений, рекомендаций или обещаний.
    """

    content = str(content or "").rstrip()

    landing = semantic_landing_from_brief(
        brief
    )

    has_existing_analysis_url = (
        SEMANTIC_ANALYSIS_URL_PREFIX
        in content
    )

    if landing is None:
        if has_existing_analysis_url:
            raise ValueError(
                "В body обнаружена ссылка на анализ, "
                "но у brief нет semantic-привязки."
            )

        return content

    if has_existing_analysis_url:
        raise ValueError(
            "В исходном body уже присутствует ссылка "
            "на анализ КДЛ «Дзагуров»."
        )

    block = (
        f"{SEMANTIC_SECTION_HEADING}\n\n"
        "Карточка анализа в каталоге "
        "КДЛ «Дзагуров»: "
        f"[{landing['anchor']}]"
        f"({landing['url']})."
    )

    return (
        f"{content}\n\n{block}\n"
    )


def validate_semantic_landing_content(
    content,
    brief,
):
    """
    Проверяет результат после добавления semantic-блока
    и после добавления внешних источников.
    """

    content = str(content or "")

    landing = semantic_landing_from_brief(
        brief
    )

    url_occurrences = content.count(
        SEMANTIC_ANALYSIS_URL_PREFIX
    )

    matches = list(
        SEMANTIC_MARKDOWN_LINK_RE.finditer(
            content
        )
    )

    if landing is None:
        if url_occurrences or matches:
            raise ValueError(
                "Экспорт содержит внутреннюю ссылку "
                "без semantic-привязки."
            )

        return None

    if url_occurrences != 1:
        raise ValueError(
            "В экспортируемом тексте должна быть "
            "ровно одна ссылка на анализ; "
            f"найдено: {url_occurrences}."
        )

    if len(matches) != 1:
        raise ValueError(
            "Не найдена единственная корректная "
            "Markdown-ссылка на анализ."
        )

    match = matches[0]
    actual_anchor = clean_markdown_label(
        match.group(1)
    )
    actual_url = match.group(2)

    if actual_anchor != landing["anchor"]:
        raise ValueError(
            "Анкор ссылки не совпадает с "
            "semantic-привязкой."
        )

    if actual_url != landing["url"]:
        raise ValueError(
            "URL ссылки не совпадает с "
            "semantic-привязкой."
        )

    if (
        content.count(
            SEMANTIC_SECTION_HEADING
        )
        != 1
    ):
        raise ValueError(
            "Semantic-раздел должен встречаться "
            "ровно один раз."
        )

    return landing


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

            try:
                content = append_semantic_landing_link(
                    content,
                    brief,
                )

                content = append_source_links(
                    content,
                    get_source_urls(brief),
                )

                semantic_landing = (
                    validate_semantic_landing_content(
                        content,
                        brief,
                    )
                )
            except ValueError as exc:
                self.stdout.write(
                    self.style.WARNING(
                        f"SKIP #{item.id}: "
                        f"semantic landing validation "
                        f"failed: {exc}"
                    )
                )
                continue

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
                "content_type": (
                    "evergreen_article"
                    if brief and brief.event_id is None
                    else "news"
                ),
                "expert_review_required": bool(
                    brief and brief.event_id is None
                ),
                "semantic_panel_id": (
                    semantic_landing["panel_id"]
                    if semantic_landing
                    else None
                ),
                "semantic_panel_code": (
                    semantic_landing["code"]
                    if semantic_landing
                    else ""
                ),
                "semantic_panel_url": (
                    semantic_landing["url"]
                    if semantic_landing
                    else ""
                ),
                "semantic_anchor": (
                    semantic_landing["anchor"]
                    if semantic_landing
                    else ""
                ),
                "semantic_score": (
                    semantic_landing["score"]
                    if semantic_landing
                    else 0
                ),
                "semantic_feed_sha256": (
                    semantic_landing["feed_sha256"]
                    if semantic_landing
                    else ""
                ),
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
