import re

from django.core.management.base import BaseCommand, CommandError

from intel.medical_editorial_validation import unsupported_claim_hits
from intel.models import GeneratedMedicalNews


BAD_PATTERNS = [
    r"as\s+an\s+ai",
    r"я\s+не\s+могу",
    r"я\s+не\s+являюсь\s+врачом",
    r"ollama",
    r"```",
    r"^title\s*:",
    r"^заголовок\s*:",
    r"^meta\s*description",
    r"seo[-\s]*title",
    r"ключевые\s+слова\s*:",
    r"контакты\s+пресс-службы",
    r"файлы\s+cookie",
    r"политика\s+конфиденциальности",
    r"\[\s*(?:указать|вставить)[^\]]*\]",
    r"\{\{[^}]+\}\}",
    r"[\u3400-\u4dbf\u4e00-\u9fff]",
    r"please\s+(?:wait|stand\s+by)",
]

REQUIRED_MEDICAL_WORDS = [
    "анализ", "лаборатор", "диагност", "кров", "обследован",
    "профилактик", "пациент", "здоров", "врач", "медицин",
    "диабет", "иммунитет", "грипп", "орви", "диспансеризац",
]

BRAND_WORDS = [
    "дзагуров", "кдл",
]

BRAND_CTA = """

В лаборатории «Дзагуров КДЛ» во Владикавказе можно уточнить перечень подходящих исследований, сроки выполнения анализов и правила подготовки. Результаты лабораторной диагностики помогают врачу оценивать состояние пациента на основе объективных данных, но не заменяют очную консультацию специалиста.

Материал носит информационный характер и не является медицинским назначением. Подбор анализов и интерпретацию результатов следует проводить совместно с врачом.
""".strip()


def normalize(value):
    value = value or ""
    value = str(value).lower().replace("ё", "е")

    # Нормализуем типографские тире и дефисы.
    value = re.sub(r"[‐-‒–—−]", "-", value)
    value = re.sub(r"\s*-\s*", "-", value)

    value = re.sub(r"\s+", " ", value)
    return value.strip()


def model_has_field(model, field_name):
    return any(field.name == field_name for field in model._meta.fields)


def get_first_existing(obj, field_names):
    for field_name in field_names:
        if hasattr(obj, field_name):
            value = getattr(obj, field_name) or ""
            if value:
                return str(value)
    return ""


def get_body_field_name(obj):
    for field_name in ["content", "body", "text", "article", "html"]:
        if hasattr(obj, field_name):
            return field_name
    return None


def has_any(text, words):
    text = normalize(text)
    return any(word in text for word in words)


def has_bad_pattern(text):
    text = normalize(text)

    return any(
        re.search(pattern, text, flags=re.I | re.M)
        for pattern in BAD_PATTERNS
    )


def confirmed_source_text(item):
    brief = getattr(item, "brief", None)

    if not brief:
        return ""

    event = getattr(brief, "event", None)

    return normalize(
        " ".join(
            [
                brief.title or "",
                brief.facts or "",
                event.title if event else "",
                event.summary if event else "",
            ]
        )
    )


def invented_locality_hits(item, generated_text):
    generated_text = normalize(generated_text)

    # Уже добавленный системой фирменный CTA не является
    # географией исходного события.
    normalized_cta = normalize(BRAND_CTA)

    if normalized_cta:
        generated_text = generated_text.replace(
            normalized_cta,
            " ",
        )

    source_text = confirmed_source_text(item)

    locality_groups = {
        "vladikavkaz": (
            "владикавказ",
            "vladikavkaz",
        ),
        "north-ossetia": (
            "северная осетия",
            "северной осетии",
            "северную осетию",
            "рсо-алания",
            "рсо алания",
            "severnaya-osetiya",
            "severnoy-osetii",
            "north-ossetia",
        ),
    }

    hits = []

    for label, markers in locality_groups.items():
        generated_has = any(
            marker in generated_text
            for marker in markers
        )

        source_has = any(
            marker in source_text
            for marker in markers
        )

        if generated_has and not source_has:
            hits.append(label)

    return hits


def append_brand_cta_if_missing(item):
    """
    Автоматическая вставка рекламного CTA временно отключена.

    Медицинская или региональная тематика сама по себе не является
    достаточным основанием для добавления блока лаборатории.
    """

    return False, "brand-cta-disabled"


def parse_ids(raw_value):
    raw_value = str(raw_value or "").strip()

    if not raw_value:
        return []

    result = []

    for token in re.split(r"[\s,;]+", raw_value):
        if not token:
            continue

        try:
            value = int(token)
        except ValueError as exc:
            raise CommandError(
                "Некорректный GeneratedMedicalNews "
                f"ID: {token}"
            ) from exc

        if value <= 0:
            raise CommandError(
                "Некорректный GeneratedMedicalNews "
                f"ID: {token}"
            )

        result.append(value)

    return list(dict.fromkeys(result))


class Command(BaseCommand):
    help = "Automatically approve generated medical news after deterministic editorial checks."

    def add_arguments(self, parser):
        parser.add_argument("--status", default="review")
        parser.add_argument("--limit", type=int, default=10)
        parser.add_argument("--min-chars", type=int, default=1700)
        parser.add_argument(
            "--news-ids",
            default="",
            help=(
                "ID GeneratedMedicalNews через "
                "запятую, пробел или точку с запятой."
            ),
        )
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--show-rejected", action="store_true")

    def handle(self, *args, **options):
        status = options["status"]
        limit = options["limit"]
        min_chars = options["min_chars"]
        dry_run = options["dry_run"]
        show_rejected = options["show_rejected"]
        requested_ids = parse_ids(
            options["news_ids"]
        )

        qs = (
            GeneratedMedicalNews.objects
            .filter(status=status)
            .select_related("brief", "brief__event")
            .order_by("-id")
        )

        if requested_ids:
            qs = qs.filter(
                id__in=requested_ids,
            )

        qs = qs[:limit]

        checked = 0
        approved = 0
        skipped = 0

        for item in qs:
            checked += 1

            title = get_first_existing(
                item,
                ["title", "headline", "name"],
            )
            body = get_first_existing(
                item,
                ["content", "body", "text", "article", "html"],
            )
            slug = get_first_existing(item, ["slug"])
            meta_description = get_first_existing(
                item,
                ["meta_description", "description"],
            )
            source_note = get_first_existing(
                item,
                ["source_note"],
            )

            combined = "\n".join([
                title,
                slug,
                meta_description,
                body,
                source_note,
            ])

            reasons = []

            if len(normalize(title)) < 18:
                reasons.append("short-title")

            normalized_body_length = len(normalize(body))
            source_length = len(confirmed_source_text(item))

            # Короткий официальный анонс не следует искусственно
            # растягивать ради общего порога 1700 символов.
            required_min_chars = (
                900
                if source_length < 1200
                else 1300
                if source_length < 2500
                else min_chars
            )

            if normalized_body_length < required_min_chars:
                reasons.append(
                    "short-body:"
                    f"{normalized_body_length}"
                    f"<{required_min_chars}"
                )

            if has_bad_pattern(combined):
                reasons.append("bad-llm-artifact")

            locality_hits = invented_locality_hits(
                item,
                combined,
            )

            if locality_hits:
                reasons.append(
                    "invented-locality:"
                    + ",".join(locality_hits)
                )

            claim_hits = unsupported_claim_hits(
                generated_text=combined,
                source_text=confirmed_source_text(item),
            )

            if claim_hits:
                reasons.append(
                    "unsupported-claims:"
                    + ",".join(claim_hits)
                )

            if not has_any(combined, REQUIRED_MEDICAL_WORDS):
                reasons.append("no-medical-context")

            if reasons:
                skipped += 1

                if show_rejected:
                    self.stdout.write(
                        self.style.WARNING(
                            f"SKIP #{item.id}: {', '.join(reasons)} | {title[:140]}"
                        )
                    )
                continue

            brand_action = "brand-not-needed"

            if not has_any(combined, BRAND_WORDS):
                if dry_run:
                    brand_action = "brand-cta-disabled"
                else:
                    _, brand_action = append_brand_cta_if_missing(item)

            approved += 1

            if dry_run:
                self.stdout.write(
                    self.style.SUCCESS(
                        f"WOULD APPROVE #{item.id}: {brand_action} | {title[:140]}"
                    )
                )
            else:
                item.status = "published"
                item.save(update_fields=["status"])

                self.stdout.write(
                    self.style.SUCCESS(
                        f"PUBLISHED #{item.id}: {brand_action} | {title[:140]}"
                    )
                )

        self.stdout.write("")
        self.stdout.write(f"Checked: {checked}")
        self.stdout.write(f"Approved: {approved}")
        self.stdout.write(f"Skipped: {skipped}")

        if dry_run:
            self.stdout.write(self.style.WARNING("Dry-run only. Database was not changed."))
