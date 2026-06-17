import re

from django.core.management.base import BaseCommand

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
]

REQUIRED_MEDICAL_WORDS = [
    "анализ", "лаборатор", "диагност", "кров", "обследован",
    "профилактик", "пациент", "здоров", "врач", "медицин",
    "диабет", "иммунитет", "грипп", "орви", "диспансеризац",
]

REQUIRED_LOCAL_WORDS = [
    "владикавказ", "северной осет", "рсо", "алания",
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
    return any(re.search(pattern, text, flags=re.I | re.M) for pattern in BAD_PATTERNS)


def append_brand_cta_if_missing(item):
    title = get_first_existing(item, ["title", "headline", "name"])
    body_field = get_body_field_name(item)

    if not body_field:
        return False, "no-body-field"

    body = getattr(item, body_field) or ""
    combined = f"{title}\n{body}"

    if has_any(combined, BRAND_WORDS):
        return False, "brand-already-exists"

    body = str(body).rstrip()
    new_body = f"{body}\n\n{BRAND_CTA}"

    setattr(item, body_field, new_body)
    item.save(update_fields=[body_field])

    return True, "brand-cta-added"


class Command(BaseCommand):
    help = "Automatically approve generated medical news after quality checks and brand CTA injection."

    def add_arguments(self, parser):
        parser.add_argument("--status", default="review")
        parser.add_argument("--limit", type=int, default=10)
        parser.add_argument("--min-chars", type=int, default=1700)
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--show-rejected", action="store_true")

    def handle(self, *args, **options):
        status = options["status"]
        limit = options["limit"]
        min_chars = options["min_chars"]
        dry_run = options["dry_run"]
        show_rejected = options["show_rejected"]

        qs = GeneratedMedicalNews.objects.filter(status=status).order_by("-id")[:limit]

        checked = 0
        approved = 0
        skipped = 0

        for item in qs:
            checked += 1

            title = get_first_existing(item, ["title", "headline", "name"])
            body = get_first_existing(item, ["content", "body", "text", "article", "html"])
            combined = f"{title}\n{body}"

            reasons = []

            if len(normalize(title)) < 18:
                reasons.append("short-title")

            if len(normalize(body)) < min_chars:
                reasons.append(f"short-body:{len(normalize(body))}")

            if has_bad_pattern(combined):
                reasons.append("bad-llm-artifact")

            if not has_any(combined, REQUIRED_MEDICAL_WORDS):
                reasons.append("no-medical-context")

            if not has_any(combined, REQUIRED_LOCAL_WORDS):
                reasons.append("no-local-context")

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
                    brand_action = "would-add-brand-cta"
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
