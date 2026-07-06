import re
from difflib import SequenceMatcher

from django.core.management.base import BaseCommand

from intel.models import MedicalBrief, GeneratedMedicalNews


BAD_TITLE_PATTERNS = [
    r"контакты\s+пресс-службы",
    r"правила\s+и\s+условия",
    r"использования\s+контента",
    r"политика\s+конфиденциальности",
    r"персональн\w+\s+данн",
    r"файлы\s+cookies?",
    r"cookie",
    r"новости\s+статистики",
    r"официальн\w+\s+выступлен",
    r"материалы\s+сми",
    r"федеральная\s+повестка",
    r"перечень\s+медицинских\s+организаций",
    r"территориальн\w+\s+программ",
    r"государственных\s+гарантий",
    r"минздрав\s+.*\s+информирует$",
    r"энергодар",
    r"запорожск",
]

BAD_BODY_PATTERNS = [
    r"мы\s+используем\s+файлы\s+cookies?",
    r"продолжая\s+пользоваться\s+сайтом",
    r"соглашаетесь\s+с\s+условиями\s+использования",
    r"политикой\s+конфиденциальности",
    r"условиями\s+использования\s+персональных\s+данных",
    r"копирование\s+материалов",
    r"при\s+использовании\s+материалов\s+ссылка",
]

MEDICAL_WORDS = [
    "анализ", "лаборатор", "диагност", "кров", "моч", "гормон",
    "витамин", "ферритин", "ттг", "глюкоз", "сахар", "диабет",
    "профилактик", "обследован", "диспансеризац", "пациент",
    "здоров", "врач", "медицин", "поликлиник", "больниц",
    "донор", "беремен", "сердц", "печень", "печеноч", "печёноч", "щитовид",
    "онколог", "реабилитац",
]

LOCAL_WORDS = [
    "северной осет", "рсо", "алания", "владикавказ",
    "республикан", "осетин",
]

LAB_SEO_WORDS = [
    "лаборатор", "анализ", "диагност", "кров", "глюкоз",
    "ферритин", "ттг", "витамин", "печень", "печеноч", "печёноч", "сахар",
    "диспансеризац", "профилактик", "обследован",
]

STRONG_PATIENT_TOPICS = [
    "диабет", "сахар", "диспансеризац", "профилактик",
    "онколог", "реабилитац", "анем", "ферритин",
    "ттг", "щитовид", "сердц", "беремен", "витамин",
]


EDITORIAL_MEDICAL_TOPICS = [
    "врач", "хирург", "кардиолог", "нейрохирург",
    "студент", "университет", "медицинское образование",
    "ординатур", "стажиров", "наставнич", "практик",
    "оборудован", "аппарат", "медицинская техника",
    "реанимац", "скорая помощь", "операц",
    "реабилитац", "физиотерап", "клиническ",
    "здравоохран", "больниц", "поликлиник",
]


def normalize_text(value: str) -> str:
    value = value or ""
    value = str(value).lower().replace("ё", "е")
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def has_any_pattern(text: str, patterns: list[str]) -> bool:
    text = normalize_text(text)
    return any(re.search(pattern, text, flags=re.I) for pattern in patterns)


def has_any_word(text: str, words: list[str]) -> bool:
    text = normalize_text(text)
    return any(word in text for word in words)


def similarity(a: str, b: str) -> float:
    a = normalize_text(a)
    b = normalize_text(b)

    if not a or not b:
        return 0.0

    return SequenceMatcher(None, a, b).ratio()


def model_has_field(model, field_name: str) -> bool:
    return any(field.name == field_name for field in model._meta.fields)


def get_text_field_names(model):
    result = []

    for field in model._meta.fields:
        internal_type = field.get_internal_type()

        if internal_type in ("CharField", "TextField", "SlugField", "URLField"):
            if field.name not in ("status", "slug"):
                result.append(field.name)

    return result


def get_obj_text(obj, fields: list[str]) -> str:
    parts = []

    for field in fields:
        if hasattr(obj, field):
            value = getattr(obj, field) or ""
            if value:
                parts.append(str(value))

    return "\n".join(parts)


def get_title(obj) -> str:
    preferred = [
        "title",
        "headline",
        "event_title",
        "source_title",
        "name",
    ]

    for field in preferred:
        if hasattr(obj, field):
            value = getattr(obj, field) or ""
            if value:
                return str(value)

    text_fields = get_text_field_names(obj.__class__)
    text = get_obj_text(obj, text_fields)
    return normalize_text(text)[:180]


def get_all_text(obj) -> str:
    text_fields = get_text_field_names(obj.__class__)
    return get_obj_text(obj, text_fields)


def table_like_score(text: str) -> int:
    raw = text or ""
    score = 0

    if raw.count("|") >= 5:
        score += 2

    if len(re.findall(r"\b\d+\.\d+\b", raw)) >= 8:
        score += 2

    if len(re.findall(r"\b\d{6,}\b", raw)) >= 8:
        score += 1

    if len(re.findall(r"\bИНН\b|\bОГРН\b|\bКПП\b", raw, flags=re.I)) >= 2:
        score += 2

    return score


class Command(BaseCommand):
    help = "Audit MedicalBrief quality and reject weak/noisy briefs before news generation."

    def add_arguments(self, parser):
        parser.add_argument(
            "--status",
            default="ready",
            help="MedicalBrief status to audit. Default: ready",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=0,
            help="Limit briefs count. 0 = no limit.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show decisions without updating statuses.",
        )
        parser.add_argument(
            "--show-ok",
            action="store_true",
            help="Also print accepted briefs.",
        )
        parser.add_argument(
            "--max-title-similarity",
            type=float,
            default=0.86,
            help="Reject if title is too similar to generated news title. Default: 0.86",
        )

    def handle(self, *args, **options):
        status = options["status"]
        limit = options["limit"]
        dry_run = options["dry_run"]
        show_ok = options["show_ok"]
        max_title_similarity = options["max_title_similarity"]

        text_fields = get_text_field_names(MedicalBrief)
        self.stdout.write(f"MedicalBrief text fields: {', '.join(text_fields) or 'none'}")

        qs = MedicalBrief.objects.filter(status=status).order_by("id")
        if limit > 0:
            qs = qs[:limit]

        generated_titles = []

        for item in GeneratedMedicalNews.objects.exclude(status="error").order_by("-id")[:300]:
            old_title = get_title(item)
            if old_title:
                generated_titles.append(old_title)

        checked = 0
        rejected = 0
        accepted = 0

        for brief in qs:
            checked += 1

            title = get_title(brief)
            combined = get_all_text(brief)
            normalized_combined = normalize_text(combined)

            reasons = []

            if not title or len(normalize_text(title)) < 12:
                reasons.append("short-title")

            if not normalized_combined or len(normalized_combined) < 120:
                reasons.append("short-brief-text")

            if has_any_pattern(title, BAD_TITLE_PATTERNS):
                reasons.append("bad-title")

            if has_any_pattern(combined, BAD_BODY_PATTERNS):
                reasons.append("bad-body")

            if table_like_score(combined) >= 3:
                reasons.append("table-like")

            source_text = get_obj_text(
                brief,
                ["title", "facts"],
            )

            has_medical = has_any_word(source_text, MEDICAL_WORDS)
            has_local = has_any_word(source_text, LOCAL_WORDS)
            has_lab_seo = has_any_word(source_text, LAB_SEO_WORDS)
            has_strong_patient_topic = has_any_word(
                source_text,
                STRONG_PATIENT_TOPICS,
            )
            has_editorial_medical_topic = has_any_word(
                source_text,
                EDITORIAL_MEDICAL_TOPICS,
            )

            if not has_medical:
                reasons.append("no-medical-context")

            if (
                has_medical
                and not has_local
                and not (
                    has_lab_seo
                    or has_strong_patient_topic
                    or has_editorial_medical_topic
                )
            ):
                reasons.append("weak-medical-relevance")

            for old_title in generated_titles:
                ratio = similarity(title, old_title)
                if ratio >= max_title_similarity:
                    reasons.append(f"duplicate-title:{ratio:.2f}")
                    break

            if reasons:
                rejected += 1
                self.stdout.write(
                    self.style.WARNING(
                        f"REJECT #{brief.id}: {', '.join(reasons)} | {title[:150]}"
                    )
                )

                if not dry_run:
                    update_kwargs = {"status": "rejected"}

                    for reason_field in ("reject_reason", "reason", "notes", "comment"):
                        if model_has_field(MedicalBrief, reason_field):
                            update_kwargs[reason_field] = ", ".join(reasons)
                            break

                    MedicalBrief.objects.filter(pk=brief.pk).update(**update_kwargs)
            else:
                accepted += 1

                if show_ok:
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"OK #{brief.id}: {title[:150]}"
                        )
                    )

        self.stdout.write("")
        self.stdout.write(f"Checked: {checked}")
        self.stdout.write(f"Accepted: {accepted}")
        self.stdout.write(f"Rejected: {rejected}")

        if dry_run:
            self.stdout.write(self.style.WARNING("Dry-run only. Database was not changed."))
