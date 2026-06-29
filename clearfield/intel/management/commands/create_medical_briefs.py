import re

from datetime import timedelta

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from intel.models import (
    Article,
    Event,
    MedicalBrief,
    MedicalBriefStatus,
    Topic,
)


# =============================================================================
# TOPICS
# =============================================================================

MEDICAL_TOPICS = [
    Topic.MEDICINE,
    Topic.LABS,
    "medicine",
    "labs",
]


# =============================================================================
# EDITORIAL FILTERS
# =============================================================================

BAD_TITLE_EXACT = {
    "контакты пресс-службы",
    "правила и условия использования контента сайта",
    "условия использования сайта",
    "материалы сми",
    "федеральная повестка",
    "состоявшиеся мероприятия",
    "тексты официальных выступлений",
    "текущая страница",
    "перейти на страницу 2",
    "перейти на страницу 3",
    "на последнюю страницу",
    "новости статистики",
    "календарь медицинских конференций",
    "министерство здравоохранения республики северная осетия-алания информирует:",
    "минздрав рсо-алания информирует",
}


# Жёсткий reject: это почти точно не материал для SEO-новости лаборатории.
BAD_TITLE_PARTS = (
    "контакты пресс-службы",
    "правила и условия",
    "использования контента",
    "перечень медицинских организаций",
    "территориальной программе государственных гарантий",
    "территориальная программа государственных гарантий",
    "программа государственных гарантий",
    "тексты официальных выступлений",
    "федеральная повестка",
    "материалы сми",
    "состоявшиеся мероприятия",
    "перейти на страницу",
    "текущая страница",
    "на последнюю страницу",
    "министерство обороны",
    "военную службу по контракту",
    "службу по контракту",
    "ростелеком",
    "цифрового ликбеза",
    "с глубоким прискорбием сообщает",
    "ушел из жизни",
    "ушёл из жизни",
    "правила и условия использования",
)


# Мягкий штраф: тема может быть медицинской, но обычно слабая для сайта лаборатории.
WEAK_TITLE_PARTS = (
    "вольной борьбе",
    "соревнования",
    "спортсмен",
    "юниоров",
    "спортивного",
    "спортивная",
    "молодых специалистов",
    "наставничеств",
    "выпускники медколледжей",
    "выпускники вузов",
    "назначена",
    "назначен",
    "рабочее совещание",
    "интервью",
    "подписали соглашение",
    "договорились о развитии",
    "форум",
    "пмэф",
)


GOOD_MEDICAL_MARKERS = (
    "диабет",
    "сахарн",
    "грипп",
    "орви",
    "простуд",
    "иммунитет",
    "клещ",
    "диспансеризац",
    "профилактик",
    "донор",
    "переливан",
    "кров",
    "центр здоровья",
    "поликлиник",
    "кардиолог",
    "онколог",
    "реабилитац",
    "медицинск",
    "лаборатор",
    "анализ",
    "здоров",
    "помощ",
    "оборудован",
    "врач",
    "пациент",
    "осмотр",
    "профосмотр",
    "обследован",
)


LAB_MARKERS = (
    "анализ",
    "лаборатор",
    "кров",
    "диагностик",
    "пцр",
    "тест",
    "скрининг",
    "ферритин",
    "гемоглобин",
    "ттг",
    "витамин",
    "глюкоз",
    "холестерин",
)


STRONG_PATIENT_TOPICS = (
    "диабет",
    "сахарн",
    "грипп",
    "орви",
    "простуд",
    "иммунитет",
    "клещ",
    "диспансеризац",
    "профилактик",
    "донор",
    "переливан",
    "центр здоровья",
    "кардиолог",
    "онколог",
    "реабилитац",
    "обследован",
    "профосмотр",
)


# =============================================================================
# TEXT HELPERS
# =============================================================================

def has_real_liver_topic(value):
    """
    True only for explicit liver-related context.

    Important:
    Do not use broad "печен" marker.
    It can match unrelated words like "обеспечение".
    """
    value = (value or "").lower().replace("ё", "е")

    patterns = [
        r"(?<![а-яa-z])печень(?![а-яa-z])",
        r"(?<![а-яa-z])печени(?![а-яa-z])",
        r"печеночн",
        r"гепатит",
        r"желтух",
        r"цирроз",
        r"билирубин",
        r"(?<![а-яa-z])алт(?![а-яa-z])",
        r"(?<![а-яa-z])аст(?![а-яa-z])",
        r"желч",
    ]

    return any(re.search(pattern, value, flags=re.I) for pattern in patterns)


def compact(value: str, limit: int = 1200) -> str:
    value = (value or "").strip()
    value = " ".join(value.split())

    if len(value) <= limit:
        return value

    return value[:limit].rstrip() + "..."


def normalize(value: str) -> str:
    value = compact(value, 5000).lower()
    value = value.replace("ё", "е")
    value = value.strip(" .,:;!?—-«»\"'()[]{}")
    return value


def has_marker(text: str, markers: tuple[str, ...]) -> bool:
    return any(marker in text for marker in markers)


def is_bad_title(title: str) -> bool:
    value = normalize(title)

    if not value:
        return True

    if value in BAD_TITLE_EXACT:
        return True

    if any(part in value for part in BAD_TITLE_PARTS):
        return True

    return False


def weak_title_hits(text: str) -> list[str]:
    return [part for part in WEAK_TITLE_PARTS if part in text]


# =============================================================================
# SCORING
# =============================================================================

def candidate_score(event: Event) -> tuple[int, list[str]]:
    """
    Оценка пригодности события для медицинской SEO-новости.

    Возвращает:
      score
      reasons

    Логика:
    - служебные страницы и юридические документы отсекаются жёстко;
    - спорные темы получают штраф;
    - пациентские темы получают приоритет;
    - лабораторные маркеры добавляют ценность, но не должны превращать мусор в кандидата.
    """

    title = event.title or ""
    summary = event.summary or ""

    text = normalize(f"{title} {summary}")

    score = 0
    reasons: list[str] = []

    if is_bad_title(title):
        score -= 10
        reasons.append("bad-title")

    weak_hits = weak_title_hits(text)
    if weak_hits:
        score -= 3
        reasons.append("weak-topic:" + ",".join(weak_hits[:3]))

    summary_len = len(summary.strip())

    if summary_len >= 250:
        score += 2
        reasons.append("summary>=250")
    elif summary_len >= 120:
        score += 1
        reasons.append("summary>=120")
    else:
        score -= 2
        reasons.append("short-summary")

    good_hits = [marker for marker in GOOD_MEDICAL_MARKERS if marker in text]
    lab_hits = [marker for marker in LAB_MARKERS if marker in text]
    strong_hits = [marker for marker in STRONG_PATIENT_TOPICS if marker in text]

    if good_hits:
        score += min(4, len(good_hits))
        reasons.append("medical:" + ",".join(good_hits[:4]))

    if lab_hits:
        score += min(3, len(lab_hits))
        reasons.append("lab:" + ",".join(lab_hits[:3]))

    if strong_hits:
        score += min(4, len(strong_hits))
        reasons.append("strong:" + ",".join(strong_hits[:4]))
    else:
        score -= 1
        reasons.append("no-strong-patient-topic")

    if event.evidence_level >= 2:
        score += 1
        reasons.append("evidence>=2")

    if "RU" in (event.region or ""):
        score += 1
        reasons.append("region-ru")

    if "владикавказ" in text or "северн" in text or "осет" in text:
        score += 2
        reasons.append("local")

    # Документы и таблицы часто дают длинный текст, но плохо подходят для новости.
    if "|" in summary and summary.count("|") >= 5:
        score -= 5
        reasons.append("table-like")

    if "постановление" in text and "программа государственных гарантий" in text:
        score -= 5
        reasons.append("legal-document")

    if "cookie" in text or "cookies" in text or "файлы cookies" in text:
        score -= 5
        reasons.append("cookie-text")

    return score, reasons


# =============================================================================
# SEO KEYWORD DETECTION
# =============================================================================

def detect_keyword(title: str, summary: str) -> str:
    text = normalize(f"{title} {summary}")

    # Самые точные и безопасные темы — первыми.
    if has_marker(text, ("донор", "донорск", "переливан")) or (
        "сдач" in text and "кров" in text
    ):
        return "анализы крови перед донорством"

    if has_marker(text, ("сахарный диабет", "диабет", "глюкоз", "сахар крови")):
        return "анализы при сахарном диабете во Владикавказе"

    if has_marker(text, ("грипп", "орви", "простуд", "сезон простуд")):
        return "анализы при ОРВИ и гриппе во Владикавказе"

    if has_marker(
        text,
        (
            "центр здоровья",
            "диспансеризац",
            "профилактическ",
            "профосмотр",
            "медицинский осмотр",
            "медицинские осмотры",
        ),
    ):
        return "профилактические анализы и диспансеризация"

    if has_marker(text, ("иммунитет", "укрепления иммунитета")):
        return "анализы для оценки состояния здоровья"

    if has_marker(text, ("клещ", "укус клеща", "клещев")):
        return "лабораторная диагностика после укуса клеща"

    if has_marker(
        text,
        (
            "беремен",
            "материнств",
            "роддом",
            "рождение ребенка",
            "рождение ребёнка",
            "охрана материнства",
        ),
    ):
        return "анализы при беременности во Владикавказе"

    if has_marker(text, ("кардиолог", "сердечно", "аритми", "сердц")):
        return "анализы для оценки сердечно-сосудистого риска"

    if has_marker(text, ("онколог", "онкопациент", "онкотерап")):
        return "лабораторная диагностика и профилактические обследования"

    if has_marker(text, ("витамин d", "витамин д")):
        return "анализ на витамин D во Владикавказе"

    if has_marker(text, ("ферритин",)):
        return "анализ на ферритин во Владикавказе"

    if has_marker(text, ("анем", "гемоглобин")):
        return "общий анализ крови во Владикавказе"

    if has_marker(text, ("ттг", "щитовид")):
        return "анализы щитовидной железы во Владикавказе"

    if has_marker(text, ("холестерин", "липид")):
        return "липидограмма во Владикавказе"

    # Важно: печёночные пробы разрешены только при явной теме печени.
    # Нельзя использовать широкий маркер "печен": он ложно срабатывает на "обеспечение".
    if has_real_liver_topic(text):
        return "печёночные пробы во Владикавказе"

    if has_marker(text, ("аллерг",)):
        return "анализы на аллергию во Владикавказе"

    if has_marker(text, ("коронавирус", "covid", "пцр")):
        return "ПЦР и лабораторная диагностика инфекций"

    if has_marker(text, ("гормон",)):
        return "анализы на гормоны во Владикавказе"

    return "лабораторная диагностика во Владикавказе"


def build_angle(title: str, summary: str) -> str:
    keyword = detect_keyword(title, summary)

    return (
        "Подать материал как спокойную информационную новость для пациентов "
        "медицинской лаборатории. Не делать медицинских назначений и не обещать "
        f"результат. Основной акцент: {keyword}. "
        "Связать событие с ролью лабораторной диагностики, профилактических обследований, "
        "подготовки к анализам и необходимости интерпретации результатов специалистом. "
        "Не добавлять конкретные анализы, если они не указаны в фактах."
    )


# =============================================================================
# FACT COLLECTION
# =============================================================================

def collect_event_facts(event: Event) -> tuple[str, str]:
    facts: list[str] = []
    urls: list[str] = []

    if event.title:
        facts.append(f"Событие: {compact(event.title, 500)}")

    if event.summary:
        facts.append(f"Краткое содержание: {compact(event.summary, 1600)}")

    event_items = (
        event.items
        .select_related("item", "item__source")
        .order_by("-created_at")[:8]
    )

    for event_item in event_items:
        item = event_item.item

        if not item:
            continue

        if item.url and item.url not in urls:
            urls.append(item.url)

        source_name = item.source.name if item.source else "источник"

        if item.title:
            facts.append(f"Материал ({source_name}): {compact(item.title, 500)}")

        if item.summary:
            facts.append(f"Описание ({source_name}): {compact(item.summary, 900)}")

        try:
            article = item.article
        except Article.DoesNotExist:
            article = None

        if article and article.text:
            facts.append(f"Фрагмент статьи ({source_name}): {compact(article.text, 1800)}")

    facts_text = "\n\n".join(facts).strip()
    urls_text = "\n".join(urls).strip()

    return facts_text, urls_text


# =============================================================================
# COMMAND
# =============================================================================

class Command(BaseCommand):
    help = "Создаёт медицинские редакционные задания из качественных событий CLEARFIELD."

    def add_arguments(self, parser):
        parser.add_argument(
            "--hours",
            type=int,
            default=getattr(settings, "CLEARFIELD_BRIEF_HOURS", 72),
            help="За сколько последних часов брать события.",
        )
        parser.add_argument(
            "--min-evidence",
            type=int,
            default=getattr(settings, "CLEARFIELD_MIN_EVIDENCE", 2),
            help="Минимальный уровень доказательности события.",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=20,
            help="Максимум заданий за один запуск.",
        )
        parser.add_argument(
            "--status",
            choices=[
                MedicalBriefStatus.DRAFT,
                MedicalBriefStatus.READY,
            ],
            default=MedicalBriefStatus.READY,
            help="Статус создаваемых заданий.",
        )
        parser.add_argument(
            "--min-score",
            type=int,
            default=5,
            help="Минимальный редакционный score события.",
        )
        parser.add_argument(
            "--min-summary-len",
            type=int,
            default=180,
            help="Минимальная длина Event.summary.",
        )
        parser.add_argument(
            "--show-rejected",
            action="store_true",
            help="Показывать отклонённые события и причины.",
        )
        parser.add_argument(
            "--allow-weak",
            action="store_true",
            help="Разрешить слабые события. Только для ручного теста.",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help="Создавать новое задание даже если у события уже есть MedicalBrief.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Показать, что будет создано, без записи в базу.",
        )

    def handle(self, *args, **options):
        hours = options["hours"]
        min_evidence = options["min_evidence"]
        limit = options["limit"]
        status = options["status"]
        min_score = options["min_score"]
        min_summary_len = options["min_summary_len"]
        show_rejected = options["show_rejected"]
        allow_weak = options["allow_weak"]
        force = options["force"]
        dry_run = options["dry_run"]

        since = timezone.now() - timedelta(hours=hours)

        queryset = (
            Event.objects
            .filter(
                updated_at__gte=since,
                evidence_level__gte=min_evidence,
                topic__in=MEDICAL_TOPICS,
            )
            .exclude(summary="")
            .order_by("-updated_at")
        )

        if not force:
            queryset = queryset.filter(medical_briefs__isnull=True)

        events = list(queryset.distinct())

        if not events:
            self.stdout.write(self.style.WARNING("Нет событий для анализа MedicalBrief."))
            return

        selected: list[tuple[Event, int, list[str]]] = []
        rejected_rows: list[tuple[Event, int, list[str], int]] = []

        for event in events:
            score, reasons = candidate_score(event)
            summary_len = len((event.summary or "").strip())

            is_good = (
                score >= min_score
                and summary_len >= min_summary_len
                and not is_bad_title(event.title or "")
            )

            if allow_weak:
                is_good = score >= 0 and summary_len >= 80

            if is_good:
                selected.append((event, score, reasons))
            else:
                rejected_rows.append((event, score, reasons, summary_len))

        # Приоритет: сначала самые сильные кандидаты, затем более свежие.
        selected.sort(
            key=lambda row: (
                row[1],
                row[0].updated_at or row[0].created_at,
            ),
            reverse=True,
        )

        selected = selected[:limit]

        if show_rejected:
            for event, score, reasons, summary_len in rejected_rows:
                self.stdout.write(
                    self.style.WARNING(
                        f"REJECT Event #{event.pk} score={score} "
                        f"summary_len={summary_len} "
                        f"reasons={','.join(reasons)} | {(event.title or '')[:140]}"
                    )
                )

        if not selected:
            self.stdout.write(
                self.style.WARNING(
                    f"Нет качественных событий для MedicalBrief. "
                    f"Проверено: {len(events)}, отклонено: {len(rejected_rows)}."
                )
            )
            return

        created = 0

        for event, score, reasons in selected:
            facts, source_urls = collect_event_facts(event)

            title = event.title or f"Медицинское событие #{event.pk}"
            summary = event.summary or ""

            target_keyword = detect_keyword(title, summary)
            angle = build_angle(title, summary)

            safety_notes = (
                "Не ставить диагнозы. Не назначать лечение. Не давать индивидуальных "
                "медицинских рекомендаций. Не обещать результат. Не использовать факты, "
                "которых нет во входных данных. Не добавлять конкретные анализы, если "
                "они не указаны во входных данных. Обязательно добавить дисклеймер."
            )

            self.stdout.write("")
            self.stdout.write(
                self.style.NOTICE(
                    f"Event #{event.pk}: score={score} | {compact(title, 160)}"
                )
            )
            self.stdout.write(f"Reasons: {', '.join(reasons)}")
            self.stdout.write(f"SEO: {target_keyword}")

            if dry_run:
                continue

            with transaction.atomic():
                MedicalBrief.objects.create(
                    event=event,
                    title=title,
                    angle=angle,
                    target_keyword=target_keyword,
                    secondary_keywords="",
                    facts=facts,
                    source_urls=source_urls,
                    audience=getattr(
                        settings,
                        "MEDICAL_NEWS_DEFAULT_AUDIENCE",
                        "пациенты медицинской лаборатории",
                    ),
                    region_text=getattr(
                        settings,
                        "MEDICAL_NEWS_DEFAULT_REGION_TEXT",
                        "Владикавказ и Северная Осетия",
                    ),
                    safety_notes=safety_notes,
                    disclaimer_required=True,
                    status=status,
                )

                created += 1

        if dry_run:
            self.stdout.write(
                self.style.SUCCESS(
                    f"Dry-run завершён. Кандидатов: {len(selected)}, "
                    f"отклонено: {len(rejected_rows)}"
                )
            )
        else:
            self.stdout.write(self.style.SUCCESS(f"Создано MedicalBrief: {created}"))
