import re
from datetime import timedelta
from urllib.parse import urlsplit

from django.core.management.base import (
    BaseCommand,
    CommandError,
)
from django.db import transaction
from django.utils import timezone

from intel.models import (
    AutomotiveBrief,
    AutomotiveBriefStatus,
    Event,
    SourceClass,
    Topic,
)


NOISE_TERMS = (
    "все новости",
    "главная страница",
    "архив новостей",
    "контактная информация",
    "подписаться",
    "все мероприятия",
    "предстоящие мероприятия",
    "прошедшие мероприятия",
)


HARD_REJECT_TITLE_TERMS = (
    "вопрос дня",
    "объявление дня",
    "фоторепортаж",
    "фестиваль",
    "автофестиваль",
    "пикник",
    "автошоу",
    "развлекательное мероприятие",
)


HARD_REJECT_CONTEXT_TERMS = (
    "задержали мужчину",
    "задержан мужчина",
    "перепродаж",
    "административной ответственности",
    "перевозку опасных грузов",
)


AUTOMOTIVE_CONTEXT_TERMS = (
    "автомоб",
    "авторын",
    "автозавод",
    "автопроизвод",
    "машин",
    "кроссовер",
    "седан",
    "внедорожник",
    "пикап",
    "грузовик",
    "тягач",
    "автобус",
    "мотоцикл",
    "электромоб",
    "гибрид",
    "двигател",
    "мотор",
    "трансмисс",
    "акп",
    "осаго",
    "такси",
    "автоваз",
    "ремонт",
    "обслуживан",
    "запчаст",
    "топлив",
    "роботакси",
)


SERVICE_INTENT_TERMS = (
    "ремонт",
    "обслуживан",
    "диагност",
    "неисправ",
    "код ошиб",
    "ошибк автомобил",
    "автосервис",
    "техническое обслуживан",
    "стоимость ремонта",
    "средний чек",
    "замена",
    "износ",
    "поломк",
    "стук",
    "люфт",
    "вибрац",
    "перегрев",
    "утечк",
    "техосмотр",
    "сервисная кампания",
    "отзывная кампания",
)


SERVICE_PROFILE_NAMES = {
    "maintenance",
    "suspension",
    "auto_electrics",
    "brakes",
    "engine",
    "tires",
    "diagnostics",
}


PROFILES = (
    {
        "name": "legislation",
        "terms": (
            "осаго",
            "страхов",
            "закон",
            "прав потребител",
            "правила ремонта",
            "правил ремонта",
            "данные для ремонта",
            "обяжут",
            "обязан предостав",
            "техрегламент",
            "технический регламент",
        ),
        "target_keyword": (
            "правила ремонта автомобилей "
            "и права автовладельцев"
        ),
        "secondary_keywords": (
            "правила ремонта автомобилей",
            "права владельца автомобиля",
            "данные для ремонта автомобиля",
            "стоимость запчастей по ОСАГО",
            "ремонт автомобиля во Владикавказе",
        ),
        "angle": (
            "Объяснить автовладельцу, как изменения "
            "правил, требований или страховых расчётов "
            "могут повлиять на ремонт автомобиля, "
            "доступность технической информации и "
            "стоимость восстановления."
        ),
    },
    {
        "name": "maintenance",
        "terms": (
            "ремонт",
            "обслуживан",
            "автосервис",
            "станция технического обслуживан",
            "средний чек",
            "стоимость ремонта",
            "стоимость обслуживан",
            "запчаст",
            "сервисная кампания",
            "отзывная кампания",
            "техосмотр",
        ),
        "target_keyword": (
            "ремонт и обслуживание автомобилей "
            "во Владикавказе"
        ),
        "secondary_keywords": (
            "стоимость ремонта автомобиля",
            "техническое обслуживание автомобиля",
            "автосервис во Владикавказе",
            "диагностика перед ремонтом",
            "обслуживание автомобиля",
        ),
        "angle": (
            "Показать практическое значение события "
            "для владельца автомобиля: из чего складывается "
            "стоимость обслуживания, когда требуется "
            "диагностика и почему объём ремонта определяют "
            "только после проверки автомобиля."
        ),
    },
    {
        "name": "suspension",
        "terms": (
            "подвес",
            "ходов",
            "амортиз",
            "шаров",
            "сайлентблок",
            "рулев",
            "ступич",
            "вибрац",
            "стук",
            "люфт",
        ),
        "target_keyword": (
            "диагностика и ремонт ходовой "
            "во Владикавказе"
        ),
        "secondary_keywords": (
            "диагностика подвески",
            "ремонт ходовой части",
            "стук в подвеске",
            "проверка рулевого управления",
            "автосервис во Владикавказе",
        ),
        "angle": (
            "Объяснить владельцу автомобиля, "
            "какие симптомы требуют проверки ходовой, "
            "что диагностирует мастер и почему нельзя "
            "делать вывод о замене деталей без осмотра."
        ),
    },
    {
        "name": "auto_electrics",
        "terms": (
            "автоэлект",
            "электрик",
            "проводк",
            "генератор",
            "стартер",
            "аккумулятор",
            "блок управления",
            "электронн",
            "напряжен",
        ),
        "target_keyword": (
            "диагностика автоэлектрики "
            "во Владикавказе"
        ),
        "secondary_keywords": (
            "автоэлектрик во Владикавказе",
            "компьютерная диагностика автомобиля",
            "диагностика электронных систем",
            "проверка генератора и стартера",
            "поиск неисправности проводки",
        ),
        "angle": (
            "Показать, как электрические неисправности "
            "проявляются на автомобиле и почему поиск "
            "причины должен начинаться с измерений "
            "и компьютерной диагностики."
        ),
    },
    {
        "name": "brakes",
        "terms": (
            "тормоз",
            "колодк",
            "тормозной диск",
            "abs",
            "суппорт",
            "тормозная жидкость",
        ),
        "target_keyword": (
            "диагностика тормозной системы "
            "во Владикавказе"
        ),
        "secondary_keywords": (
            "проверка тормозов",
            "замена тормозных колодок",
            "диагностика ABS",
            "износ тормозных дисков",
            "ремонт тормозной системы",
        ),
        "angle": (
            "Объяснить признаки проблем с тормозной "
            "системой, необходимость своевременного "
            "осмотра и недопустимость дистанционного "
            "определения исправности тормозов."
        ),
    },
    {
        "name": "engine",
        "terms": (
            "двигател",
            "мотор",
            "грм",
            "турбин",
            "перегрев",
            "охлажден",
            "расход масла",
            "масля",
            "зажиган",
            "топливная систем",
        ),
        "target_keyword": (
            "диагностика двигателя "
            "во Владикавказе"
        ),
        "secondary_keywords": (
            "компьютерная диагностика двигателя",
            "поиск причины неисправности двигателя",
            "проверка системы охлаждения",
            "диагностика системы зажигания",
            "ремонт автомобиля во Владикавказе",
        ),
        "angle": (
            "Связать событие с практическими признаками "
            "неисправности двигателя и подчеркнуть, "
            "что решение о ремонте принимается только "
            "после диагностики."
        ),
    },
    {
        "name": "tires",
        "terms": (
            "шин",
            "покрыш",
            "колес",
            "давление в шинах",
            "протектор",
            "сезонные шины",
        ),
        "target_keyword": (
            "проверка шин и колёс "
            "во Владикавказе"
        ),
        "secondary_keywords": (
            "проверка состояния шин",
            "давление в шинах",
            "износ протектора",
            "вибрация рулевого колеса",
            "обслуживание автомобиля",
        ),
        "angle": (
            "Объяснить, как состояние шин и колёс "
            "влияет на безопасность, управляемость "
            "и диагностику вибраций."
        ),
    },
    {
        "name": "diagnostics",
        "terms": (
            "диагност",
            "неисправ",
            "код ошиб",
            "датчик",
            "obd",
            "check engine",
            "чек энджин",
            "компьютерная проверка",
        ),
        "target_keyword": (
            "компьютерная диагностика автомобиля "
            "во Владикавказе"
        ),
        "secondary_keywords": (
            "диагностика автомобиля",
            "поиск неисправности автомобиля",
            "расшифровка ошибок автомобиля",
            "проверка электронных систем",
            "автосервис во Владикавказе",
        ),
        "angle": (
            "Объяснить событие через практическую пользу "
            "диагностики: какие симптомы замечает водитель, "
            "что проверяет специалист и почему код ошибки "
            "сам по себе не является диагнозом."
        ),
    },
    {
        "name": "automotive_market",
        "terms": (
            "рынок",
            "авторынок",
            "продаж",
            "реализовано",
            "импорт",
            "доля",
            "итоги",
            "сократ",
            "сниз",
            "вырос",
            "рост",
            "подорож",
            "стоимость",
            "цена",
            "цены",
            "дефицит",
            "конкурент",
            "перспектив",
            "предзаказ",
            "дилер",
            "локализ",
        ),
        "target_keyword": (
            "автомобильный рынок России"
        ),
        "secondary_keywords": (
            "новости автомобильного рынка",
            "продажи автомобилей в России",
            "цены на автомобили",
            "новые автомобили в России",
            "автомобильные новости",
        ),
        "angle": (
            "Раскрыть событие как новость автомобильного "
            "рынка: что именно изменилось, какие цифры "
            "или планы подтверждены источником и почему "
            "эта информация может быть интересна "
            "автовладельцам и участникам рынка."
        ),
    },
    {
        "name": "general_automotive_news",
        "terms": (
            "представил",
            "представлена",
            "представлен",
            "новый",
            "новая",
            "новое",
            "модель",
            "кроссовер",
            "седан",
            "внедорожник",
            "пикап",
            "грузовик",
            "тягач",
            "электромоб",
            "гибрид",
            "характеристик",
            "оснащен",
            "оснащение",
            "комплектац",
            "производство",
            "выпуск",
            "локализ",
            "стартуют продажи",
            "начало продаж",
            "доступен",
            "роботакси",
            "назначен",
            "гендиректор",
            "автозавод",
        ),
        "target_keyword": (
            "автомобильные новости России"
        ),
        "secondary_keywords": (
            "новости автомобилей",
            "новые модели автомобилей",
            "автомобильные технологии",
            "производство автомобилей",
            "новинки автомобильного рынка",
        ),
        "angle": (
            "Подготовить самостоятельную автомобильную "
            "новость, сохранив исходное событие главной "
            "темой. Передать только подтверждённые "
            "характеристики, цены, сроки и заявления, "
            "не превращая материал в рекламу или "
            "искусственный текст об автосервисе."
        ),
    },

)


def normalize(value):
    text = str(value or "").lower()
    text = text.replace("ё", "е")
    text = re.sub(
        r"[‐-‒–—−]",
        "-",
        text,
    )
    text = re.sub(
        r"\s+",
        " ",
        text,
    )
    return text.strip()


def term_matches(
    text,
    term,
):
    normalized_text = normalize(text)
    normalized_term = normalize(term)

    if not normalized_term:
        return False

    # Для одиночных русских/латинских основ используем
    # начало слова, а не произвольное вхождение.
    #
    # Поэтому "шин" совпадает с "шины", но не совпадает
    # со словом "машине".
    if re.fullmatch(
        r"[0-9a-zа-я]+",
        normalized_term,
    ):
        pattern = (
            r"(?<![0-9a-zа-я])"
            + re.escape(normalized_term)
            + r"[0-9a-zа-я]*"
        )

        return (
            re.search(
                pattern,
                normalized_text,
            )
            is not None
        )

    return normalized_term in normalized_text


def matching_terms(
    text,
    terms,
):
    return [
        term
        for term in terms
        if term_matches(
            text,
            term,
        )
    ]


def compact(value, limit=1000):
    text = re.sub(
        r"\s+",
        " ",
        str(value or ""),
    ).strip()

    if len(text) <= limit:
        return text

    return text[:limit].rstrip() + "…"


def unique_values(values):
    result = []
    seen = set()

    for value in values:
        clean = str(value or "").strip()

        if not clean:
            continue

        key = normalize(clean)

        if key in seen:
            continue

        seen.add(key)
        result.append(clean)

    return result


def valid_http_url(value):
    try:
        parsed = urlsplit(
            str(value or "").strip()
        )
    except ValueError:
        return False

    return (
        parsed.scheme in {
            "http",
            "https",
        }
        and bool(parsed.netloc)
    )


def parse_ids(raw):
    if not raw:
        return []

    result = []

    for token in re.split(
        r"[\s,;]+",
        raw.strip(),
    ):
        if not token:
            continue

        try:
            value = int(token)
        except ValueError as exc:
            raise CommandError(
                f"Invalid Event ID: {token}"
            ) from exc

        if value <= 0:
            raise CommandError(
                f"Invalid Event ID: {token}"
            )

        result.append(value)

    return sorted(set(result))


def choose_profile(text):
    best_profile = None
    best_hits = 0

    service_intent = bool(
        matching_terms(
            text,
            SERVICE_INTENT_TERMS,
        )
    )

    for profile in PROFILES:
        if (
            profile["name"]
            in SERVICE_PROFILE_NAMES
            and not service_intent
        ):
            continue

        hits = len(
            matching_terms(
                text,
                profile["terms"],
            )
        )

        if hits > best_hits:
            best_profile = profile
            best_hits = hits

    return best_profile, best_hits


def collect_event_material(event):
    event_items = list(
        event.items
        .select_related(
            "item",
            "item__source",
        )
        .order_by(
            "-item__published_at",
            "-created_at",
            "-id",
        )[:10]
    )

    raw_items = [
        relation.item
        for relation in event_items
        if relation.item_id
    ]

    urls = unique_values(
        raw.url
        for raw in raw_items
        if valid_http_url(raw.url)
    )

    fact_candidates = []

    if event.summary:
        fact_candidates.append(
            compact(
                event.summary,
                1200,
            )
        )

    for raw in raw_items:
        source_name = compact(
            getattr(
                raw.source,
                "name",
                "",
            ),
            180,
        )

        title = compact(
            raw.title,
            500,
        )

        summary = compact(
            raw.summary,
            900,
        )

        if title:
            if source_name:
                fact_candidates.append(
                    f"{source_name}: {title}"
                )
            else:
                fact_candidates.append(title)

        if summary and normalize(summary) != normalize(title):
            fact_candidates.append(summary)

    facts = unique_values(
        fact_candidates
    )[:10]

    source_classes = {
        raw.source.source_class
        for raw in raw_items
        if getattr(raw, "source_id", None)
    }

    return {
        "raw_items": raw_items,
        "urls": urls,
        "facts": facts,
        "source_classes": source_classes,
    }


def score_event(
    event,
    material,
    profile_hits,
):
    score = 0
    reasons = []

    title = compact(
        event.title,
        1000,
    )

    summary = compact(
        event.summary,
        3000,
    )

    normalized_title = normalize(title)

    if not title:
        reasons.append("no-title")
    elif len(title) >= 35:
        score += 2
    elif len(title) >= 15:
        score += 1
    else:
        reasons.append("short-title")

    if len(summary) >= 180:
        score += 2
    elif len(summary) >= 60:
        score += 1
    else:
        reasons.append("short-summary")

    evidence = int(
        event.evidence_level or 0
    )

    score += min(
        max(evidence, 0),
        3,
    ) * 2

    url_count = len(
        material["urls"]
    )

    if url_count >= 2:
        score += 2
    elif url_count == 1:
        score += 1
    else:
        reasons.append("no-source-url")

    trusted_classes = {
        SourceClass.OFFICIAL,
        SourceClass.STATS,
        SourceClass.INDUSTRY,
        SourceClass.AGENCY,
    }

    if (
        material["source_classes"]
        & trusted_classes
    ):
        score += 1

    score += min(
        profile_hits,
        3,
    )

    if profile_hits == 0:
        reasons.append(
            "weak-automotive-context"
        )

    if any(
        noise in normalized_title
        for noise in NOISE_TERMS
    ):
        score -= 5
        reasons.append("navigation-noise")

    if not material["facts"]:
        score -= 3
        reasons.append("no-facts")

    return score, reasons


def is_service_profile(
    profile,
):
    return (
        profile
        and profile["name"]
        in SERVICE_PROFILE_NAMES
    )


def audience_for_profile(
    profile,
):
    if is_service_profile(profile):
        return (
            "автовладельцы и клиенты "
            "автосервиса"
        )

    if (
        profile
        and profile["name"]
        == "automotive_market"
    ):
        return (
            "автовладельцы, покупатели автомобилей, "
            "предприниматели и участники "
            "автомобильного рынка"
        )

    return (
        "автовладельцы, покупатели автомобилей "
        "и читатели автомобильных новостей"
    )


def region_text_for_event(
    event,
    profile,
):
    region = str(
        event.region or ""
    ).strip()

    if not is_service_profile(profile):
        if region == "RU":
            return "Россия"

        return region or "Регион не указан"

    if region == "RU":
        return (
            "Россия; практический контекст материала — "
            "Владикавказ и Северная Осетия"
        )

    if region:
        return (
            f"{region}; практический контекст материала — "
            "Владикавказ и Северная Осетия"
        )

    return "Владикавказ и Северная Осетия"


def safety_notes_for_profile(
    profile,
):
    common = (
        "Не добавлять неподтверждённые факты, "
        "причины, прогнозы, последствия, цены, "
        "характеристики и географию. "
        "Не выдавать предположение за факт."
    )

    if not is_service_profile(profile):
        return (
            common
            + " Не превращать автомобильную новость "
            "в рекламу автосервиса и не добавлять "
            "советы по диагностике или ремонту, "
            "если они не относятся к исходному событию."
        )

    return (
        common
        + " Не ставить диагноз по симптомам или "
        "коду ошибки. Не обещать результат ремонта "
        "и не рекомендовать замену конкретной детали "
        "без диагностики."
    )


def disclaimer_required_for_profile(
    profile,
):
    return is_service_profile(profile)


class Command(BaseCommand):
    help = (
        "Create AutomotiveBrief records from "
        "quality Event records with topic=auto."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--hours",
            type=int,
            default=720,
        )
        parser.add_argument(
            "--min-evidence",
            type=int,
            default=1,
        )
        parser.add_argument(
            "--min-score",
            type=int,
            default=8,
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=10,
        )
        parser.add_argument(
            "--event-ids",
            default="",
            help=(
                "Comma or space separated Event IDs. "
                "When provided, the time window is ignored."
            ),
        )
        parser.add_argument(
            "--status",
            choices=[
                AutomotiveBriefStatus.DRAFT,
                AutomotiveBriefStatus.READY,
            ],
            default=AutomotiveBriefStatus.READY,
        )
        parser.add_argument(
            "--allow-weak",
            action="store_true",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
        )
        parser.add_argument(
            "--show-rejected",
            action="store_true",
        )

    def handle(self, *args, **options):
        hours = max(
            int(options["hours"]),
            1,
        )

        min_evidence = max(
            int(options["min_evidence"]),
            0,
        )

        min_score = int(
            options["min_score"]
        )

        limit = max(
            int(options["limit"]),
            1,
        )

        event_ids = parse_ids(
            options["event_ids"]
        )

        queryset = (
            Event.objects
            .filter(
                topic=Topic.AUTO,
                evidence_level__gte=(
                    min_evidence
                ),
                automotive_briefs__isnull=True,
            )
            .prefetch_related(
                "items__item__source",
            )
            .distinct()
            .order_by(
                "-updated_at",
                "-id",
            )
        )

        if event_ids:
            queryset = queryset.filter(
                id__in=event_ids,
            )
        else:
            since = (
                timezone.now()
                - timedelta(hours=hours)
            )

            queryset = queryset.filter(
                updated_at__gte=since,
            )

        events = list(
            queryset[:limit]
        )

        checked = 0
        accepted = 0
        rejected = 0
        created = 0

        for event in events:
            checked += 1

            title = compact(
                event.title,
                1000,
            )

            material = collect_event_material(
                event
            )

            profile_text = "\n".join(
                [
                    title,
                    compact(
                        event.summary,
                        3000,
                    ),
                    *material["facts"],
                ]
            )

            profile, profile_hits = (
                choose_profile(profile_text)
            )

            service_intent_hits = matching_terms(
                profile_text,
                SERVICE_INTENT_TERMS,
            )

            automotive_hits = matching_terms(
                profile_text,
                AUTOMOTIVE_CONTEXT_TERMS,
            )

            hard_title_hits = matching_terms(
                title,
                HARD_REJECT_TITLE_TERMS,
            )

            hard_context_hits = matching_terms(
                profile_text,
                HARD_REJECT_CONTEXT_TERMS,
            )

            score, reasons = score_event(
                event,
                material,
                profile_hits,
            )

            if profile is None:
                reasons.append(
                    "no-specific-profile"
                )
            elif not is_service_profile(profile):
                reasons.append(
                    "non-service-profile:"
                    + profile["name"]
                )

            if not service_intent_hits:
                reasons.append(
                    "no-service-search-intent"
                )

            if not automotive_hits:
                reasons.append(
                    "no-automotive-context"
                )

            if hard_title_hits:
                reasons.append(
                    "hard-title-noise:"
                    + ",".join(
                        hard_title_hits
                    )
                )

            if hard_context_hits:
                reasons.append(
                    "hard-context-noise:"
                    + ",".join(
                        hard_context_hits
                    )
                )

            editorial_rejected = (
                profile is None
                or not is_service_profile(profile)
                or not service_intent_hits
                or not automotive_hits
                or bool(hard_title_hits)
                or bool(hard_context_hits)
            )

            weak = (
                not title
                or not material["urls"]
                or not material["facts"]
                or score < min_score
            )

            if (
                editorial_rejected
                or (
                    weak
                    and not options["allow_weak"]
                )
            ):
                rejected += 1

                if options["show_rejected"]:
                    self.stdout.write(
                        self.style.WARNING(
                            f"SKIP Event #{event.id}: "
                            f"score={score}; "
                            f"{', '.join(reasons) or 'weak'} | "
                            f"{title[:130]}"
                        )
                    )

                continue

            accepted += 1

            facts = "\n".join(
                f"- {fact}"
                for fact in material["facts"]
            )

            source_urls = "\n".join(
                material["urls"]
            )

            secondary_keywords = "\n".join(
                profile[
                    "secondary_keywords"
                ]
            )

            if options["dry_run"]:
                self.stdout.write(
                    self.style.SUCCESS(
                        f"WOULD CREATE Event #{event.id}: "
                        f"score={score}; "
                        f"profile={profile['name']} | "
                        f"{title[:130]}"
                    )
                )
                continue

            with transaction.atomic():
                locked_event = (
                    Event.objects
                    .select_for_update()
                    .get(pk=event.pk)
                )

                if AutomotiveBrief.objects.filter(
                    event=locked_event,
                ).exists():
                    continue

                AutomotiveBrief.objects.create(
                    event=locked_event,
                    title=title,
                    angle=profile["angle"],
                    target_keyword=profile[
                        "target_keyword"
                    ],
                    secondary_keywords=(
                        secondary_keywords
                    ),
                    facts=facts,
                    source_urls=source_urls,
                    audience=(
                        audience_for_profile(
                            profile
                        )
                    ),
                    region_text=(
                        region_text_for_event(
                            locked_event,
                            profile,
                        )
                    ),
                    safety_notes=(
                        safety_notes_for_profile(
                            profile
                        )
                    ),
                    disclaimer_required=(
                        disclaimer_required_for_profile(
                            profile
                        )
                    ),
                    status=options["status"],
                )

                created += 1

                self.stdout.write(
                    self.style.SUCCESS(
                        f"CREATED Event #{event.id}: "
                        f"score={score}; "
                        f"profile={profile['name']} | "
                        f"{title[:130]}"
                    )
                )

        if not events:
            self.stdout.write(
                self.style.WARNING(
                    "Нет автомобильных Event "
                    "для создания AutomotiveBrief."
                )
            )

        self.stdout.write("")
        self.stdout.write(
            f"Checked: {checked}"
        )
        self.stdout.write(
            f"Accepted: {accepted}"
        )
        self.stdout.write(
            f"Rejected: {rejected}"
        )
        self.stdout.write(
            f"Created: {created}"
        )

        if options["dry_run"]:
            self.stdout.write(
                self.style.NOTICE(
                    "Dry-run: база данных не изменена."
                )
            )
