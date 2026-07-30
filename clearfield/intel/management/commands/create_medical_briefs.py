import hashlib
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
    RawItem,
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
    "анем",
    "ферритин",
    "гемоглобин",
    "щитовид",
    "ттг",
    "аллерг",
    "беремен",
    "инфекц",
    "гепатит",
    "холестерин",
    "сердечно-сосуд",
    "онкомаркер",
    "скрининг",
)

HARD_OFFTOPIC_MARKERS = (
    "бензин",
    "азс",
    "топлив",
    "дтп",
    "автомоб",
    "ремонт дорог",
    "пожар",
    "мчс",
    "сизо",
    "конкурс",
    "фотоконкурс",
    "поздравил",
    "поздравление",
    "кадровой политик",
    "выпускник",
    "студент",
    "совещание",
    "подписали соглашение",
)

INSTITUTIONAL_MEDICINE_MARKERS = (
    "открылась больница",
    "открылась поликлиника",
    "открылось отделение",
    "капитального ремонта",
    "пополнил коллектив",
    "назначен главн",
    "провели операцию",
    "проведена операция",
    "выполнили операцию",
    "спасли пациент",
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
    offtopic_hits = [marker for marker in HARD_OFFTOPIC_MARKERS if marker in text]
    institutional_hits = [
        marker for marker in INSTITUTIONAL_MEDICINE_MARKERS if marker in text
    ]

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

    if offtopic_hits:
        score -= 20
        reasons.append("hard-offtopic:" + ",".join(offtopic_hits[:3]))

    if institutional_hits and not lab_hits:
        score -= 8
        reasons.append(
            "institutional-no-commercial-intent:"
            + ",".join(institutional_hits[:2])
        )

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

SEO_KEYWORD_POOLS = {
    "donation": (
        "обследования перед донорством",
        "безопасность донорства",
        "здоровье доноров",
    ),
    "diabetes": (
        "наблюдение при сахарном диабете",
        "оценка углеводного обмена",
        "профилактика осложнений диабета",
    ),
    "infections": (
        "диагностика инфекционных заболеваний",
        "профилактика сезонных инфекций",
        "обследование при симптомах ОРВИ",
    ),
    "prevention": (
        "профилактические обследования",
        "диспансеризация и профилактика заболеваний",
        "раннее выявление заболеваний",
    ),
    "maternal": (
        "здоровье матери и ребёнка",
        "обследования во время беременности",
        "медицинское наблюдение при беременности",
    ),
    "cardiology": (
        "профилактика сердечно-сосудистых заболеваний",
        "оценка сердечно-сосудистого риска",
        "обследование сердца и сосудов",
    ),
    "oncology": (
        "раннее выявление онкологических заболеваний",
        "диагностика и наблюдение в онкологии",
        "современная онкологическая диагностика",
    ),
    "thyroid": (
        "обследование щитовидной железы",
        "диагностика нарушений функции щитовидной железы",
        "наблюдение при заболеваниях щитовидной железы",
    ),
    "iron": (
        "диагностика дефицита железа",
        "обследования при анемии",
        "оценка обмена железа",
    ),
    "allergy": (
        "диагностика аллергических заболеваний",
        "обследования при аллергии",
        "выявление причин аллергических реакций",
    ),
    "hormones": (
        "исследование гормонального статуса",
        "диагностика гормональных нарушений",
        "обследования эндокринной системы",
    ),
    "liver": (
        "обследование функции печени",
        "диагностика заболеваний печени",
        "наблюдение за состоянием печени",
    ),
    "ultrasound": (
        "ультразвуковая диагностика",
        "современные методы УЗИ",
        "развитие инструментальной диагностики",
    ),
    "rehabilitation": (
        "медицинская реабилитация",
        "восстановление после заболеваний и травм",
        "современные методы реабилитации",
    ),
    "technology": (
        "современные медицинские технологии",
        "новое диагностическое оборудование",
        "модернизация медицинской помощи",
    ),
    "infrastructure": (
        "развитие медицинской инфраструктуры",
        "доступность медицинской помощи",
        "модернизация медицинских учреждений",
        "обновление объектов здравоохранения",
    ),
    "healthcare": (
        "развитие медицинской помощи",
        "доступность медицинских услуг",
        "современная организация здравоохранения",
    ),
    "laboratory": (
        "современная лабораторная диагностика",
        "лабораторные исследования и профилактика",
        "качество лабораторных исследований",
    ),
}


ANGLE_POOLS = {
    "donation": (
        "Сохранить акцент на донорстве и безопасности донорской помощи.",
        "Рассказать о значении донорства без неподтверждённых правил подготовки.",
    ),
    "diabetes": (
        "Сохранить акцент на профилактике и медицинском наблюдении при сахарном диабете.",
        "Объяснить значение контроля состояния здоровья при нарушениях углеводного обмена.",
    ),
    "infections": (
        "Сохранить акцент на профилактике инфекций и своевременном обращении за медицинской помощью.",
        "Подать событие в контексте инфекционной безопасности без неподтверждённых назначений.",
    ),
    "prevention": (
        "Раскрыть значение профилактических осмотров и раннего выявления заболеваний.",
        "Сохранить акцент на диспансеризации и доступности обследований.",
    ),
    "maternal": (
        "Подать событие в контексте здоровья матери и ребёнка.",
        "Сохранить акцент на медицинском наблюдении во время беременности.",
    ),
    "cardiology": (
        "Сохранить акцент на профилактике сердечно-сосудистых заболеваний.",
        "Раскрыть значение комплексной оценки состояния сердца и сосудов.",
    ),
    "oncology": (
        "Сохранить внимание на онкологическом событии, раннем обращении и наблюдении специалистом.",
        "Подать материал без запугивания, с акцентом на современной диагностике и лечении.",
    ),
    "thyroid": (
        "Подать материал в контексте здоровья щитовидной железы и наблюдения специалистом.",
    ),
    "iron": (
        "Сохранить акцент на выявлении дефицита железа и медицинской интерпретации результатов.",
    ),
    "allergy": (
        "Раскрыть тему аллергии и медицинской диагностики без категоричных выводов.",
    ),
    "hormones": (
        "Подать тему гормонального здоровья без самостоятельных назначений.",
    ),
    "liver": (
        "Сохранить акцент на заболеваниях печени и комплексной оценке состояния пациента.",
    ),
    "ultrasound": (
        "Сохранить акцент на возможностях УЗИ и инструментальной диагностике.",
        "Рассказать о применении ультразвукового оборудования без лабораторной вставки.",
    ),
    "rehabilitation": (
        "Сохранить основное внимание на восстановлении пациента и работе специалистов.",
        "Подать материал в контексте медицинской реабилитации без искусственной связи с анализами.",
    ),
    "education": (
        "Сохранить акцент на подготовке медицинских специалистов и клинической практике.",
        "Подать событие как образовательную медицинскую новость без отдельного лабораторного блока.",
        "Рассказать о профессиональном развитии медицинских работников.",
    ),
    "technology": (
        "Сохранить акцент на назначении оборудования и развитии медицинских технологий.",
        "Не заменять исходную тему общим текстом о лабораторной диагностике.",
    ),
    "infrastructure": (
        "Сохранить акцент на строительстве, ремонте или обновлении медицинского объекта и его значении для доступности помощи.",
        "Подать событие как новость о развитии медицинской инфраструктуры без искусственной связи с анализами.",
        "Рассказать об обновлении учреждения и организации медицинской помощи, не добавляя лабораторный раздел.",
    ),
    "healthcare": (
        "Сохранить акцент на организации и доступности медицинской помощи.",
        "Подать событие как отраслевую медицинскую новость без обязательного блока про анализы.",
    ),
    "laboratory": (
        "Сохранить акцент на лабораторной диагностике и корректной интерпретации результатов.",
        "Раскрыть значение лабораторных исследований только в пределах исходных фактов.",
    ),
    "emergency": (
        "Подать материал как нейтральную новость об экстренной помощи или происшествии. Не добавлять SEO-блок про анализы.",
        "Сохранить акцент на действиях медицинских или спасательных служб без продвижения лабораторных услуг.",
    ),
    "general": (
        "Сохранить исходную тему события. Не создавать искусственную связь с анализами или лабораторией.",
        "Подать материал как спокойную информационную новость без шаблонной SEO-вставки.",
    ),
}


NO_SEO_TOPICS = {
    "emergency",
    "education",
    "general",
}


OPTIONAL_SEO_TOPICS = {
    "technology",
    "infrastructure",
    "healthcare",
    "rehabilitation",
}


def stable_number(seed: str) -> int:
    digest = hashlib.sha256(
        normalize(seed).encode("utf-8")
    ).digest()

    return int.from_bytes(digest[:8], "big")


def stable_choice(values, seed: str) -> str:
    values = tuple(values or ())

    if not values:
        return ""

    return values[stable_number(seed) % len(values)]


def detect_content_topic(title: str, summary: str) -> str:
    title_text = normalize(title)
    text = normalize(f"{title} {summary}")

    # Широкие организационные события определяем прежде всего
    # по заголовку. Так случайное упоминание УЗИ, беременности или
    # другой темы внутри длинного summary не меняет основной сюжет.
    if has_marker(
        title_text,
        (
            "капитальный ремонт",
            "ремонт поликлиники",
            "ремонт больницы",
            "новое здание поликлиники",
            "новое здание больницы",
            "строительство поликлиники",
            "строительство больницы",
            "передвижных медицинских комплексов",
            "мобильных медицинских комплексов",
            "открыл новое здание",
        ),
    ):
        return "infrastructure"

    if has_marker(
        title_text,
        (
            "с рабочим визитом",
            "рабочий визит",
        ),
    ):
        return "healthcare"

    if has_marker(text, ("донор", "донорск", "переливан")):
        return "donation"

    if has_marker(
        text,
        ("диабет", "глюкоз", "сахар крови"),
    ):
        return "diabetes"

    if has_marker(
        text,
        (
            "грипп",
            "орви",
            "простуд",
            "инфекц",
            "коронавирус",
            "covid",
            "пцр",
            "клещ",
        ),
    ):
        return "infections"

    if has_marker(
        text,
        (
            "диспансеризац",
            "профилактическ",
            "профосмотр",
            "медицинский осмотр",
            "скрининг",
        ),
    ):
        return "prevention"

    if has_marker(
        text,
        (
            "беремен",
            "материнств",
            "роддом",
            "педиатр",
            "детск",
        ),
    ):
        return "maternal"

    if has_marker(
        text,
        (
            "кардиолог",
            "сердечно",
            "сердц",
            "аритми",
            "инфаркт",
            "инсульт",
        ),
    ):
        return "cardiology"

    if has_marker(
        text,
        (
            "онколог",
            "опухол",
            "меланом",
            "новообразован",
            "онкопациент",
        ),
    ):
        return "oncology"

    if has_marker(text, ("ттг", "щитовид")):
        return "thyroid"

    if has_marker(
        text,
        ("ферритин", "анем", "гемоглобин", "дефицит желез"),
    ):
        return "iron"

    if has_marker(text, ("аллерг",)):
        return "allergy"

    if has_marker(text, ("гормон", "эндокрин")):
        return "hormones"

    if has_real_liver_topic(text):
        return "liver"

    if has_marker(
        text,
        (
            "узи",
            "ультразвук",
            "ультразвуков",
            "маммограф",
            "флюорограф",
        ),
    ):
        return "ultrasound"

    if has_marker(
        text,
        (
            "реабилитац",
            "восстановлен",
            "физиотерап",
            "лечебная физкультура",
        ),
    ):
        return "rehabilitation"

    if has_marker(
        text,
        (
            "дтп",
            "авари",
            "экстренн",
            "реанимац",
            "скорая помощь",
            "первая помощь",
            "мчс",
            "спасател",
            "погиб",
            "обнаружили тело",
            "утонул",
            "чрезвычай",
        ),
    ):
        return "emergency"

    if has_marker(
        text,
        (
            "студент",
            "университет",
            "институт",
            "ординатур",
            "стажиров",
            "производственная практика",
            "наставнич",
            "выпускник",
        ),
    ):
        return "education"

    if has_marker(
        text,
        (
            "новое оборудование",
            "современное оборудование",
            "медицинская техника",
            "новый аппарат",
            "цифровая система",
            "телемедицин",
            "модернизац",
            "капитальный ремонт",
        ),
    ):
        return "technology"

    if has_marker(
        text,
        (
            "лаборатор",
            "анализ крови",
            "анализ мочи",
            "биоматериал",
        ),
    ):
        return "laboratory"

    if has_marker(
        text,
        (
            "больниц",
            "поликлиник",
            "врач",
            "хирург",
            "медицинск",
            "здравоохран",
            "пациент",
            "клиник",
        ),
    ):
        return "healthcare"

    return "general"


def detect_keyword(title: str, summary: str) -> str:
    topic = detect_content_topic(title, summary)

    if topic in NO_SEO_TOPICS:
        return ""

    # Для широких тем примерно каждая третья статья выходит
    # без заданной SEO-вставки.
    if (
        topic in OPTIONAL_SEO_TOPICS
        and stable_number(f"{title}|{summary}|seo") % 3 == 0
    ):
        return ""

    return stable_choice(
        SEO_KEYWORD_POOLS.get(topic, ()),
        f"{title}|{summary}|keyword",
    )


def build_angle(title: str, summary: str) -> str:
    topic = detect_content_topic(title, summary)
    keyword = detect_keyword(title, summary)

    angle = stable_choice(
        ANGLE_POOLS.get(topic, ANGLE_POOLS["general"]),
        f"{title}|{summary}|angle",
    )

    result = (
        angle
        + " Не делать медицинских назначений, не обещать результат "
        + "и не добавлять факты, которых нет в исходных данных."
    )

    if keyword:
        result += (
            f" Допустимый SEO-ориентир: «{keyword}». "
            "Использовать его только при естественной связи с событием. "
            "Не создавать ради него отдельный шаблонный раздел."
        )
    else:
        result += (
            " SEO-ориентир не задан. Не добавлять отдельный блок "
            "о лабораторной диагностике, анализах или подготовке к ним."
        )

    return result


# =============================================================================
# FACT COLLECTION
# =============================================================================

def collect_event_facts(event: Event) -> tuple[str, str]:
    facts: list[str] = []
    urls: list[str] = []
    seen_item_ids: set[int] = set()

    if event.title:
        facts.append(f"Событие: {compact(event.title, 500)}")

    if event.summary:
        facts.append(f"Краткое содержание: {compact(event.summary, 1600)}")

    def add_raw_item(item: RawItem | None) -> None:
        if not item or item.pk in seen_item_ids:
            return

        seen_item_ids.add(item.pk)

        url = (item.url or "").strip()

        if (
            url.startswith(("http://", "https://"))
            and url not in urls
        ):
            urls.append(url)

        source_name = (
            item.source.name
            if item.source
            else "источник"
        )

        if item.title:
            facts.append(
                f"Материал ({source_name}): "
                f"{compact(item.title, 500)}"
            )

        if item.summary:
            facts.append(
                f"Описание ({source_name}): "
                f"{compact(item.summary, 900)}"
            )

        try:
            article = item.article
        except Article.DoesNotExist:
            article = None

        if article and article.text:
            facts.append(
                f"Фрагмент статьи ({source_name}): "
                f"{compact(article.text, 1800)}"
            )

    event_items = (
        event.items
        .select_related("item", "item__source")
        .order_by("-created_at")[:8]
    )

    for event_item in event_items:
        add_raw_item(event_item.item)

    # Старые Event могут потерять EventItem-связи.
    # Сначала пробуем восстановить RawItem по cluster_key.
    if not urls:
        raw_items = (
            RawItem.objects
            .select_related("source")
            .order_by("-published_at", "-created_at", "-id")
        )

        cluster_key = (event.cluster_key or "").strip()

        if cluster_key.startswith("ih:"):
            item_hash = cluster_key.removeprefix("ih:").strip()

            if item_hash:
                for item in raw_items.filter(
                    item_hash=item_hash
                )[:8]:
                    add_raw_item(item)

        # Некоторые старые дубликаты Event имеют другой cluster_key.
        # В таком случае точный заголовок является безопасным fallback.
        if not urls and event.title:
            normalized_title = " ".join(event.title.split())

            for item in raw_items.filter(
                title__iexact=normalized_title
            )[:8]:
                add_raw_item(item)

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

            has_medical_signal = any(
                reason.startswith(("medical:", "lab:", "strong:"))
                for reason in reasons
            )
            has_commercial_signal = any(
                reason.startswith(("lab:", "strong:"))
                for reason in reasons
            )
            has_hard_offtopic = any(
                reason.startswith("hard-offtopic:")
                for reason in reasons
            )
            has_institutional_drift = any(
                reason.startswith("institutional-no-commercial-intent:")
                for reason in reasons
            )

            if not has_medical_signal:
                reasons = [*reasons, "no-medical-signal"]

            bad_title = is_bad_title(event.title or "")

            is_good = (
                score >= min_score
                and summary_len >= min_summary_len
                and not bad_title
                and has_medical_signal
                and has_commercial_signal
                and not has_hard_offtopic
                and not has_institutional_drift
            )

            if allow_weak:
                is_good = (
                    score >= 0
                    and summary_len >= 80
                    and not bad_title
                    and has_medical_signal
                )

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
