import re
from typing import Iterable


# Каждая группа описывает утверждения, которые нельзя добавлять
# автоматически, если соответствующего факта нет в исходном материале.
#
# Проверка намеренно консервативная: для автоматической публикации
# лучше оставить материал на review, чем опубликовать правдоподобную
# галлюцинацию.
UNSUPPORTED_CLAIM_RULES: tuple[
    tuple[str, tuple[str, ...]],
    ...,
] = (
    (
        "remote-area-scope",
        (
            r"\bотдаленн\w*",
            r"\bудаленн\w*",
            r"\bтруднодоступн\w*",
        ),
    ),
    (
        "missing-medical-facilities",
        (
            r"отсутств\w+ постоянн\w+ доступност\w+",
            r"отсутств\w+ стационарн\w+ медицинск\w+ учрежден",
            r"нет стационарн\w+ медицинск\w+ учрежден",
        ),
    ),
    (
        "preventive-examinations",
        (
            r"профилактическ\w+ осмотр",
            r"профилактическ\w+ обследован",
        ),
    ),
    (
        "general-practitioner-consultations",
        (
            r"консультац\w+ врач\w+ общей практик",
            r"консультац\w+ врач\w+ общей врачебн\w+ практик",
        ),
    ),
    (
        "primary-care-services",
        (
            r"первичн\w+ медицинск\w+ помощ",
            r"форм\w+ первичн\w+ медицинск\w+ помощ",
        ),
    ),
    (
        "medical-teams",
        (
            r"медицинск\w+ бригад",
            r"выездн\w+ бригад",
        ),
    ),
    (
        "routing-criteria",
        (
            r"сезонн\w+ фактор",
            r"\bлогистик\w*",
            r"доступност\w+ транспорт",
        ),
    ),
    (
        "population-needs",
        (
            r"с учетом потребност\w+ населения",
            r"с учётом потребност\w+ населения",
        ),
    ),
    (
        "approved-plans",
        (
            r"в соответствии с утвержденн\w+ план",
            r"в соответствии с утверждённ\w+ план",
        ),
    ),
    (
        "local-information-channels",
        (
            r"местн\w+ администрац",
            r"информационн\w+ стенд",
        ),
    ),
    (
        "early-detection-effect",
        (
            r"ранн\w+ выявлен",
            r"выявля\w+ на ранн\w+ стад",
        ),
    ),
    (
        "respiratory-pathology-effect",
        (
            r"патологи\w+ орган\w+ дыхан",
            r"заболеван\w+ орган\w+ дыхан",
        ),
    ),
    (
        "access-improvement-effect",
        (
            r"повышен\w+ доступност",
            r"обеспеч\w+ доступност",
            r"сокращен\w+ разрыв\w* в доступност",
        ),
    ),
    (
        "health-improvement-effect",
        (
            r"улучшен\w+ общ\w+ состояни\w+ здоров",
            r"улучшен\w+ здоров\w+ населения",
        ),
    ),
)


def normalize_editorial_text(value: object) -> str:
    text = str(value or "").lower().replace("ё", "е")
    text = re.sub(r"[‐-‒–—−]", "-", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _has_pattern(text: str, patterns: Iterable[str]) -> bool:
    return any(
        re.search(pattern, text, flags=re.I)
        for pattern in patterns
    )


def unsupported_claim_hits(
    generated_text: object,
    source_text: object,
) -> list[str]:
    """
    Возвращает группы утверждений, присутствующие в сгенерированном
    материале, но отсутствующие в подтверждённом исходном тексте.

    Это не универсальная fact-checking система. Функция блокирует
    типовые правдоподобные домыслы, встречающиеся в автоматических
    медицинских новостях.
    """

    generated = normalize_editorial_text(generated_text)
    source = normalize_editorial_text(source_text)

    hits: list[str] = []

    for label, patterns in UNSUPPORTED_CLAIM_RULES:
        generated_has = _has_pattern(generated, patterns)
        source_has = _has_pattern(source, patterns)

        if generated_has and not source_has:
            hits.append(label)

    return hits
