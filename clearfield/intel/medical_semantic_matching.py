from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


TOKEN_RE = re.compile(
    r"[0-9a-zа-я]+",
    re.IGNORECASE,
)


PROFILE_RULES = (
    {
        "name": "diabetes",
        "markers": (
            "диабет",
            "сахарный диабет",
            "глюкоз",
            "гликирован",
            "инсулинорезист",
            "углеводный обмен",
        ),
        "preferred": (
            (
                "гликированный гемоглобин",
                280,
            ),
            (
                "диагностика диабета",
                230,
            ),
            (
                "оценка инсулинорезистентности",
                220,
            ),
            (
                "глюкоза",
                190,
            ),
            (
                "инсулин",
                170,
            ),
            (
                "с-пептид",
                150,
            ),
        ),
        "reject_title": (
            "риск развития",
            "генетичес",
            "предрасполож",
        ),
    },
    {
        "name": "thyroid",
        "markers": (
            "щитовид",
            "гипотире",
            "гипертире",
            "тиреотроп",
            "тироксин",
            "ттг",
        ),
        "preferred": (
            (
                "тиреотропный гормон",
                280,
            ),
            (
                "ттг",
                275,
            ),
            (
                "т4 свободный",
                220,
            ),
            (
                "т3 свободный",
                190,
            ),
            (
                "антитела к тиреопероксидазе",
                175,
            ),
        ),
        "reject_title": (
            "пунктат",
            "цитологичес",
        ),
    },
    {
        "name": "anemia",
        "markers": (
            "анеми",
            "железодефиц",
            "дефицит желез",
            "ферритин",
            "гемоглобин",
        ),
        "preferred": (
            (
                "ферритин",
                280,
            ),
            (
                "клинический анализ крови",
                225,
            ),
            (
                "общий анализ крови",
                220,
            ),
            (
                "железо",
                190,
            ),
            (
                "трансферрин",
                175,
            ),
            (
                "витамин b12",
                160,
            ),
            (
                "фолиевая кислота",
                150,
            ),
        ),
        "reject_title": (
            "гликированный гемоглобин",
            "карбоксигемоглобин",
            "метгемоглобин",
            "скрытая кровь",
        ),
    },
    {
        "name": "vitamin_d",
        "markers": (
            "витамин d",
            "кальциферол",
            "25-oh",
            "25 он витамин",
            "дефицит витамина d",
        ),
        "preferred": (
            (
                "витамин d",
                285,
            ),
            (
                "25 он витамин d",
                280,
            ),
            (
                "кальциферол",
                270,
            ),
        ),
        "reject_title": (),
    },
    {
        "name": "liver",
        "markers": (
            "печен",
            "гепатит",
            "гепатоз",
            "билирубин",
            "аланинаминотрансфераз",
            "аспартатаминотрансфераз",
            "гамма гт",
            "алт",
            "аст",
        ),
        "preferred": (
            (
                "аланинаминотрансфераза",
                270,
            ),
            (
                "алт",
                265,
            ),
            (
                "аспартатаминотрансфераза",
                245,
            ),
            (
                "аст",
                240,
            ),
            (
                "билирубин общий",
                220,
            ),
            (
                "гамма гт",
                200,
            ),
            (
                "печеночный профиль",
                190,
            ),
        ),
        "reject_title": (
            "генетичес",
            "риск развития",
        ),
    },
    {
        "name": "kidney",
        "markers": (
            "почек",
            "почечн",
            "креатинин",
            "мочевин",
            "клубочков",
            "скф",
        ),
        "preferred": (
            (
                "креатинин в сыворотке",
                285,
            ),
            (
                "креатинин",
                270,
            ),
            (
                "определением скф",
                265,
            ),
            (
                "скорость клубочковой фильтрации",
                260,
            ),
            (
                "альбумин креатининовое соотношение",
                220,
            ),
            (
                "мочевина",
                190,
            ),
        ),
        "reject_title": (
            "генетичес",
            "риск развития",
        ),
    },
    {
        "name": "lipids",
        "markers": (
            "ишемичес",
            "инфаркт",
            "атеросклер",
            "холестерин",
            "липид",
            "лпнп",
            "лпвп",
        ),
        "preferred": (
            (
                "липидный профиль базовый",
                290,
            ),
            (
                "липидограмма",
                285,
            ),
            (
                "липидный профиль",
                270,
            ),
            (
                "холестерин лпнп",
                225,
            ),
            (
                "холестерин общий",
                200,
            ),
            (
                "индекс атерогенности",
                190,
            ),
            (
                "триглицериды",
                175,
            ),
        ),
        "reject_title": (
            "генетичес",
            "риск развития",
        ),
    },
    {
        "name": "inflammation",
        "markers": (
            "воспален",
            "с реактив",
            "срб",
            "прокальцитонин",
            "скорость оседания эритроцитов",
            "соэ",
        ),
        "preferred": (
            (
                "с реактивный белок",
                285,
            ),
            (
                "срб",
                280,
            ),
            (
                "прокальцитонин",
                230,
            ),
            (
                "скорость оседания эритроцитов",
                215,
            ),
            (
                "соэ",
                205,
            ),
        ),
        "reject_title": (),
    },
)


ACTIVATION_FIELDS = (
    (
        "title",
        55,
    ),
    (
        "angle",
        35,
    ),
)

SUPPLEMENTAL_FIELDS = (
    (
        "facts",
        15,
    ),
    (
        "target_keyword",
        10,
    ),
    (
        "secondary_keywords",
        5,
    ),
)


def normalize_text(value: Any) -> str:
    value = (
        str(value or "")
        .casefold()
        .replace("ё", "е")
    )

    value = re.sub(
        r"[^0-9a-zа-я]+",
        " ",
        value,
    )

    return " ".join(value.split())


def marker_present(
    normalized_text: str,
    marker: str,
) -> bool:
    normalized_marker = normalize_text(marker)

    if not normalized_marker:
        return False

    marker_tokens = normalized_marker.split()

    if (
        len(marker_tokens) == 1
        and len(marker_tokens[0]) <= 4
    ):
        return (
            marker_tokens[0]
            in normalized_text.split()
        )

    return normalized_marker in normalized_text


def canonical_items_sha256(
    items: list[dict[str, Any]],
) -> str:
    serialized = json.dumps(
        items,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")

    return hashlib.sha256(
        serialized
    ).hexdigest()


def load_semantic_feed(
    path: str | Path,
) -> dict[str, Any]:
    path = Path(path)

    try:
        payload = json.loads(
            path.read_text(encoding="utf-8")
        )
    except (
        OSError,
        json.JSONDecodeError,
    ) as exc:
        raise ValueError(
            "Не удалось прочитать semantic feed: "
            f"{exc}"
        ) from exc

    if not isinstance(payload, dict):
        raise ValueError(
            "Semantic feed должен быть JSON-объектом."
        )

    if payload.get("version") != 1:
        raise ValueError(
            "Поддерживается только semantic feed version=1."
        )

    if payload.get("source") != "kdl-dzagurov.ru":
        raise ValueError(
            "Неожиданный источник semantic feed."
        )

    if (
        payload.get("base_url")
        != "https://kdl-dzagurov.ru"
    ):
        raise ValueError(
            "Неожиданный base_url semantic feed."
        )

    items = payload.get("items")

    if not isinstance(items, list):
        raise ValueError(
            "Поле items должно быть списком."
        )

    if payload.get("item_count") != len(items):
        raise ValueError(
            "item_count не совпадает с items."
        )

    expected_hash = str(
        payload.get("content_sha256") or ""
    )

    actual_hash = canonical_items_sha256(
        items
    )

    if expected_hash != actual_hash:
        raise ValueError(
            "SHA256 semantic feed не совпадает: "
            f"expected={expected_hash}, "
            f"actual={actual_hash}"
        )

    return payload


@dataclass(frozen=True)
class ProfileEvidence:
    name: str
    score: int
    reasons: tuple[str, ...]
    rule: dict[str, Any]


@dataclass(frozen=True)
class Candidate:
    item: dict[str, Any]
    title_normalized: str
    category_normalized: str
    tests_normalized: tuple[str, ...]


@dataclass(frozen=True)
class RankedCandidate:
    item: dict[str, Any]
    score: int
    reasons: tuple[str, ...]
    profiles: tuple[str, ...]


class SemanticCatalog:
    def __init__(
        self,
        payload: dict[str, Any],
    ):
        self.payload = payload

        self.candidates = tuple(
            self._build_candidate(item)
            for item in payload["items"]
        )

    @staticmethod
    def _build_candidate(
        item: dict[str, Any],
    ) -> Candidate:
        return Candidate(
            item=item,
            title_normalized=normalize_text(
                item.get("title")
            ),
            category_normalized=normalize_text(
                item.get("category")
            ),
            tests_normalized=tuple(
                normalize_text(value)
                for value
                in (item.get("tests") or [])
                if str(value or "").strip()
            ),
        )

    @staticmethod
    def _brief_field(
        brief: Any,
        field: str,
    ) -> str:
        return normalize_text(
            getattr(brief, field, "") or ""
        )

    def detect_profiles(
        self,
        brief: Any,
    ) -> tuple[ProfileEvidence, ...]:
        normalized_fields = {
            field: self._brief_field(
                brief,
                field,
            )
            for field, _weight in (
                ACTIVATION_FIELDS
                + SUPPLEMENTAL_FIELDS
            )
        }

        results = []

        for rule in PROFILE_RULES:
            reasons = []
            score = 0
            activated = False

            for field, base_weight in (
                ACTIVATION_FIELDS
            ):
                hits = [
                    marker
                    for marker in rule["markers"]
                    if marker_present(
                        normalized_fields[field],
                        marker,
                    )
                ]

                if not hits:
                    continue

                activated = True

                contribution = (
                    base_weight
                    + min(
                        24,
                        8 * (len(hits) - 1),
                    )
                )

                score += contribution
                reasons.append(
                    f"{field}: "
                    f"{', '.join(hits)} "
                    f"(+{contribution})"
                )

            # target_keyword и facts могут усилить уже
            # найденную тему, но не могут активировать её.
            if not activated:
                continue

            for field, base_weight in (
                SUPPLEMENTAL_FIELDS
            ):
                hits = [
                    marker
                    for marker in rule["markers"]
                    if marker_present(
                        normalized_fields[field],
                        marker,
                    )
                ]

                if not hits:
                    continue

                contribution = (
                    base_weight
                    + min(
                        10,
                        5 * (len(hits) - 1),
                    )
                )

                score += contribution
                reasons.append(
                    f"{field}: "
                    f"{', '.join(hits)} "
                    f"(+{contribution})"
                )

            results.append(
                ProfileEvidence(
                    name=rule["name"],
                    score=min(score, 100),
                    reasons=tuple(reasons),
                    rule=rule,
                )
            )

        results.sort(
            key=lambda item: (
                -item.score,
                item.name,
            )
        )

        return tuple(results)

    @staticmethod
    def _phrase_candidate_bonus(
        candidate: Candidate,
        phrase: str,
        weight: int,
    ) -> tuple[int, str]:
        if marker_present(
            candidate.title_normalized,
            phrase,
        ):
            return (
                weight,
                "название карточки",
            )

        if any(
            marker_present(
                test,
                phrase,
            )
            for test
            in candidate.tests_normalized
        ):
            return (
                round(weight * 0.68),
                "состав панели",
            )

        if marker_present(
            candidate.category_normalized,
            phrase,
        ):
            return (
                round(weight * 0.30),
                "категория",
            )

        return 0, ""

    @staticmethod
    def _candidate_is_rejected(
        candidate: Candidate,
        rule: dict[str, Any],
    ) -> tuple[bool, str]:
        for marker in rule.get(
            "reject_title",
            (),
        ):
            if marker_present(
                candidate.title_normalized,
                marker,
            ):
                return (
                    True,
                    f"исключён маркером «{marker}»",
                )

        return False, ""

    def _score_for_profile(
        self,
        brief: Any,
        candidate: Candidate,
        evidence: ProfileEvidence,
    ) -> tuple[int, tuple[str, ...]]:
        rejected, rejection_reason = (
            self._candidate_is_rejected(
                candidate,
                evidence.rule,
            )
        )

        if rejected:
            return 0, (rejection_reason,)

        best_bonus = 0
        best_phrase = ""
        best_location = ""

        for phrase, weight in (
            evidence.rule["preferred"]
        ):
            bonus, location = (
                self._phrase_candidate_bonus(
                    candidate,
                    phrase,
                    weight,
                )
            )

            if bonus <= best_bonus:
                continue

            best_bonus = bonus
            best_phrase = phrase
            best_location = location

        if not best_bonus:
            return 0, ()

        score = (
            best_bonus
            + evidence.score
        )

        reasons = [
            (
                f"профиль {evidence.name}: "
                f"«{best_phrase}» в {best_location} "
                f"(+{best_bonus})"
            ),
            (
                f"доказательность темы "
                f"(+{evidence.score})"
            ),
        ]

        core_text = " ".join(
            [
                self._brief_field(
                    brief,
                    "title",
                ),
                self._brief_field(
                    brief,
                    "angle",
                ),
            ]
        )

        if marker_present(
            core_text,
            best_phrase,
        ):
            score += 45
            reasons.append(
                "название анализа прямо присутствует "
                "в заголовке или ракурсе (+45)"
            )

        test_count = len(
            candidate.item.get("tests") or []
        )

        if test_count > 8:
            penalty = min(
                40,
                (test_count - 8) * 5,
            )

            score -= penalty
            reasons.append(
                f"слишком широкая панель: "
                f"{test_count} тестов (-{penalty})"
            )

        return (
            max(0, int(score)),
            tuple(reasons),
        )

    def rank(
        self,
        brief: Any,
        top_n: int = 5,
    ) -> list[RankedCandidate]:
        profile_evidence = (
            self.detect_profiles(brief)
        )

        if not profile_evidence:
            return []

        ranked = []

        for candidate in self.candidates:
            best_score = 0
            best_reasons = ()
            best_profile = ""

            for evidence in profile_evidence:
                score, reasons = (
                    self._score_for_profile(
                        brief,
                        candidate,
                        evidence,
                    )
                )

                if score <= best_score:
                    continue

                best_score = score
                best_reasons = reasons
                best_profile = evidence.name

            if best_score <= 0:
                continue

            ranked.append(
                RankedCandidate(
                    item=candidate.item,
                    score=best_score,
                    reasons=best_reasons,
                    profiles=(
                        (best_profile,)
                        if best_profile
                        else ()
                    ),
                )
            )

        ranked.sort(
            key=lambda result: (
                -result.score,
                len(
                    result.item.get("tests")
                    or []
                ),
                str(
                    result.item.get("title")
                    or ""
                ).casefold(),
                str(
                    result.item.get("code")
                    or ""
                ),
            )
        )

        return ranked[:max(1, top_n)]
