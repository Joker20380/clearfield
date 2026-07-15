from types import SimpleNamespace

from django.test import SimpleTestCase

from intel.medical_semantic_matching import (
    SemanticCatalog,
    canonical_items_sha256,
    marker_present,
    normalize_text,
)


def make_item(
    panel_id,
    code,
    title,
    tests=None,
    category="",
):
    return {
        "panel_id": panel_id,
        "code": code,
        "title": title,
        "canonical_anchor": title,
        "relative_url": (
            f"/analysis/{code}/"
        ),
        "url": (
            "https://kdl-dzagurov.ru/"
            f"analysis/{code}/"
        ),
        "category": category,
        "price": "1000.00",
        "currency": "RUB",
        "duration": "1",
        "tests": tests or [],
        "biomaterials": [
            "СЫВОРОТКА КРОВИ",
        ],
        "semantic_terms": [
            title,
            category,
            *(tests or []),
        ],
        "search_text": " ".join(
            [
                code,
                title,
                category,
                *(tests or []),
            ]
        ),
        "boost": 1.0,
    }


def make_payload(items):
    return {
        "version": 1,
        "generated_at": (
            "2026-07-15T00:00:00+00:00"
        ),
        "source": "kdl-dzagurov.ru",
        "base_url": (
            "https://kdl-dzagurov.ru"
        ),
        "item_count": len(items),
        "category_count": 0,
        "content_sha256": (
            canonical_items_sha256(items)
        ),
        "categories": [],
        "items": items,
    }


def make_brief(
    title,
    target_keyword="",
    angle="",
    facts="",
    secondary_keywords="",
):
    return SimpleNamespace(
        title=title,
        target_keyword=target_keyword,
        angle=angle,
        facts=facts,
        secondary_keywords=secondary_keywords,
    )


class MedicalSemanticMatchingTests(
    SimpleTestCase
):
    def test_short_abbreviation_requires_whole_token(
        self,
    ):
        self.assertTrue(
            marker_present(
                normalize_text(
                    "Контроль ТТГ и Т4"
                ),
                "ТТГ",
            )
        )

        self.assertFalse(
            marker_present(
                normalize_text(
                    "Исследование СТТГ-профиля"
                ),
                "ТТГ",
            )
        )

    def test_target_keyword_cannot_activate_profile(
        self,
    ):
        brief = make_brief(
            title=(
                "Новое оборудование помогает "
                "самым маленьким пациентам"
            ),
            target_keyword=(
                "обследования во время беременности"
            ),
        )

        catalog = SemanticCatalog(
            make_payload(
                [
                    make_item(
                        1,
                        "18.103",
                        (
                            "Глюкозотолерантный тест "
                            "при беременности"
                        ),
                    ),
                ]
            )
        )

        self.assertEqual(
            catalog.detect_profiles(brief),
            (),
        )
        self.assertEqual(
            catalog.rank(brief),
            [],
        )

    def test_organizational_news_has_no_profile(
        self,
    ):
        brief = make_brief(
            title=(
                "СКГМИ и СКФУ: "
                "общий вектор развития"
            ),
            angle=(
                "Сотрудничество образовательных "
                "организаций"
            ),
        )

        catalog = SemanticCatalog(
            make_payload(
                [
                    make_item(
                        1,
                        "93.320",
                        (
                            "Риск развития диабета "
                            "- скрининг"
                        ),
                    ),
                ]
            )
        )

        self.assertEqual(
            catalog.detect_profiles(brief),
            (),
        )
        self.assertEqual(
            catalog.rank(brief),
            [],
        )

    def test_diabetes_prefers_hba1c_and_rejects_risk_panels(
        self,
    ):
        items = [
            make_item(
                1,
                "18.105",
                "Гликированный гемоглобин",
                tests=[
                    "Гликированный гемоглобин",
                ],
                category=(
                    "Биохимические исследования"
                ),
            ),
            make_item(
                2,
                "56.160",
                (
                    "Риск развития "
                    "инсулинзависимого "
                    "сахарного диабета I типа"
                ),
                tests=[
                    "Генетическое исследование",
                ],
            ),
            make_item(
                3,
                "93.321",
                "Диагностика диабета",
                tests=[
                    "Глюкоза",
                    "Гликированный гемоглобин",
                ],
            ),
        ]

        brief = make_brief(
            title=(
                "Школа сахарного диабета "
                "заработала в районной больнице"
            ),
            target_keyword=(
                "оценка углеводного обмена"
            ),
            angle=(
                "Контроль сахарного диабета"
            ),
        )

        ranked = SemanticCatalog(
            make_payload(items)
        ).rank(
            brief,
            top_n=3,
        )

        self.assertEqual(
            ranked[0].item["code"],
            "18.105",
        )

        returned_codes = {
            result.item["code"]
            for result in ranked
        }

        self.assertNotIn(
            "56.160",
            returned_codes,
        )

    def test_ischemic_heart_disease_prefers_basic_lipid_profile(
        self,
    ):
        items = [
            make_item(
                1,
                "93.140",
                "Липидный профиль - базовый",
                tests=[
                    "Холестерин общий",
                    "Холестерин-ЛПНП",
                    "Холестерин-ЛПВП",
                    "Триглицериды",
                ],
            ),
            make_item(
                2,
                "19.135",
                (
                    "Холестерин-ЛПНП "
                    "(липопротеины низкой плотности)"
                ),
            ),
            make_item(
                3,
                "56.200",
                (
                    "Риск развития осложнений "
                    "при гормональной терапии"
                ),
            ),
        ]

        brief = make_brief(
            title=(
                "Врачи спасли пациента "
                "с ишемической болезнью сердца"
            ),
            target_keyword=(
                "профилактика "
                "сердечно-сосудистых заболеваний"
            ),
            angle=(
                "Профилактика повторного инфаркта"
            ),
        )

        ranked = SemanticCatalog(
            make_payload(items)
        ).rank(
            brief,
            top_n=3,
        )

        self.assertEqual(
            ranked[0].item["code"],
            "93.140",
        )

        self.assertGreaterEqual(
            ranked[0].score,
            230,
        )
