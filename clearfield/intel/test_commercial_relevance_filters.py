from types import SimpleNamespace

from django.test import SimpleTestCase

from intel.management.commands.audit_automotive_briefs import (
    audit_brief as audit_automotive_brief,
)
from intel.management.commands.create_automotive_briefs import (
    is_service_profile,
    choose_profile,
)
from intel.management.commands.create_medical_briefs import (
    candidate_score as medical_candidate_score,
)


class MedicalCommercialRelevanceTests(SimpleTestCase):
    def event(self, title, summary):
        return SimpleNamespace(
            title=title,
            summary=summary,
            evidence_level=2,
            region="RU",
        )

    def test_rejects_local_fuel_news_from_medical_pipeline(self):
        score, reasons = medical_candidate_score(
            self.event(
                "Ситуация с бензином на АЗС Северной Осетии",
                (
                    "В республике сообщили о наличии топлива "
                    "и графике поставок на автозаправочные станции."
                ),
            )
        )

        self.assertLess(score, 0)
        self.assertTrue(
            any(reason.startswith("hard-offtopic:") for reason in reasons)
        )

    def test_prioritizes_patient_laboratory_search_topic(self):
        score, reasons = medical_candidate_score(
            self.event(
                "В Северной Осетии расширили скрининг диабета",
                (
                    "Пациентам доступно обследование: анализ крови "
                    "на глюкозу и оценка риска сахарного диабета. "
                    "Профилактика помогает раньше выявлять нарушения."
                ),
            )
        )

        self.assertGreater(score, 0)
        self.assertTrue(
            any(reason.startswith("lab:") for reason in reasons)
        )


class AutomotiveCommercialRelevanceTests(SimpleTestCase):
    def test_market_sales_profile_is_not_a_service_profile(self):
        profile, _ = choose_profile(
            "Продажи новых кроссоверов на авторынке выросли на 20 процентов"
        )

        self.assertIsNotNone(profile)
        self.assertFalse(is_service_profile(profile))

    def test_audit_rejects_news_without_service_search_intent(self):
        brief = SimpleNamespace(
            title="Toyota представила новый кроссовер для рынка России",
            facts=(
                "Производитель показал комплектации модели и сообщил "
                "о планируемом начале продаж автомобиля."
            ),
            target_keyword="новый кроссовер Toyota",
            angle=(
                "Рассказать о новой модели, её комплектациях "
                "и положении на российском автомобильном рынке."
            ),
            secondary_keywords="новинки авторынка\nкроссоверы России",
            region_text="Россия",
            safety_notes="Не делать неподтверждённых выводов.",
            source_urls="https://example.com/toyota",
            event_id=None,
        )

        reasons = audit_automotive_brief(
            brief,
            min_title_chars=20,
            min_facts_chars=80,
            allow_unlinked=True,
        )

        self.assertIn("no-service-search-intent", reasons)
