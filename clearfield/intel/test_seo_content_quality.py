from django.test import SimpleTestCase

from intel.seo_content_quality import assess_seo_content


class SeoContentQualityTests(SimpleTestCase):
    def test_accepts_natural_keyword_wording_with_terms_in_order(self):
        result = assess_seo_content(
            title=(
                "HbA1c и глюкоза крови: в чём разница между анализами"
            ),
            meta_description=(
                "Подробное объяснение показателей на основе проверенных "
                "источников для обсуждения результатов со специалистом."
            ),
            body=(
                "## Первый раздел\n\n"
                "HbA1c и глюкоза крови показывают разные данные; разница "
                "раскрыта ниже. "
                + ("Подтверждённое объяснение. " * 80)
                + "\n\n## Второй раздел\n\n"
                + ("Дополнительное объяснение. " * 40)
                + "\n\n## Третий раздел\n\n"
                + ("Итоговое объяснение. " * 40)
            ),
            target_keyword="HbA1c и глюкоза крови разница",
            source_urls="https://example.com/source",
            evergreen=True,
        )

        self.assertNotIn("keyword-not-in-title", result.issues)
        self.assertNotIn("keyword-not-in-body", result.issues)

    def test_flags_thin_unsourced_evergreen_content(self):
        result = assess_seo_content(
            title="Короткий текст",
            meta_description="Кратко",
            body="Небольшой абзац.",
            target_keyword="диагностика двигателя",
            source_urls="",
            evergreen=True,
        )

        self.assertLess(result.score, 50)
        self.assertIn("body-too-short", result.issues)
        self.assertIn("no-sources", result.issues)

    def test_rejects_keyword_stuffing_signal(self):
        phrase = "диагностика двигателя"
        result = assess_seo_content(
            title=f"{phrase.capitalize()}: подробное руководство владельцу",
            meta_description=(
                "Практическое руководство по проверке автомобиля, "
                "которое помогает понять порядок обращения в сервис."
            ),
            body=(
                "## Первый раздел\n\n"
                + ((phrase + " ") * 70)
                + "\n\n## Второй раздел\n\n"
                + ("Полезное объяснение. " * 80)
                + "\n\n## Третий раздел\n\n"
                + ("Последовательность проверки. " * 80)
            ),
            target_keyword=phrase,
            source_urls="https://example.com/source",
            evergreen=True,
        )

        self.assertIn("keyword-stuffing", result.issues)
