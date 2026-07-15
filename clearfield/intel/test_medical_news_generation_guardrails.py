from types import SimpleNamespace

from django.test import SimpleTestCase

from intel.management.commands.generate_medical_news import (
    SYSTEM_PROMPT,
    build_user_prompt,
    markdown_section_count,
    medical_editorial_meta_hits,
)


def make_brief():
    return SimpleNamespace(
        title="В больнице открылась школа сахарного диабета",
        angle="Обучение пациентов управлению заболеванием",
        audience="пациенты медицинских организаций",
        region_text="Россия",
        target_keyword="оценка углеводного обмена",
        secondary_keywords="",
        facts=(
            "Школа начала работу. "
            "Занятия проводит врач-эндокринолог."
        ),
        source_urls="https://example.org/source",
        safety_notes=(
            "Не ставить диагнозы и не назначать лечение."
        ),
        event=None,
    )


class MedicalNewsGenerationGuardrailsTests(
    SimpleTestCase
):
    def test_detects_source_gap_commentary(
        self,
    ):
        text = (
            "Конкретные методы диагностики и анализы "
            "не упоминаются в источниках."
        )

        self.assertIn(
            "source-gap-commentary",
            medical_editorial_meta_hits(text),
        )

    def test_detects_generic_test_preparation(
        self,
    ):
        text = (
            "Подготовка к любым медицинским "
            "исследованиям зависит от случая."
        )

        self.assertIn(
            "generic-test-preparation",
            medical_editorial_meta_hits(text),
        )

    def test_detects_extra_disclaimer_language(
        self,
    ):
        text = (
            "Информация не является медицинским "
            "назначением. Для получения точной и "
            "актуальной медицинской помощи нужно "
            "обратиться к специалисту."
        )

        hits = medical_editorial_meta_hits(
            text
        )

        self.assertIn(
            "extra-medical-disclaimer",
            hits,
        )
        self.assertIn(
            "generic-care-direction",
            hits,
        )

    def test_counts_markdown_sections(
        self,
    ):
        body = (
            "Введение.\n\n"
            "## Как работает школа\n\n"
            "Текст.\n\n"
            "## Кто может участвовать\n\n"
            "Текст."
        )

        self.assertEqual(
            markdown_section_count(body),
            2,
        )

    def test_prompt_requires_sections_and_forbids_meta_commentary(
        self,
    ):
        prompt = build_user_prompt(
            make_brief()
        )

        self.assertIn(
            "2–4 смысловых подзаголовка",
            prompt,
        )
        self.assertIn(
            "Не обсуждай, каких сведений нет",
            prompt,
        )
        self.assertIn(
            "полностью пропусти его",
            prompt,
        )

        self.assertNotIn(
            (
                "Подготовка зависит от конкретного "
                "исследования"
            ),
            SYSTEM_PROMPT,
        )
