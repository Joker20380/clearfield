from datetime import datetime, timezone
from types import SimpleNamespace

from django.test import SimpleTestCase

from intel.management.commands.export_medical_news_feed import (
    SEMANTIC_SECTION_HEADING,
    append_semantic_landing_link,
    append_source_links,
    semantic_landing_from_brief,
    validate_semantic_landing_content,
)


VALID_SHA = "a" * 64


def make_brief(
    *,
    assigned=True,
    code="18.105",
    url=None,
    anchor="Гликированный гемоглобин",
):
    if url is None:
        url = (
            "https://kdl-dzagurov.ru/"
            f"analysis/{code}/"
        )

    return SimpleNamespace(
        source_urls=(
            "https://example.org/source"
        ),
        semantic_panel_id=(
            67
            if assigned
            else None
        ),
        semantic_panel_code=(
            code
            if assigned
            else ""
        ),
        semantic_panel_title=(
            anchor
            if assigned
            else ""
        ),
        semantic_panel_url=(
            url
            if assigned
            else ""
        ),
        semantic_anchor=(
            anchor
            if assigned
            else ""
        ),
        semantic_score=(
            380
            if assigned
            else 0
        ),
        semantic_feed_sha256=(
            VALID_SHA
            if assigned
            else ""
        ),
        semantic_assigned_at=(
            datetime.now(timezone.utc)
            if assigned
            else None
        ),
    )


class MedicalNewsSemanticExportTests(
    SimpleTestCase
):
    def test_unassigned_brief_leaves_content_unchanged(
        self,
    ):
        content = "Основной текст новости."

        result = append_semantic_landing_link(
            content,
            make_brief(assigned=False),
        )

        self.assertEqual(
            result,
            content,
        )

        self.assertIsNone(
            validate_semantic_landing_content(
                result,
                make_brief(assigned=False),
            )
        )

    def test_valid_assignment_appends_exactly_one_link(
        self,
    ):
        brief = make_brief()

        content = append_semantic_landing_link(
            "Основной текст новости.",
            brief,
        )

        landing = (
            validate_semantic_landing_content(
                content,
                brief,
            )
        )

        self.assertEqual(
            landing["code"],
            "18.105",
        )

        self.assertEqual(
            content.count(
                "https://kdl-dzagurov.ru/"
                "analysis/18.105/"
            ),
            1,
        )

        self.assertEqual(
            content.count(
                SEMANTIC_SECTION_HEADING
            ),
            1,
        )

        self.assertIn(
            (
                "[Гликированный гемоглобин]"
                "(https://kdl-dzagurov.ru/"
                "analysis/18.105/)"
            ),
            content,
        )

    def test_external_sources_do_not_break_validation(
        self,
    ):
        brief = make_brief()

        content = append_semantic_landing_link(
            "Основной текст новости.",
            brief,
        )

        content = append_source_links(
            content,
            [
                "https://example.org/source",
            ],
        )

        landing = (
            validate_semantic_landing_content(
                content,
                brief,
            )
        )

        self.assertEqual(
            landing["code"],
            "18.105",
        )

        self.assertIn(
            "[Источник]"
            "(https://example.org/source)",
            content,
        )

    def test_mismatched_code_and_url_are_rejected(
        self,
    ):
        brief = make_brief(
            code="18.105",
            url=(
                "https://kdl-dzagurov.ru/"
                "analysis/93.140/"
            ),
        )

        with self.assertRaisesRegex(
            ValueError,
            "не соответствует",
        ):
            semantic_landing_from_brief(
                brief
            )

    def test_preexisting_analysis_link_is_rejected(
        self,
    ):
        brief = make_brief()

        content = (
            "Текст с уже добавленной ссылкой: "
            "[анализ]"
            "(https://kdl-dzagurov.ru/"
            "analysis/18.105/)."
        )

        with self.assertRaisesRegex(
            ValueError,
            "уже присутствует",
        ):
            append_semantic_landing_link(
                content,
                brief,
            )

    def test_unassigned_brief_cannot_export_analysis_link(
        self,
    ):
        brief = make_brief(
            assigned=False
        )

        content = (
            "Несанкционированная ссылка: "
            "[анализ]"
            "(https://kdl-dzagurov.ru/"
            "analysis/18.105/)."
        )

        with self.assertRaisesRegex(
            ValueError,
            "нет semantic-привязки",
        ):
            append_semantic_landing_link(
                content,
                brief,
            )

    def test_markdown_brackets_in_anchor_are_sanitized(
        self,
    ):
        brief = make_brief(
            anchor=(
                "Анализ [контрольный]"
            ),
        )

        content = append_semantic_landing_link(
            "Основной текст.",
            brief,
        )

        self.assertIn(
            (
                "[Анализ (контрольный)]"
                "(https://kdl-dzagurov.ru/"
                "analysis/18.105/)"
            ),
            content,
        )

        validate_semantic_landing_content(
            content,
            brief,
        )
