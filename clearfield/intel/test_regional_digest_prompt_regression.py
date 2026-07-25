from datetime import datetime, timezone
from types import SimpleNamespace

from django.test import SimpleTestCase

from intel.management.commands.generate_regional_digest import (
    build_article_prompt,
    build_fact_prompt,
)


class RegionalDigestPromptRegressionTests(
    SimpleTestCase
):
    def test_fact_prompt_does_not_require_digest(self):
        evidence_pack = {
            "schema_version": 1,
            "region": {
                "code": "north_ossetia",
                "label": "Северная Осетия",
            },
            "topic": "medicine",
            "period": {
                "start": (
                    "2026-07-01T00:00:00+00:00"
                ),
                "end": (
                    "2026-07-23T00:00:00+00:00"
                ),
            },
            "events": [],
        }

        prompt = build_fact_prompt(
            evidence_pack
        )

        self.assertIn(
            "ИСТОЧНИКИ:",
            prompt,
        )

        self.assertIn(
            '"events": []',
            prompt,
        )

    def test_article_prompt_contains_semantic_core(
        self,
    ):
        digest = SimpleNamespace(
            title="",
            region_code="north_ossetia",
            region_label="Северная Осетия",
            topic="medicine",
            period_start=datetime(
                2026,
                7,
                1,
                tzinfo=timezone.utc,
            ),
            period_end=datetime(
                2026,
                7,
                23,
                tzinfo=timezone.utc,
            ),
        )

        fact_pack = {
            "facts": [
                {
                    "fact_id": "F1",
                    "event_id": "E1",
                    "source_ids": ["S1"],
                    "statement": (
                        "В регионе развивают "
                        "медицинскую диагностику."
                    ),
                    "evidence_quote": (
                        "медицинскую диагностику"
                    ),
                    "fact_type": "event",
                },
            ],
        }

        prompt = build_article_prompt(
            digest,
            fact_pack,
        )

        self.assertIn(
            '"site_semantic_core"',
            prompt,
        )

        self.assertIn(
            "лабораторная диагностика "
            "во Владикавказе",
            prompt,
        )

        self.assertIn(
            "Не формулируй неподтверждённые "
            "ожидаемые эффекты",
            prompt,
        )

        self.assertIn(
            "вообще не используй слова "
            "и словоформы с корнями",
            prompt,
        )

        self.assertIn(
            "Не добавляй выводы о пользе",
            prompt,
        )

        self.assertNotIn(
            "может быть связано с",
            prompt,
        )

        self.assertIn(
            "Не связывай события с предполагаемой "
            "пользой",
            prompt,
        )



from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from django.test import SimpleTestCase

from intel.management.commands import (
    generate_regional_digest as regional_digest_module,
)


class RegionalDigestCompositionRetryRegressionTests(
    SimpleTestCase
):
    def test_always_semantic_group_is_optional(self):
        groups = [
            {
                "key": "fallback",
                "label": "Fallback",
                "always": True,
                "triggers": [],
                "phrases": [
                    "Лабораторная диагностика",
                ],
                "patient_context": "Optional context",
            },
        ]

        with patch.object(
            regional_digest_module,
            "SITE_SEMANTIC_CORE",
            groups,
        ):
            selected = (
                regional_digest_module
                .select_site_semantic_core_from_text(
                    "Региональная медицинская новость"
                )
            )

        self.assertEqual(len(selected), 1)
        self.assertFalse(selected[0]["required"])
        self.assertEqual(
            selected[0]["selection_mode"],
            "fallback",
        )

    def test_triggered_semantic_group_is_required(self):
        groups = [
            {
                "key": "imaging",
                "label": "Imaging",
                "always": False,
                "triggers": [
                    "рентген",
                ],
                "phrases": [
                    "рентгенография",
                ],
                "patient_context": "Triggered context",
            },
        ]

        with patch.object(
            regional_digest_module,
            "SITE_SEMANTIC_CORE",
            groups,
        ):
            selected = (
                regional_digest_module
                .select_site_semantic_core_from_text(
                    "Запланирована рентгенография"
                )
            )

        self.assertEqual(len(selected), 1)
        self.assertTrue(selected[0]["required"])
        self.assertEqual(
            selected[0]["selection_mode"],
            "triggered",
        )

    def test_article_prompt_contains_retry_feedback(self):
        digest = SimpleNamespace(
            title="",
            region_code="",
            region_label="Северная Осетия",
            topic="medicine",
            period_start=datetime(
                2026,
                7,
                24,
                tzinfo=timezone.utc,
            ),
            period_end=datetime(
                2026,
                7,
                25,
                tzinfo=timezone.utc,
            ),
            digest_items=SimpleNamespace(),
        )

        fact_pack = {
            "facts": [
                {
                    "fact_id": "F1",
                    "event_id": "E1",
                    "statement": (
                        "Проверен медицинский пункт."
                    ),
                    "evidence_quote": (
                        "Проверен медицинский пункт."
                    ),
                    "fact_type": "inspection",
                },
            ],
        }

        prompt = (
            regional_digest_module.build_article_prompt(
                digest,
                fact_pack,
                retry_feedback=(
                    "Generated meta_description "
                    "is too short"
                ),
            )
        )

        self.assertIn(
            "ИСПРАВЛЕНИЕ ПРЕДЫДУЩЕЙ ПОПЫТКИ",
            prompt,
        )
        self.assertIn(
            "Generated meta_description is too short",
            prompt,
        )
        self.assertIn("120", prompt)
        self.assertIn("180", prompt)
        self.assertIn("required=false", prompt)



class RegionalDigestSemanticTriggerRegressionTests(
    SimpleTestCase
):
    def selected_keys(self, text):
        return {
            group["key"]
            for group in (
                regional_digest_module
                .select_site_semantic_core_from_text(
                    text
                )
            )
        }

    def test_generic_inspection_does_not_trigger_prevention(
        self,
    ):
        keys = self.selected_keys(
            "Проведён осмотр помещений учреждения."
        )

        self.assertNotIn(
            "prevention",
            keys,
        )

    def test_sanitary_conditions_do_not_trigger_infection(
        self,
    ):
        keys = self.selected_keys(
            "Проверено санитарное состояние помещений."
        )

        self.assertNotIn(
            "infection",
            keys,
        )

    def test_short_det_fragment_does_not_trigger_children(
        self,
    ):
        keys = self.selected_keys(
            "Проведена проверка деятельности учреждения."
        )

        self.assertNotIn(
            "children",
            keys,
        )

    def test_real_prevention_still_triggers_group(self):
        keys = self.selected_keys(
            "Организованы профилактические обследования "
            "и диспансеризация населения."
        )

        self.assertIn(
            "prevention",
            keys,
        )

    def test_real_infection_topic_still_triggers_group(self):
        keys = self.selected_keys(
            "В регионе проводится ПЦР-диагностика "
            "вирусной инфекции."
        )

        self.assertIn(
            "infection",
            keys,
        )

    def test_real_children_topic_still_triggers_group(self):
        keys = self.selected_keys(
            "Педиатр провёл обследование детей."
        )

        self.assertIn(
            "children",
            keys,
        )



class RegionalDigestAtomicGroundingRegressionTests(
    SimpleTestCase
):
    def test_composition_facts_exclude_evidence_quote(
        self,
    ):
        fact_pack = {
            "facts": [
                {
                    "fact_id": "F1",
                    "event_id": "E1",
                    "source_ids": ["S1"],
                    "statement": (
                        "Проверено медицинское помещение."
                    ),
                    "evidence_quote": (
                        "Проверено помещение, обсуждались "
                        "жалобы, адвокаты и родственники."
                    ),
                    "fact_type": "inspection",
                },
            ],
        }

        facts = (
            regional_digest_module
            .article_facts_for_composition(
                fact_pack
            )
        )

        self.assertEqual(len(facts), 1)
        self.assertNotIn(
            "evidence_quote",
            facts[0],
        )
        self.assertIn(
            "statement",
            facts[0],
        )

    def test_statement_support_excludes_evidence_quote(
        self,
    ):
        fact_pack = {
            "facts": [
                {
                    "statement": (
                        "Проверено медицинское помещение."
                    ),
                    "evidence_quote": (
                        "Разъяснялась процедура подачи жалоб."
                    ),
                },
            ],
        }

        support = (
            regional_digest_module
            .article_fact_support_text(
                fact_pack
            )
        )

        self.assertIn(
            "Проверено медицинское помещение",
            support,
        )
        self.assertNotIn(
            "жалоб",
            support,
        )

    def test_meta_description_rejects_too_short(
        self,
    ):
        with self.assertRaisesRegex(
            regional_digest_module.CommandError,
            "too short",
        ):
            (
                regional_digest_module
                .validate_meta_description_length(
                    "Короткое описание."
                )
            )

    def test_meta_description_rejects_too_long(
        self,
    ):
        with self.assertRaisesRegex(
            regional_digest_module.CommandError,
            "too long",
        ):
            (
                regional_digest_module
                .validate_meta_description_length(
                    "Я" * 181
                )
            )

    def test_meta_description_accepts_valid_length(
        self,
    ):
        (
            regional_digest_module
            .validate_meta_description_length(
                "Я" * 150
            )
        )

    def test_complaint_pattern_is_guarded(
        self,
    ):
        import re

        matches = [
            description
            for pattern, description in (
                regional_digest_module
                .COMPOSITION_PROCEDURAL_CLAIM_PATTERNS
            )
            if re.search(
                pattern,
                "Разъяснялась процедура подачи жалоб.",
                flags=re.I,
            )
        ]

        self.assertIn(
            "сведения о жалобах",
            matches,
        )



class RegionalDigestMetaCompactionRegressionTests(
    SimpleTestCase
):
    def test_valid_meta_is_not_changed(self):
        value = "Я" * 150

        result = (
            regional_digest_module
            .compact_meta_description(value)
        )

        self.assertEqual(result, value)

    def test_overlong_meta_is_compacted(self):
        value = (
            "В Северной Осетии рассмотрели обращения "
            "граждан по вопросам санитарного состояния, "
            "качества пищевых продуктов и условий "
            "проживания населения. "
            "Отчёт содержит подтверждённые сведения "
            "за первое полугодие 2026 года."
        )

        self.assertGreater(
            len(value),
            180,
        )

        result = (
            regional_digest_module
            .compact_meta_description(value)
        )

        self.assertGreaterEqual(
            len(result),
            120,
        )
        self.assertLessEqual(
            len(result),
            180,
        )
        self.assertTrue(
            result.endswith(
                (".", "!", "?")
            )
        )

    def test_compaction_does_not_cut_word(self):
        value = " ".join(
            ["подтверждённые"] * 30
        )

        result = (
            regional_digest_module
            .compact_meta_description(value)
        )

        self.assertLessEqual(
            len(result),
            180,
        )
        self.assertFalse(
            result.endswith("-")
        )
        self.assertTrue(
            result.endswith(".")
        )

    def test_compacted_meta_passes_validator(self):
        value = (
            "В Северной Осетии рассмотрели обращения "
            "граждан по вопросам санитарного состояния, "
            "качества пищевых продуктов и условий "
            "проживания населения. "
            "Отчёт содержит подтверждённые сведения "
            "за первое полугодие 2026 года."
        )

        result = (
            regional_digest_module
            .compact_meta_description(value)
        )

        (
            regional_digest_module
            .validate_meta_description_length(
                result
            )
        )



class RegionalDigestSingleEventSectionRegressionTests(
    SimpleTestCase
):
    def test_single_event_sections_are_merged(self):
        payload = {
            "blocks": [
                {
                    "kind": "lead",
                    "heading": "",
                    "text": "Вводный текст.",
                    "fact_ids": ["F1"],
                },
                {
                    "kind": "section",
                    "heading": "Первый раздел",
                    "text": "Первый факт.",
                    "fact_ids": ["F1", "F2"],
                },
                {
                    "kind": "section",
                    "heading": "Второй раздел",
                    "text": "Второй факт.",
                    "fact_ids": ["F3"],
                },
                {
                    "kind": "conclusion",
                    "heading": "Итоги",
                    "text": "Итоговый текст.",
                    "fact_ids": ["F1"],
                },
            ],
        }

        fact_pack = {
            "facts": [
                {
                    "fact_id": "F1",
                    "event_id": "E1796",
                },
                {
                    "fact_id": "F2",
                    "event_id": "E1796",
                },
                {
                    "fact_id": "F3",
                    "event_id": "E1796",
                },
            ],
        }

        normalized = (
            regional_digest_module
            .normalize_single_event_sections(
                payload,
                fact_pack,
            )
        )

        sections = [
            block
            for block in normalized["blocks"]
            if block.get("kind") == "section"
        ]

        self.assertEqual(
            len(sections),
            1,
        )

        self.assertEqual(
            sections[0]["fact_ids"],
            ["F1", "F2", "F3"],
        )

        self.assertIn(
            "Первый факт.",
            sections[0]["text"],
        )

        self.assertIn(
            "Второй факт.",
            sections[0]["text"],
        )

    def test_multiple_events_are_not_merged(self):
        payload = {
            "blocks": [
                {
                    "kind": "section",
                    "heading": "Событие 1",
                    "text": "Текст 1.",
                    "fact_ids": ["F1"],
                },
                {
                    "kind": "section",
                    "heading": "Событие 2",
                    "text": "Текст 2.",
                    "fact_ids": ["F2"],
                },
            ],
        }

        fact_pack = {
            "facts": [
                {
                    "fact_id": "F1",
                    "event_id": "E1",
                },
                {
                    "fact_id": "F2",
                    "event_id": "E2",
                },
            ],
        }

        normalized = (
            regional_digest_module
            .normalize_single_event_sections(
                payload,
                fact_pack,
            )
        )

        self.assertEqual(
            normalized,
            payload,
        )

    def test_normalization_does_not_mutate_payload(self):
        payload = {
            "blocks": [
                {
                    "kind": "section",
                    "heading": "Один",
                    "text": "Текст один.",
                    "fact_ids": ["F1"],
                },
                {
                    "kind": "section",
                    "heading": "Два",
                    "text": "Текст два.",
                    "fact_ids": ["F2"],
                },
            ],
        }

        original_count = len(
            payload["blocks"]
        )

        fact_pack = {
            "facts": [
                {
                    "fact_id": "F1",
                    "event_id": "E1",
                },
                {
                    "fact_id": "F2",
                    "event_id": "E1",
                },
            ],
        }

        (
            regional_digest_module
            .normalize_single_event_sections(
                payload,
                fact_pack,
            )
        )

        self.assertEqual(
            len(payload["blocks"]),
            original_count,
        )



class RegionalDigestPublicHealthTriggerRegressionTests(
    SimpleTestCase
):
    def selected_keys(self, text):
        return {
            group["key"]
            for group in (
                regional_digest_module
                .select_site_semantic_core_from_text(
                    text
                )
            )
        }

    def test_sanitary_epidemiological_wellbeing_is_not_infection(
        self,
    ):
        keys = self.selected_keys(
            "Рассмотрены обращения граждан по вопросам "
            "санитарно-эпидемиологического благополучия "
            "населения, качества продуктов и условий "
            "проживания."
        )

        self.assertNotIn(
            "infection",
            keys,
        )

    def test_explicit_viral_infection_still_triggers_group(
        self,
    ):
        keys = self.selected_keys(
            "В регионе зарегистрирована вирусная "
            "инфекция и проводится ПЦР-диагностика."
        )

        self.assertIn(
            "infection",
            keys,
        )
