import json
from io import StringIO
from types import SimpleNamespace

from django.core.management import call_command
from django.test import TestCase

from intel.models import (
    ContentBrief,
    ContentBriefStatus,
    ContentProject,
    ContentTemplate,
    GeneratedContent,
    GeneratedContentStatus,
)
from intel.universal_content_engine import (
    parse_and_audit_generated_content,
    parse_factual_verification,
    validate_evidence_pack,
)


class UniversalContentEngineTests(TestCase):
    def setUp(self):
        self.project = ContentProject.objects.create(
            key="test-project",
            name="Test",
            niche="тестовая тематика",
            audience="читатели",
            policy={
                "min_evidence_claims": 2,
                "min_source_domains": 2,
                "min_evidence_chars": 150,
            },
        )
        self.template = ContentTemplate.objects.create(
            project=self.project,
            key="guide",
            name="Guide",
            instructions="Написать полезное руководство.",
        )
        self.brief = ContentBrief.objects.create(
            project=self.project,
            template=self.template,
            cluster_key="test-cluster",
            title="Подробное руководство по тестовой теме",
            primary_keyword="руководство по тестовой теме",
            status=ContentBriefStatus.READY,
            evidence_pack=[
                {
                    "id": "E1",
                    "claim": (
                        "Первое подтверждённое утверждение содержит "
                        "достаточно подробное фактическое основание."
                    ),
                    "source_url": "https://one.example/source",
                    "source_title": "Источник один",
                },
                {
                    "id": "E2",
                    "claim": (
                        "Второе подтверждённое утверждение дополняет "
                        "материал независимым фактическим основанием."
                    ),
                    "source_url": "https://two.example/source",
                    "source_title": "Источник два",
                },
            ],
        )

    def test_validates_source_diversity(self):
        self.assertEqual(validate_evidence_pack(self.brief), [])

        self.brief.evidence_pack[1]["source_url"] = (
            "https://one.example/another"
        )
        errors = validate_evidence_pack(self.brief)
        self.assertIn("not-enough-source-domains:1<2", errors)

    def test_rejects_thin_evidence_for_long_article(self):
        self.project.policy["min_evidence_chars"] = 1000
        self.project.save(update_fields=["policy", "updated_at"])

        errors = validate_evidence_pack(self.brief)

        self.assertTrue(
            any(error.startswith("evidence-pack-too-thin:") for error in errors)
        )

    def test_audit_rejects_unknown_evidence_id(self):
        payload = json.dumps(
            {
                "title": (
                    "Руководство по тестовой теме: практический разбор"
                ),
                "slug": "test-guide",
                "meta_description": (
                    "Подробное описание тестовой темы на основе "
                    "проверенных источников и практической структуры."
                ),
                "body_markdown": (
                    "Краткий прямой ответ. [E1]\n\n"
                    "## Первый раздел\n\n"
                    + ("Полезное объяснение. " * 50)
                    + "[E1]\n\n## Второй раздел\n\n"
                    + ("Дополнительное объяснение. " * 50)
                    + "[E9]\n\n## Третий раздел\n\n"
                    + ("Итоговое объяснение. " * 50)
                ),
                "used_evidence_ids": ["E1", "E9"],
            },
            ensure_ascii=False,
        )

        result = parse_and_audit_generated_content(self.brief, payload)

        self.assertTrue(result["qa_report"]["hard_issues"])
        self.assertIn(
            "unknown-evidence:E9",
            result["qa_report"]["issues"],
        )

    def test_seed_projects_is_idempotent(self):
        call_command("seed_content_projects", "--apply")
        call_command("seed_content_projects", "--apply")

        self.assertEqual(
            ContentProject.objects.filter(
                key__in=["dzagurov", "diagnost"]
            ).count(),
            2,
        )
        self.assertEqual(
            ContentTemplate.objects.filter(
                project__key__in=["dzagurov", "diagnost"]
            ).count(),
            4,
        )

    def test_export_blocks_missing_expert_review_metadata(self):
        GeneratedContent.objects.create(
            brief=self.brief,
            title="Подробное руководство по тестовой теме",
            slug="test-guide",
            meta_description=(
                "Полезный материал с проверяемыми источниками "
                "и прозрачной редакционной подготовкой."
            ),
            body="## Раздел\n\nТекст [E1]",
            used_evidence_ids=["E1"],
            source_urls=["https://one.example/source"],
            qa_report={},
            quality_score=90,
            status=GeneratedContentStatus.APPROVED,
        )
        output = StringIO()

        call_command(
            "export_content",
            "--project=test-project",
            "--output=/tmp/not-used.json",
            "--dry-run",
            stdout=output,
        )

        self.assertIn("expert-review-metadata-required", output.getvalue())
        self.assertIn('"item_count": 0', output.getvalue())

    def test_factual_verification_is_conservative(self):
        result = parse_factual_verification(
            json.dumps(
                {
                    "supported": True,
                    "unsupported_claims": [
                        {
                            "quote": "Добавленный технический факт",
                            "reason": "Этого нет в evidence pack",
                        }
                    ],
                },
                ensure_ascii=False,
            )
        )

        self.assertFalse(result["supported"])
        self.assertEqual(len(result["unsupported_claims"]), 1)
