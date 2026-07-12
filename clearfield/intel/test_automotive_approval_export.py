import json
import tempfile
from io import StringIO
from pathlib import Path

from django.core.management import (
    call_command,
)
from django.test import TestCase

from intel.automotive_editorial_validation import (
    DEFAULT_DISCLAIMER,
)
from intel.models import (
    AutomotiveBrief,
    AutomotiveBriefStatus,
    AutomotiveNewsStatus,
    GeneratedAutomotiveNews,
)


def valid_body():
    paragraph = (
        "Компьютерная диагностика помогает "
        "зафиксировать коды ошибок и текущие "
        "параметры электронных систем. "
        "Однако один код не определяет точную "
        "причину неисправности. Для проверки "
        "специалист сопоставляет данные блоков "
        "управления с электрическими измерениями, "
        "состоянием проводки и разъёмов. "
    )

    return (
        "## Что изменилось\n\n"
        + paragraph * 5
        + "\n\n## Что важно владельцу\n\n"
        + paragraph * 5
        + "\n\n## Как проходит проверка\n\n"
        + paragraph * 3
        + "\n\n"
        + DEFAULT_DISCLAIMER
    )


class AutomotiveApprovalExportTests(
    TestCase
):
    def setUp(self):
        self.brief = (
            AutomotiveBrief.objects.create(
                title=(
                    "Обновлены рекомендации "
                    "по диагностике электронных "
                    "систем автомобиля"
                ),
                angle=(
                    "Объяснить последовательность "
                    "диагностики без дистанционного "
                    "определения причины."
                ),
                target_keyword=(
                    "диагностика автоэлектрики"
                ),
                secondary_keywords=(
                    "компьютерная диагностика\n"
                    "проверка датчиков"
                ),
                facts=(
                    "- Коды ошибок сопоставляются "
                    "с текущими параметрами.\n"
                    "- Для поиска причины проводят "
                    "электрические измерения."
                ),
                source_urls=(
                    "https://example.com/"
                    "automotive/source-1"
                ),
                audience="автовладельцы",
                region_text=(
                    "Практический контекст — "
                    "Владикавказ"
                ),
                safety_notes=(
                    "Не утверждать необходимость "
                    "замены детали без диагностики."
                ),
                disclaimer_required=True,
                status=(
                    AutomotiveBriefStatus.USED
                ),
            )
        )

    def create_news(
        self,
        *,
        quality_score=85,
        status=AutomotiveNewsStatus.REVIEW,
    ):
        return (
            GeneratedAutomotiveNews
            .objects.create(
                brief=self.brief,
                title=(
                    "Как проверяют электронные "
                    "системы современного автомобиля"
                ),
                slug=(
                    "proverka-elektronnyh-"
                    "sistem-avtomobilya"
                ),
                meta_description=(
                    "Почему одного кода ошибки "
                    "недостаточно и как специалист "
                    "проверяет электронные системы "
                    "современного автомобиля."
                ),
                body=valid_body(),
                source_note=(
                    "Материал подготовлен "
                    "по подтверждённым сведениям "
                    "редакционного задания."
                ),
                source_urls=[
                    (
                        "https://example.com/"
                        "automotive/source-1"
                    ),
                ],
                image_topic="auto_electrics",
                disclaimer=DEFAULT_DISCLAIMER,
                quality_score=quality_score,
                status=status,
                llm_model="test-model",
            )
        )

    def test_auto_approve_valid_news(self):
        news = self.create_news()

        call_command(
            "auto_approve_automotive_news",
            "--news-ids",
            str(news.pk),
            "--min-score",
            "70",
            stdout=StringIO(),
        )

        news.refresh_from_db()

        self.assertEqual(
            news.status,
            AutomotiveNewsStatus.APPROVED,
        )

    def test_low_score_remains_review(self):
        news = self.create_news(
            quality_score=45,
        )

        output = StringIO()

        call_command(
            "auto_approve_automotive_news",
            "--news-ids",
            str(news.pk),
            "--min-score",
            "70",
            "--show-skipped",
            stdout=output,
        )

        news.refresh_from_db()

        self.assertEqual(
            news.status,
            AutomotiveNewsStatus.REVIEW,
        )

        self.assertIn(
            "low-quality-score:45",
            output.getvalue(),
        )

    def test_export_contract(self):
        news = self.create_news(
            status=(
                AutomotiveNewsStatus.APPROVED
            ),
        )

        with tempfile.TemporaryDirectory() as temp:
            output_path = (
                Path(temp)
                / "automotive-feed.json"
            )

            call_command(
                "export_automotive_news_feed",
                "--output",
                str(output_path),
                stdout=StringIO(),
            )

            payload = json.loads(
                output_path.read_text(
                    encoding="utf-8",
                )
            )

        self.assertEqual(
            payload["source"],
            (
                "clearfield_generated_"
                "automotive_news"
            ),
        )

        self.assertEqual(
            len(payload["items"]),
            1,
        )

        item = payload["items"][0]

        self.assertEqual(
            item["source_id"],
            f"automotive-news-{news.pk}",
        )

        self.assertEqual(
            item["body_markdown"],
            news.body,
        )

        self.assertEqual(
            item["body"],
            news.body,
        )

        self.assertEqual(
            item["source_urls"],
            news.source_urls,
        )

        self.assertEqual(
            item["image_topic"],
            "auto_electrics",
        )

        news.refresh_from_db()

        self.assertEqual(
            news.status,
            AutomotiveNewsStatus.APPROVED,
        )

    def test_mark_published_after_export(self):
        news = self.create_news(
            status=(
                AutomotiveNewsStatus.APPROVED
            ),
        )

        with tempfile.TemporaryDirectory() as temp:
            output_path = (
                Path(temp)
                / "automotive-feed.json"
            )

            call_command(
                "export_automotive_news_feed",
                "--output",
                str(output_path),
                "--mark-published",
                stdout=StringIO(),
            )

            self.assertTrue(
                output_path.is_file()
            )

        news.refresh_from_db()

        self.assertEqual(
            news.status,
            AutomotiveNewsStatus.PUBLISHED,
        )

        self.assertIsNotNone(
            news.published_at
        )
