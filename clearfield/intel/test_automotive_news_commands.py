import json
from io import StringIO
from unittest.mock import patch

from django.core.management import (
    call_command,
)
from django.test import TestCase

from intel.llm.ollama_client import (
    OllamaResult,
)
from intel.models import (
    AutomotiveBrief,
    AutomotiveBriefStatus,
    AutomotiveNewsStatus,
    GeneratedAutomotiveNews,
)


def long_valid_body():
    paragraph = (
        "Проверка электронных систем начинается "
        "со сбора симптомов, чтения кодов ошибок "
        "и сопоставления их с текущими параметрами. "
        "Один код не определяет точную причину, "
        "поэтому мастер дополнительно проверяет "
        "питание, проводку, разъёмы и сигналы "
        "соответствующих датчиков. "
    )

    return (
        "## Что изменилось\n\n"
        + paragraph * 5
        + "\n\n## Что это значит для владельца\n\n"
        + paragraph * 5
        + "\n\n## Когда нужна проверка\n\n"
        + paragraph * 3
    )


class AutomotiveNewsGeneratorTests(
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
                    "поиска электрической "
                    "неисправности без постановки "
                    "дистанционного диагноза."
                ),
                target_keyword=(
                    "диагностика автоэлектрики"
                ),
                secondary_keywords=(
                    "компьютерная диагностика\n"
                    "проверка датчиков\n"
                    "поиск неисправности проводки"
                ),
                facts=(
                    "- Диагностика включает чтение "
                    "кодов ошибок.\n"
                    "- Коды сопоставляются с "
                    "текущими параметрами.\n"
                    "- Для поиска причины проводят "
                    "электрические измерения."
                ),
                source_urls=(
                    "https://example.com/"
                    "automotive/source-1"
                ),
                audience=(
                    "автовладельцы и клиенты "
                    "автосервиса"
                ),
                region_text=(
                    "Практический контекст — "
                    "Владикавказ и Северная Осетия"
                ),
                safety_notes=(
                    "Не утверждать, что конкретная "
                    "деталь требует замены без "
                    "диагностики."
                ),
                disclaimer_required=True,
                status=(
                    AutomotiveBriefStatus.READY
                ),
            )
        )

    def llm_result(
        self,
        body=None,
    ):
        payload = {
            "image_topic": (
                "auto_electrics"
            ),
            "title": (
                "Как изменилась диагностика "
                "электронных систем автомобиля"
            ),
            "slug": (
                "diagnostika-elektronnyh-"
                "sistem-avtomobilya"
            ),
            "meta_description": (
                "Почему чтения кода ошибки "
                "недостаточно и какие этапы "
                "включает проверка электронных "
                "систем автомобиля."
            ),
            "body_markdown": (
                body
                if body is not None
                else long_valid_body()
            ),
            "source_note": (
                "Материал подготовлен "
                "по подтверждённым данным "
                "редакционного задания."
            ),
            "quality_score": 87,
        }

        return OllamaResult(
            text=json.dumps(
                payload,
                ensure_ascii=False,
            ),
            model="automotive-test-model",
            elapsed_ms=25,
            raw={
                "model": (
                    "automotive-test-model"
                ),
                "choices": [
                    {
                        "message": {
                            "content": payload,
                        },
                    },
                ],
            },
        )

    @patch(
        "intel.management.commands."
        "generate_automotive_news."
        "generate_with_ollama"
    )
    def test_generation_creates_review(
        self,
        mocked_generate,
    ):
        mocked_generate.return_value = (
            self.llm_result()
        )

        call_command(
            "generate_automotive_news",
            "--brief-ids",
            str(self.brief.id),
            "--model",
            "automotive-test-model",
            stdout=StringIO(),
        )

        self.assertEqual(
            GeneratedAutomotiveNews.objects.count(),
            1,
        )

        news = (
            GeneratedAutomotiveNews.objects.get()
        )

        self.brief.refresh_from_db()

        self.assertEqual(
            news.status,
            AutomotiveNewsStatus.REVIEW,
        )

        self.assertEqual(
            self.brief.status,
            AutomotiveBriefStatus.USED,
        )

        self.assertEqual(
            news.source_urls,
            [
                (
                    "https://example.com/"
                    "automotive/source-1"
                ),
            ],
        )

        self.assertEqual(
            news.image_topic,
            "auto_electrics",
        )

        self.assertGreaterEqual(
            news.quality_score,
            60,
        )

        self.assertEqual(
            news.body.count(
                news.disclaimer
            ),
            1,
        )

        self.assertEqual(
            news.llm_model,
            "automotive-test-model",
        )

    @patch(
        "intel.management.commands."
        "generate_automotive_news."
        "generate_with_ollama"
    )
    def test_unsafe_generation_creates_error(
        self,
        mocked_generate,
    ):
        unsafe_body = (
            long_valid_body()
            + "\n\nМожно безопасно "
            "продолжать движение."
        )

        mocked_generate.return_value = (
            self.llm_result(
                body=unsafe_body,
            )
        )

        call_command(
            "generate_automotive_news",
            "--brief-ids",
            str(self.brief.id),
            "--model",
            "automotive-test-model",
            stdout=StringIO(),
            stderr=StringIO(),
        )

        self.brief.refresh_from_db()

        error_news = (
            GeneratedAutomotiveNews.objects.get()
        )

        self.assertEqual(
            error_news.status,
            AutomotiveNewsStatus.ERROR,
        )

        self.assertIn(
            "Automotive safety check failed",
            error_news.llm_error,
        )

        self.assertEqual(
            self.brief.status,
            AutomotiveBriefStatus.READY,
        )

    @patch(
        "intel.management.commands."
        "generate_automotive_news."
        "generate_with_ollama"
    )
    def test_dry_run_does_not_call_llm(
        self,
        mocked_generate,
    ):
        output = StringIO()

        call_command(
            "generate_automotive_news",
            "--brief-ids",
            str(self.brief.id),
            "--dry-run",
            stdout=output,
        )

        mocked_generate.assert_not_called()

        self.assertEqual(
            GeneratedAutomotiveNews.objects.count(),
            0,
        )

        self.brief.refresh_from_db()

        self.assertEqual(
            self.brief.status,
            AutomotiveBriefStatus.READY,
        )

        self.assertIn(
            "Подтверждённые факты",
            output.getvalue(),
        )

    def test_invalid_id_is_rejected(self):
        with self.assertRaisesMessage(
            Exception,
            "Некорректный AutomotiveBrief ID",
        ):
            call_command(
                "generate_automotive_news",
                "--brief-ids",
                "bad-id",
                stdout=StringIO(),
            )


from types import SimpleNamespace

from django.test import SimpleTestCase


class AutomotiveContentModeTests(
    SimpleTestCase
):
    def make_market_brief(self):
        return SimpleNamespace(
            title=(
                "Рынок новых LCV снизился "
                "на 19,4%"
            ),
            facts=(
                "В России реализовано 31 668 "
                "новых LCV, что на 19,4% меньше "
                "прошлогоднего результата."
            ),
            target_keyword=(
                "автомобильный рынок России"
            ),
            angle=(
                "Раскрыть подтверждённые "
                "показатели рынка."
            ),
            event=None,
            disclaimer_required=False,
        )

    def test_detects_unconfirmed_market_claims(
        self,
    ):
        from intel.automotive_editorial_validation import (
            unsupported_market_claim_errors,
        )

        brief = self.make_market_brief()

        errors = unsupported_market_claim_errors(
            brief,
            (
                "Снижение может быть связано "
                "с рядом факторов. Оно может "
                "повлиять на доступность машин. "
                "Важно отслеживать изменения "
                "на рынке."
            ),
        )

        joined = " ".join(errors)

        self.assertIn(
            "unconfirmed-cause",
            joined,
        )

        self.assertIn(
            "unconfirmed-consequence",
            joined,
        )

        self.assertIn(
            "generic-market-advice",
            joined,
        )

    def test_detects_non_service_topic_drift(
        self,
    ):
        from intel.automotive_editorial_validation import (
            unsupported_market_claim_errors,
        )

        brief = self.make_market_brief()

        errors = unsupported_market_claim_errors(
            brief,
            (
                "Клиентам автосервисов следует "
                "провести диагностику автомобиля."
            ),
        )

        self.assertIn(
            "non-service-topic-drift",
            " ".join(errors),
        )

    def test_strips_diagnostic_disclaimer(
        self,
    ):
        from intel.automotive_editorial_validation import (
            DEFAULT_DISCLAIMER,
            strip_automotive_disclaimer,
        )

        body = (
            "## Рыночная статистика\n\n"
            "Подтверждённые показатели.\n\n"
            + DEFAULT_DISCLAIMER
        )

        cleaned = strip_automotive_disclaimer(
            body
        )

        self.assertNotIn(
            DEFAULT_DISCLAIMER,
            cleaned,
        )

        self.assertIn(
            "Подтверждённые показатели",
            cleaned,
        )

    def test_market_image_topic_is_deterministic(
        self,
    ):
        from intel.management.commands.generate_automotive_news import (
            preferred_image_topic_for_brief,
        )

        brief = self.make_market_brief()

        self.assertEqual(
            preferred_image_topic_for_brief(
                brief,
                "diagnostics",
            ),
            "automotive_market",
        )


class AutomotiveDataScopeGuardTests(
    SimpleTestCase
):
    def test_detects_unconfirmed_data_scope(
        self,
    ):
        from intel.automotive_editorial_validation import (
            unsupported_market_claim_errors,
        )

        brief = SimpleNamespace(
            title=(
                "Рынок новых LCV снизился "
                "на 19,4%"
            ),
            facts=(
                "В России реализовано 31 668 "
                "новых LCV, что на 19,4% меньше "
                "прошлогоднего результата."
            ),
            target_keyword=(
                "автомобильный рынок России"
            ),
            angle=(
                "Раскрыть подтверждённые "
                "показатели рынка."
            ),
            event=None,
            disclaimer_required=False,
        )

        errors = unsupported_market_claim_errors(
            brief,
            (
                "Статистика не включает в себя "
                "подержанные и импортные автомобили."
            ),
        )

        self.assertIn(
            "unconfirmed-data-scope",
            " ".join(errors),
        )


class AutomotiveProductClaimGuardTests(
    SimpleTestCase
):
    def test_detects_unconfirmed_product_claims(
        self,
    ):
        from intel.automotive_editorial_validation import (
            unsupported_market_claim_errors,
        )

        brief = SimpleNamespace(
            title=(
                "В России представлен "
                "новый седан TENET A8"
            ),
            facts=(
                "Бренд TENET представил седан A8. "
                "Производство будет налажено "
                "на заводе в Шушарах. "
                "Стоимость и комплектации "
                "будут известны позднее."
            ),
            target_keyword=(
                "автомобильные новости России"
            ),
            angle=(
                "Передать только подтверждённые "
                "сведения о модели."
            ),
            event=None,
            disclaimer_required=False,
        )

        body = """
        Это первый шаг бренда на массовом рынке.
        Модель принадлежит к классу средних автомобилей
        и является частью стратегии развития бренда.

        Новинку показали в рамках официальной презентации.
        Локализация подтверждает намерение компании
        развивать отечественное автопроизводство.

        Ожидается, что полная информация будет
        представлена в ближайшее время.
        Нет данных о планах по экспорту.
        """

        errors = unsupported_market_claim_errors(
            brief,
            body,
        )

        joined = " ".join(errors)

        self.assertIn(
            "unconfirmed-product-positioning",
            joined,
        )

        self.assertIn(
            "unconfirmed-presentation-details",
            joined,
        )

        self.assertIn(
            "unconfirmed-corporate-intent",
            joined,
        )

        self.assertIn(
            "unconfirmed-future-disclosure",
            joined,
        )

        self.assertIn(
            "unconfirmed-absence-claim",
            joined,
        )


class AutomotiveConditionalBodyLengthTests(
    SimpleTestCase
):
    def test_general_news_minimum_is_700(
        self,
    ):
        from intel.automotive_editorial_validation import (
            minimum_body_chars_for_brief,
        )

        brief = SimpleNamespace(
            disclaimer_required=False,
        )

        self.assertEqual(
            minimum_body_chars_for_brief(
                brief
            ),
            700,
        )

    def test_service_news_minimum_is_1000(
        self,
    ):
        from intel.automotive_editorial_validation import (
            minimum_body_chars_for_brief,
        )

        brief = SimpleNamespace(
            disclaimer_required=True,
        )

        self.assertEqual(
            minimum_body_chars_for_brief(
                brief
            ),
            1000,
        )


class AutomotiveProductFramingGuardTests(
    SimpleTestCase
):
    def test_detects_unconfirmed_product_framing(
        self,
    ):
        from intel.automotive_editorial_validation import (
            unsupported_market_claim_errors,
        )

        brief = SimpleNamespace(
            title=(
                "В России представлен "
                "новый седан TENET A8"
            ),
            facts=(
                "Бренд TENET впервые представил "
                "седан A8. Производство по полному "
                "циклу будет налажено на заводе "
                "в Шушарах. Стоимость и комплектации "
                "будут известны позднее."
            ),
            target_keyword=(
                "автомобильные новости России"
            ),
            angle=(
                "Передать только подтверждённые "
                "сведения."
            ),
            event=None,
            disclaimer_required=False,
        )

        body = """
        Бренд впервые официально представил модель.
        Событие прошло в рамках анонса производства.

        Полный цикл подтверждает реализацию планов
        по локализации сборки автомобилей.

        Цена будет опубликована в установленные сроки,
        которые пока не уточнены.

        Информация о модели не содержит сведений
        о технических характеристиках, экспортных
        планах, доступности на других рынках
        или сроках начала продаж.
        """

        errors = unsupported_market_claim_errors(
            brief,
            body,
        )

        joined = " ".join(errors)

        self.assertIn(
            "unconfirmed-event-framing",
            joined,
        )

        self.assertIn(
            "unconfirmed-localization-inference",
            joined,
        )

        self.assertIn(
            "unconfirmed-deadline-framing",
            joined,
        )

        self.assertIn(
            "unconfirmed-missing-details-list",
            joined,
        )
