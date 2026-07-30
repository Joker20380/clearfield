from io import StringIO

from django.core.management import (
    call_command,
)
from django.test import TestCase
from django.utils import timezone

from intel.models import (
    AutomotiveBrief,
    AutomotiveBriefStatus,
    Cadence,
    Event,
    EventItem,
    RawItem,
    Region,
    Source,
    SourceClass,
    Topic,
)


class AutomotiveBriefCommandTests(TestCase):
    def setUp(self):
        self.source = Source.objects.create(
            name="Автомобильный тестовый источник",
            url=(
                "https://example.com/"
                "automotive-feed"
            ),
            region=Region.RU,
            topic=Topic.AUTO,
            source_class=(
                SourceClass.OFFICIAL
            ),
            cadence=Cadence.MEDIUM,
            is_enabled=True,
        )

        self.event = Event.objects.create(
            title=(
                "Новые требования к диагностике "
                "электронных систем автомобиля"
            ),
            summary=(
                "В официальном материале описаны "
                "изменения в диагностике электронных "
                "систем, датчиков и блоков управления "
                "современного автомобиля."
            ),
            region=Region.RU,
            topic=Topic.AUTO,
            evidence_level=2,
            cluster_key=(
                "automotive-command-test-event"
            ),
        )

        raw = RawItem.objects.create(
            source=self.source,
            guid="auto-test-1",
            url=(
                "https://example.com/"
                "automotive/article-1"
            ),
            title=(
                "Диагностика электронных систем "
                "автомобиля"
            ),
            summary=(
                "Проверка включает чтение кодов ошибок, "
                "контроль параметров датчиков и измерение "
                "электрических сигналов."
            ),
            published_at=timezone.now(),
            item_hash=(
                "automotive-command-test-hash"
            ),
        )

        EventItem.objects.create(
            event=self.event,
            item=raw,
        )

    def test_create_command_is_idempotent(self):
        dry_output = StringIO()

        call_command(
            "create_automotive_briefs",
            "--event-ids",
            str(self.event.id),
            "--min-score",
            "0",
            "--dry-run",
            stdout=dry_output,
        )

        self.assertEqual(
            AutomotiveBrief.objects.count(),
            0,
        )

        call_command(
            "create_automotive_briefs",
            "--event-ids",
            str(self.event.id),
            "--min-score",
            "0",
            stdout=StringIO(),
        )

        self.assertEqual(
            AutomotiveBrief.objects.count(),
            1,
        )

        brief = AutomotiveBrief.objects.get()

        self.assertEqual(
            brief.event_id,
            self.event.id,
        )

        self.assertEqual(
            brief.status,
            AutomotiveBriefStatus.READY,
        )

        self.assertIn(
            "https://example.com/"
            "automotive/article-1",
            brief.source_urls,
        )

        self.assertTrue(
            brief.target_keyword
        )

        self.assertTrue(
            brief.secondary_keywords
        )

        self.assertTrue(
            brief.safety_notes
        )

        call_command(
            "create_automotive_briefs",
            "--event-ids",
            str(self.event.id),
            "--min-score",
            "0",
            stdout=StringIO(),
        )

        self.assertEqual(
            AutomotiveBrief.objects.count(),
            1,
        )

    def test_audit_rejects_invalid_brief(self):
        call_command(
            "create_automotive_briefs",
            "--event-ids",
            str(self.event.id),
            "--min-score",
            "0",
            stdout=StringIO(),
        )

        valid_brief = (
            AutomotiveBrief.objects.get(
                event=self.event,
            )
        )

        invalid_brief = (
            AutomotiveBrief.objects.create(
                title="Коротко",
                facts="Нет фактов",
                source_urls="not-a-url",
                target_keyword="",
                secondary_keywords="",
                angle="",
                region_text="",
                safety_notes="",
                disclaimer_required=True,
                status=(
                    AutomotiveBriefStatus.READY
                ),
            )
        )

        output = StringIO()

        call_command(
            "audit_automotive_briefs",
            "--status",
            AutomotiveBriefStatus.READY,
            "--show-rejected",
            stdout=output,
        )

        valid_brief.refresh_from_db()
        invalid_brief.refresh_from_db()

        self.assertEqual(
            valid_brief.status,
            AutomotiveBriefStatus.READY,
        )

        self.assertEqual(
            invalid_brief.status,
            AutomotiveBriefStatus.REJECTED,
        )

        self.assertIn(
            f"REJECT #{invalid_brief.id}",
            output.getvalue(),
        )

    def test_audit_dry_run_does_not_change_status(self):
        invalid_brief = (
            AutomotiveBrief.objects.create(
                title="Коротко",
                facts="Нет фактов",
                source_urls="",
                target_keyword="",
                secondary_keywords="",
                angle="",
                region_text="",
                safety_notes="",
                disclaimer_required=True,
                status=(
                    AutomotiveBriefStatus.READY
                ),
            )
        )

        call_command(
            "audit_automotive_briefs",
            "--status",
            AutomotiveBriefStatus.READY,
            "--dry-run",
            stdout=StringIO(),
        )

        invalid_brief.refresh_from_db()

        self.assertEqual(
            invalid_brief.status,
            AutomotiveBriefStatus.READY,
        )


class AutomotiveBriefQualityGateTests(
    TestCase
):
    def setUp(self):
        self.source = Source.objects.create(
            name="Тестовый автомобильный источник",
            url=(
                "https://example.com/"
                "quality-feed"
            ),
            region=Region.RU,
            topic=Topic.AUTO,
            source_class=(
                SourceClass.INDUSTRY
            ),
            cadence=Cadence.MEDIUM,
            is_enabled=True,
        )

    def create_event(
        self,
        *,
        suffix,
        title,
        summary,
    ):
        event = Event.objects.create(
            title=title,
            summary=summary,
            region=Region.RU,
            topic=Topic.AUTO,
            evidence_level=1,
            cluster_key=(
                f"auto-quality-{suffix}"
            ),
        )

        raw = RawItem.objects.create(
            source=self.source,
            guid=f"guid-{suffix}",
            url=(
                "https://example.com/"
                f"news-{suffix}"
            ),
            title=title,
            summary=summary,
            published_at=timezone.now(),
            item_hash=(
                f"quality-hash-{suffix}"
            ),
        )

        EventItem.objects.create(
            event=event,
            item=raw,
        )

        return event

    def run_for_event(
        self,
        event,
    ):
        output = StringIO()

        call_command(
            "create_automotive_briefs",
            "--event-ids",
            str(event.id),
            "--min-score",
            "0",
            "--show-rejected",
            stdout=output,
        )

        return output.getvalue()

    def test_tire_stem_does_not_match_inside_machine_word(
        self,
    ):
        from intel.management.commands.create_automotive_briefs import (
            choose_profile,
        )

        profile, hits = choose_profile(
            "Вопрос дня: о какой машине "
            "вы мечтали в детстве?"
        )

        # Слово «машине» не должно ошибочно
        # совпадать с основой «шин».
        #
        # У развлекательного опроса нет
        # подходящего редакционного профиля.
        self.assertIsNone(profile)
        self.assertEqual(hits, 0)

    def test_rejects_entertainment_material(
        self,
    ):
        event = self.create_event(
            suffix="entertainment",
            title=(
                "Вопрос дня: о какой машине "
                "вы мечтали в детстве?"
            ),
            summary=(
                "Редакционный опрос читателей "
                "о любимых автомобилях."
            ),
        )

        output = self.run_for_event(event)

        self.assertEqual(
            AutomotiveBrief.objects.count(),
            0,
        )

        self.assertIn(
            "hard-title-noise",
            output,
        )

    def test_rejects_general_product_news(
        self,
    ):
        event = self.create_event(
            suffix="product",
            title=(
                "Новый кроссовер получил "
                "турбированный двигатель"
            ),
            summary=(
                "Производитель представил модель "
                "и сообщил дату начала продаж."
            ),
        )

        self.run_for_event(event)

        self.assertFalse(
            AutomotiveBrief.objects.filter(
                event=event,
            ).exists()
        )

    def test_rejects_automotive_market_news(
        self,
    ):
        event = self.create_event(
            suffix="market",
            title=(
                "Продажи новых автомобилей "
                "в России выросли"
            ),
            summary=(
                "Автомобильный рынок показал "
                "рост продаж новых машин."
            ),
        )

        self.run_for_event(event)

        self.assertFalse(
            AutomotiveBrief.objects.filter(
                event=event,
            ).exists()
        )

    def test_accepts_repair_cost_material(
        self,
    ):
        event = self.create_event(
            suffix="maintenance",
            title=(
                "Средний чек по ремонту "
                "автомобилей вырос"
            ),
            summary=(
                "Средний чек по ремонту и "
                "обслуживанию автомобилей вырос. "
                "Данные предоставила сеть "
                "автосервисов."
            ),
        )

        self.run_for_event(event)

        brief = AutomotiveBrief.objects.get(
            event=event,
        )

        self.assertIn(
            "ремонт и обслуживание",
            brief.target_keyword,
        )

    def test_accepts_repair_legislation_material(
        self,
    ):
        event = self.create_event(
            suffix="legislation",
            title=(
                "Производителей обяжут "
                "предоставлять данные для ремонта"
            ),
            summary=(
                "Поправки к закону меняют "
                "правила ремонта автомобилей "
                "и доступ к техническим данным."
            ),
        )

        self.run_for_event(event)

        brief = AutomotiveBrief.objects.get(
            event=event,
        )

        self.assertIn(
            "правила ремонта",
            brief.target_keyword,
        )
