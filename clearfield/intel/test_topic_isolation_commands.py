from asgiref.sync import async_to_sync
from django.core.management import (
    call_command,
)
from django.test import TransactionTestCase

from intel.management.commands.ingest_feeds import (
    get_sources,
)
from intel.models import (
    Cadence,
    Event,
    EventItem,
    RawItem,
    Region,
    Source,
    SourceClass,
    Topic,
)


class TopicIsolationCommandTests(
    TransactionTestCase
):
    reset_sequences = True

    def setUp(self):
        self.auto_source = (
            Source.objects.create(
                name="Auto test source",
                url=(
                    "https://example.com/"
                    "auto-feed.xml"
                ),
                region=Region.RU,
                topic=Topic.AUTO,
                source_class=(
                    SourceClass.INDUSTRY
                ),
                cadence=Cadence.MEDIUM,
                is_enabled=True,
            )
        )

        self.medical_source = (
            Source.objects.create(
                name="Medical test source",
                url=(
                    "https://example.com/"
                    "medical-feed.xml"
                ),
                region=Region.RU,
                topic=Topic.MEDICINE,
                source_class=(
                    SourceClass.OFFICIAL
                ),
                cadence=Cadence.MEDIUM,
                is_enabled=True,
            )
        )

        self.auto_raw = RawItem.objects.create(
            source=self.auto_source,
            guid="auto-guid",
            url=(
                "https://example.com/"
                "auto-news"
            ),
            title="Automotive test news",
            summary="Automotive summary",
            item_hash="a" * 64,
        )

        self.medical_raw = (
            RawItem.objects.create(
                source=self.medical_source,
                guid="medical-guid",
                url=(
                    "https://example.com/"
                    "medical-news"
                ),
                title="Medical test news",
                summary="Medical summary",
                item_hash="b" * 64,
            )
        )

    def test_ingest_source_queryset_is_filtered(
        self,
    ):
        sources = async_to_sync(
            get_sources
        )(
            limit=50,
            source_id=None,
            source_ids=[],
            only_last=None,
            topic=Topic.AUTO,
        )

        self.assertEqual(
            [source.id for source in sources],
            [self.auto_source.id],
        )

    def test_cluster_dry_run_does_not_write(
        self,
    ):
        call_command(
            "cluster_events",
            "--topic",
            Topic.AUTO,
            "--dry-run",
        )

        self.assertEqual(
            Event.objects.count(),
            0,
        )

        self.assertEqual(
            EventItem.objects.count(),
            0,
        )

    def test_cluster_only_selected_topic(
        self,
    ):
        call_command(
            "cluster_events",
            "--topic",
            Topic.AUTO,
        )

        self.auto_raw.refresh_from_db()
        self.medical_raw.refresh_from_db()

        self.assertTrue(
            EventItem.objects.filter(
                item=self.auto_raw,
                event__topic=Topic.AUTO,
            ).exists()
        )

        self.assertFalse(
            EventItem.objects.filter(
                item=self.medical_raw,
            ).exists()
        )

        self.assertEqual(
            Event.objects.filter(
                topic=Topic.AUTO,
            ).count(),
            1,
        )

        self.assertEqual(
            Event.objects.filter(
                topic=Topic.MEDICINE,
            ).count(),
            0,
        )

    def test_cluster_key_fits_database_field(
        self,
    ):
        from intel.management.commands.cluster_events import (
            build_cluster_key,
        )

        key = build_cluster_key(
            self.auto_raw
        )

        field = Event._meta.get_field(
            "cluster_key"
        )

        self.assertLessEqual(
            len(key),
            field.max_length,
        )

        self.assertTrue(
            key.startswith("ih:")
        )
