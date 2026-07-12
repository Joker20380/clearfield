from io import StringIO

from django.core.management import (
    call_command,
)
from django.test import TestCase

from intel.models import (
    Cadence,
    Region,
    Source,
    SourceClass,
    Topic,
)


OTHER_CADENCE = next(
    (
        value
        for value, _label
        in Cadence.choices
        if value != Cadence.MEDIUM
    ),
    Cadence.MEDIUM,
)


EXPECTED = {
    (
        "https://www.autostat.ru/"
        "news/rss/3/"
    ): "АВТОСТАТ — Новости",
    (
        "https://www.drom.ru/"
        "export/xml/news.rss"
    ): "Drom.ru — Новости",
}


class AutomotiveSourceSeedTests(
    TestCase
):
    def test_dry_run_does_not_write(self):
        output = StringIO()

        call_command(
            "seed_ru_automotive_sources",
            "--dry-run",
            stdout=output,
        )

        self.assertEqual(
            Source.objects.count(),
            0,
        )

        self.assertIn(
            "WOULD CREATE",
            output.getvalue(),
        )

    def test_seed_creates_expected_sources(self):
        call_command(
            "seed_ru_automotive_sources",
            stdout=StringIO(),
        )

        sources = list(
            Source.objects
            .filter(topic=Topic.AUTO)
            .order_by("url")
        )

        self.assertEqual(
            len(sources),
            2,
        )

        self.assertEqual(
            {
                source.url: source.name
                for source in sources
            },
            EXPECTED,
        )

        for source in sources:
            self.assertEqual(
                source.region,
                Region.RU,
            )

            self.assertEqual(
                source.source_class,
                SourceClass.INDUSTRY,
            )

            self.assertEqual(
                source.cadence,
                Cadence.MEDIUM,
            )

            self.assertTrue(
                source.is_enabled
            )

    def test_seed_is_idempotent(self):
        call_command(
            "seed_ru_automotive_sources",
            stdout=StringIO(),
        )

        first_ids = list(
            Source.objects
            .filter(topic=Topic.AUTO)
            .order_by("url")
            .values_list("id", flat=True)
        )

        output = StringIO()

        call_command(
            "seed_ru_automotive_sources",
            stdout=output,
        )

        second_ids = list(
            Source.objects
            .filter(topic=Topic.AUTO)
            .order_by("url")
            .values_list("id", flat=True)
        )

        self.assertEqual(
            first_ids,
            second_ids,
        )

        self.assertEqual(
            Source.objects
            .filter(topic=Topic.AUTO)
            .count(),
            2,
        )

        self.assertIn(
            "Unchanged: 2",
            output.getvalue(),
        )

    def test_unrelated_source_is_preserved(self):
        unrelated = Source.objects.create(
            name="Независимый тестовый источник",
            url=(
                "https://example.com/"
                "unrelated-auto-feed"
            ),
            region=Region.RU,
            topic=Topic.AUTO,
            source_class=(
                SourceClass.OFFICIAL
            ),
            cadence=OTHER_CADENCE,
            is_enabled=False,
        )

        call_command(
            "seed_ru_automotive_sources",
            stdout=StringIO(),
        )

        unrelated.refresh_from_db()

        self.assertEqual(
            unrelated.name,
            "Независимый тестовый источник",
        )

        self.assertEqual(
            unrelated.source_class,
            SourceClass.OFFICIAL,
        )

        self.assertEqual(
            unrelated.cadence,
            OTHER_CADENCE,
        )

        self.assertFalse(
            unrelated.is_enabled
        )

        self.assertEqual(
            Source.objects.count(),
            3,
        )
