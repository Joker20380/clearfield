from django.core.management.base import BaseCommand
from django.db import transaction

from intel.models import Cadence, Region, Source, SourceClass, Topic


SOURCES = [
    {
        "name": "Минздрав РФ — новости",
        "url": "https://minzdrav.gov.ru/news.atom",
        "region": Region.RU,
        "topic": Topic.MEDICINE,
        "source_class": SourceClass.OFFICIAL,
        "cadence": Cadence.MEDIUM,
    },
    {
        "name": "Минздрав РФ — региональные новости",
        "url": "https://minzdrav.gov.ru/regional_news",
        "region": Region.RU,
        "topic": Topic.MEDICINE,
        "source_class": SourceClass.OFFICIAL,
        "cadence": Cadence.MEDIUM,
    },
    {
        "name": "Минздрав РСО-Алания — новости",
        "url": "https://minzdrav.alania.gov.ru/news",
        "region": Region.RU,
        "topic": Topic.MEDICINE,
        "source_class": SourceClass.OFFICIAL,
        "cadence": Cadence.MEDIUM,
    },
    {
        "name": "Минздрав РСО-Алания — пресс-центр",
        "url": "https://minzdrav.alania.gov.ru/pressa/news",
        "region": Region.RU,
        "topic": Topic.MEDICINE,
        "source_class": SourceClass.OFFICIAL,
        "cadence": Cadence.MEDIUM,
    },
    {
        "name": "Роспотребнадзор РФ — новости RSS",
        "url": "https://www.rospotrebnadzor.ru/region/rss/rss.php",
        "region": Region.RU,
        "topic": Topic.MEDICINE,
        "source_class": SourceClass.OFFICIAL,
        "cadence": Cadence.MEDIUM,
    },
    {
        "name": "Росздравнадзор РФ — новости",
        "url": "https://roszdravnadzor.gov.ru/news",
        "region": Region.RU,
        "topic": Topic.MEDICINE,
        "source_class": SourceClass.OFFICIAL,
        "cadence": Cadence.COLD,
    },
    {
        "name": "ФМБА России — официальный сайт",
        "url": "https://fmba.gov.ru/",
        "region": Region.RU,
        "topic": Topic.MEDICINE,
        "source_class": SourceClass.OFFICIAL,
        "cadence": Cadence.COLD,
    },
    {
        "name": "ЦНИИ эпидемиологии Роспотребнадзора — новости",
        "url": "https://www.crie.ru/about/news/",
        "region": Region.RU,
        "topic": Topic.MEDICINE,
        "source_class": SourceClass.OFFICIAL,
        "cadence": Cadence.COLD,
    },
    {
        "name": "ФБУЗ ФЦГиЭ Роспотребнадзора — новости",
        "url": "https://fcgie.ru/news/",
        "region": Region.RU,
        "topic": Topic.MEDICINE,
        "source_class": SourceClass.OFFICIAL,
        "cadence": Cadence.COLD,
    },
    {
        "name": "Росстат — здравоохранение",
        "url": "https://rosstat.gov.ru/folder/13721",
        "region": Region.RU,
        "topic": Topic.MEDICINE,
        "source_class": SourceClass.STATS,
        "cadence": Cadence.COLD,
    },
    {
        "name": "ЦНИИОИЗ — медицинская статистика",
        "url": "https://mednet.ru/napravleniya/mediczinskaya-statistika/",
        "region": Region.RU,
        "topic": Topic.MEDICINE,
        "source_class": SourceClass.STATS,
        "cadence": Cadence.COLD,
    },
    {
        "name": "ТФОМС РСО-Алания — новости",
        "url": "https://www.omsalania.ru/",
        "region": Region.RU,
        "topic": Topic.MEDICINE,
        "source_class": SourceClass.OFFICIAL,
        "cadence": Cadence.COLD,
    },
]


class Command(BaseCommand):
    help = "Добавляет стартовый набор российских медицинских источников."

    def add_arguments(self, parser):
        parser.add_argument(
            "--disable-existing",
            action="store_true",
            help="Отключить существующие источники перед добавлением новых.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Показать источники без записи в базу.",
        )

    def handle(self, *args, **options):
        disable_existing = options["disable_existing"]
        dry_run = options["dry_run"]

        self.stdout.write(self.style.NOTICE("Российские медицинские источники:"))

        for source in SOURCES:
            self.stdout.write(f"- {source['name']} | {source['url']}")

        if dry_run:
            self.stdout.write(self.style.WARNING("Dry-run: запись в базу не выполнялась."))
            return

        created = 0
        updated = 0

        with transaction.atomic():
            if disable_existing:
                disabled = Source.objects.update(is_enabled=False)
                self.stdout.write(self.style.WARNING(f"Отключено существующих источников: {disabled}"))

            for data in SOURCES:
                obj, was_created = Source.objects.update_or_create(
                    url=data["url"],
                    defaults={
                        "name": data["name"],
                        "region": data["region"],
                        "topic": data["topic"],
                        "source_class": data["source_class"],
                        "cadence": data["cadence"],
                        "is_enabled": True,
                    },
                )

                if was_created:
                    created += 1
                else:
                    updated += 1

        self.stdout.write(self.style.SUCCESS(f"Создано источников: {created}"))
        self.stdout.write(self.style.SUCCESS(f"Обновлено источников: {updated}"))
