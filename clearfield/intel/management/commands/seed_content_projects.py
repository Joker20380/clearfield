from django.core.management.base import BaseCommand

from intel.models import ContentProject, ContentSource, ContentTemplate


PROJECTS = (
    {
        "key": "dzagurov",
        "name": "КДЛ «Дзагуров»",
        "niche": "лабораторная диагностика и здоровье",
        "audience": "пациенты во Владикавказе и Северной Осетии",
        "brand_name": "КДЛ «Дзагуров»",
        "site_url": "https://kdl-dzagurov.ru/",
        "policy": {
            "risk_level": "high",
            "min_evidence_claims": 3,
            "min_source_domains": 2,
            "min_evidence_chars": 900,
            "require_source_quotes": True,
            "allowed_source_domains": [
                "cdc.gov",
                "medlineplus.gov",
                "who.int",
                "kdl-dzagurov.ru",
            ],
            "expert_review_required": True,
            "forbidden_claims": [
                "диагноз по одному анализу",
                "назначение или отмена лечения",
                "гарантия результата",
            ],
        },
        "system_prompt": (
            "Не ставь диагноз, не назначай лечение и не давай "
            "индивидуальных медицинских рекомендаций."
        ),
    },
    {
        "key": "diagnost",
        "name": "Diagnost RSO",
        "niche": "диагностика, обслуживание и ремонт автомобилей",
        "audience": "автовладельцы Владикавказа и Северной Осетии",
        "brand_name": "Diagnost RSO",
        "site_url": "https://diagnost-rso.ru/",
        "policy": {
            "risk_level": "medium",
            "min_evidence_claims": 2,
            "min_source_domains": 1,
            "min_evidence_chars": 600,
            "require_source_quotes": True,
            "allowed_source_domains": [
                "epa.gov",
                "nepis.epa.gov",
                "diagnost-rso.ru",
            ],
            "expert_review_required": True,
            "forbidden_claims": [
                "диагноз автомобиля по одному коду ошибки",
                "обязательная замена детали без проверки",
                "гарантия результата ремонта",
            ],
        },
        "system_prompt": (
            "Не ставь дистанционный технический диагноз и не советуй "
            "замену деталей без последовательной диагностики."
        ),
    },
)


TEMPLATES = (
    {
        "key": "evergreen-guide",
        "name": "Полезное руководство",
        "content_type": "evergreen_article",
        "instructions": (
            "Объясни тему последовательно: короткий ответ, основные "
            "различия или причины, порядок действий и ограничения."
        ),
        "min_chars": 1800,
        "max_chars": 4000,
        "min_sections": 3,
        "expert_review_required": True,
    },
    {
        "key": "question-answer",
        "name": "Ответ на практический вопрос",
        "content_type": "answer_article",
        "instructions": (
            "Дай прямой ответ и поясни его без искусственного FAQ "
            "и повторения поисковой фразы."
        ),
        "min_chars": 1200,
        "max_chars": 2800,
        "min_sections": 2,
        "expert_review_required": True,
    },
)


SOURCES = {
    "dzagurov": (
        {
            "name": "CDC: A1C Test",
            "url": (
                "https://www.cdc.gov/diabetes/diabetes-testing/"
                "prediabetes-a1c-test.html"
            ),
            "source_type": "official",
            "trust_level": 5,
        },
        {
            "name": "MedlinePlus: HbA1c Test",
            "url": (
                "https://medlineplus.gov/lab-tests/"
                "hemoglobin-a1c-hba1c-test/"
            ),
            "source_type": "official",
            "trust_level": 5,
        },
    ),
    "diagnost": (
        {
            "name": "EPA: OBD Questions and Answers",
            "url": (
                "https://nepis.epa.gov/Exe/ZyPURL.cgi"
                "?Dockey=P100LW9G.TXT"
            ),
            "source_type": "official",
            "trust_level": 5,
        },
        {
            "name": "EPA: OBD FAQ",
            "url": (
                "https://nepis.epa.gov/Exe/ZyPURL.cgi"
                "?Dockey=P1009Z15.TXT"
            ),
            "source_type": "official",
            "trust_level": 5,
        },
    ),
}


class Command(BaseCommand):
    help = "Создаёт профили универсального контентного движка."

    def add_arguments(self, parser):
        parser.add_argument("--apply", action="store_true")

    def handle(self, *args, **options):
        if not options["apply"]:
            self.stdout.write(
                "DRY RUN: projects=2 templates=4; добавьте --apply"
            )
            return

        for config in PROJECTS:
            values = dict(config)
            key = values.pop("key")
            project, _ = ContentProject.objects.update_or_create(
                key=key,
                defaults=values,
            )
            for template_config in TEMPLATES:
                values = dict(template_config)
                template_key = values.pop("key")
                ContentTemplate.objects.update_or_create(
                    project=project,
                    key=template_key,
                    defaults=values,
                )
            for source_config in SOURCES[key]:
                values = dict(source_config)
                url = values.pop("url")
                ContentSource.objects.update_or_create(
                    project=project,
                    url=url,
                    defaults=values,
                )

        self.stdout.write(
            self.style.SUCCESS(
                "Projects: 2; templates: 4; evidence sources: 4"
            )
        )
