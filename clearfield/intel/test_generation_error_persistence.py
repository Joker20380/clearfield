from django.test import TestCase

from intel.management.commands.generate_automotive_news import (
    save_generation_error as save_automotive_error,
)
from intel.management.commands.generate_medical_news import (
    save_generation_error as save_medical_error,
)
from intel.models import (
    AutomotiveBrief,
    AutomotiveNewsStatus,
    GeneratedAutomotiveNews,
    GeneratedMedicalNews,
    MedicalBrief,
    MedicalNewsStatus,
)


class GenerationErrorPersistenceTests(TestCase):
    def test_medical_retries_update_one_error_row(self):
        brief = MedicalBrief.objects.create(
            title="Медицинская новость",
        )

        save_medical_error(
            brief.pk,
            brief.title,
            "first prompt",
            "test-model",
            RuntimeError("first failure"),
        )
        save_medical_error(
            brief.pk,
            brief.title,
            "second prompt",
            "test-model",
            RuntimeError("second failure"),
        )

        errors = GeneratedMedicalNews.objects.filter(
            brief=brief,
            status=MedicalNewsStatus.ERROR,
        )

        self.assertEqual(errors.count(), 1)
        self.assertEqual(
            errors.get().llm_error,
            "second failure",
        )

    def test_automotive_retries_update_one_error_row(self):
        brief = AutomotiveBrief.objects.create(
            title="Автомобильная новость",
            source_urls="https://example.com/source",
        )

        save_automotive_error(
            brief=brief,
            prompt="first prompt",
            model="test-model",
            error=RuntimeError("first failure"),
        )
        save_automotive_error(
            brief=brief,
            prompt="second prompt",
            model="test-model",
            error=RuntimeError("second failure"),
        )

        errors = GeneratedAutomotiveNews.objects.filter(
            brief=brief,
            status=AutomotiveNewsStatus.ERROR,
        )

        self.assertEqual(errors.count(), 1)
        self.assertEqual(
            errors.get().llm_error,
            "second failure",
        )
