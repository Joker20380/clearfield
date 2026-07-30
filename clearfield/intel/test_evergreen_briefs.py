from django.core.management import call_command
from django.test import TestCase

from intel.evergreen_catalog import AUTOMOTIVE_TOPICS, MEDICAL_TOPICS
from intel.models import AutomotiveBrief, MedicalBrief


class EvergreenBriefCommandTests(TestCase):
    def test_dry_run_does_not_write(self):
        call_command("seed_evergreen_briefs")

        self.assertEqual(MedicalBrief.objects.count(), 0)
        self.assertEqual(AutomotiveBrief.objects.count(), 0)

    def test_apply_is_idempotent_and_has_no_events(self):
        call_command("seed_evergreen_briefs", "--apply", "--status=ready")
        call_command("seed_evergreen_briefs", "--apply", "--status=ready")

        self.assertEqual(MedicalBrief.objects.count(), len(MEDICAL_TOPICS))
        self.assertEqual(
            AutomotiveBrief.objects.count(),
            len(AUTOMOTIVE_TOPICS),
        )
        self.assertFalse(MedicalBrief.objects.exclude(event=None).exists())
        self.assertFalse(AutomotiveBrief.objects.exclude(event=None).exists())
        self.assertFalse(MedicalBrief.objects.exclude(status="ready").exists())
        self.assertFalse(
            AutomotiveBrief.objects.exclude(status="ready").exists()
        )
