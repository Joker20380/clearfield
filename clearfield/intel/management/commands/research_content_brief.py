from __future__ import annotations

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from intel.content_research import (
    build_claim_extraction_prompt,
    extract_research_document,
    fetch_public_html,
    verified_claims_from_response,
)
from intel.llm.ollama_client import generate_with_ollama
from intel.models import ContentBrief, ContentSource
from intel.universal_content_engine import validate_evidence_pack


class Command(BaseCommand):
    help = "Собирает проверяемый evidence-пакет для универсального brief."

    def add_arguments(self, parser):
        parser.add_argument("--brief-id", type=int, required=True)
        parser.add_argument("--source-ids", required=True)
        parser.add_argument("--model", default="")
        parser.add_argument("--apply", action="store_true")

    def handle(self, *args, **options):
        try:
            brief = ContentBrief.objects.select_related(
                "project", "template"
            ).get(pk=options["brief_id"])
        except ContentBrief.DoesNotExist as exc:
            raise CommandError("ContentBrief не найден") from exc

        try:
            source_ids = {
                int(token)
                for token in options["source_ids"].replace(",", " ").split()
            }
        except ValueError as exc:
            raise CommandError("Некорректный --source-ids") from exc

        sources = list(
            ContentSource.objects.filter(
                id__in=source_ids,
                project=brief.project,
                is_enabled=True,
            ).order_by("-trust_level", "id")
        )
        if len(sources) != len(source_ids):
            raise CommandError(
                "Часть источников не найдена, отключена или относится "
                "к другому проекту."
            )

        policy = (
            brief.project.policy
            if isinstance(brief.project.policy, dict)
            else {}
        )
        allowed_domains = policy.get("allowed_source_domains") or []
        evidence = []

        for source in sources:
            self.stdout.write(f"FETCH #{source.pk}: {source.url}")
            try:
                final_url, html = fetch_public_html(
                    source.url,
                    allowed_domains,
                )
                document = extract_research_document(final_url, html)
                result = generate_with_ollama(
                    prompt=build_claim_extraction_prompt(
                        brief,
                        source,
                        document,
                    ),
                    system=(
                        "Ты извлекаешь факты только из переданного документа. "
                        "Не исправляй и не выдумывай цитаты."
                    ),
                    json_mode=True,
                    model=options["model"] or settings.OLLAMA_MODEL,
                )
                claims = verified_claims_from_response(
                    result.text,
                    document["text"],
                )
                self.stdout.write(
                    f"VERIFIED #{source.pk}: {len(claims)} claims"
                )

                for claim in claims:
                    evidence.append(
                        {
                            "id": f"E{len(evidence) + 1}",
                            "claim": claim["claim"],
                            "source_quote": claim["source_quote"],
                            "source_url": document["url"],
                            "source_title": document["title"] or source.name,
                            "source_sha256": document["sha256"],
                            "fetched_at": timezone.now().isoformat(),
                        }
                    )

                if options["apply"]:
                    source.last_title = document["title"]
                    source.last_text = document["text"]
                    source.content_sha256 = document["sha256"]
                    source.fetched_at = timezone.now()
                    source.fetch_error = ""
                    source.save()
            except Exception as exc:
                if options["apply"]:
                    source.fetch_error = str(exc)[:2000]
                    source.save(update_fields=["fetch_error", "updated_at"])
                self.stderr.write(self.style.ERROR(f"ERROR #{source.pk}: {exc}"))

        original_pack = brief.evidence_pack
        brief.evidence_pack = evidence
        errors = validate_evidence_pack(brief)
        brief.evidence_pack = original_pack

        self.stdout.write(
            f"Evidence: {len(evidence)}; validation: "
            f"{', '.join(errors) if errors else 'OK'}"
        )
        if options["apply"] and evidence:
            brief.evidence_pack = evidence
            brief.status = "ready" if not errors else "draft"
            brief.save(
                update_fields=["evidence_pack", "status", "updated_at"]
            )
            self.stdout.write(
                self.style.SUCCESS(
                    f"Saved; brief status={brief.status}"
                )
            )
