from __future__ import annotations

import json

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone

from intel.llm.ollama_client import generate_with_ollama
from intel.models import (
    ContentBrief,
    ContentBriefStatus,
    GeneratedContent,
    GeneratedContentStatus,
)
from intel.universal_content_engine import (
    build_factual_verification_prompt,
    build_universal_prompt,
    parse_factual_verification,
    parse_and_audit_generated_content,
    validate_evidence_pack,
)


class Command(BaseCommand):
    help = "Генерирует материалы универсального контентного движка."

    def add_arguments(self, parser):
        parser.add_argument("--project", default="")
        parser.add_argument("--brief-ids", default="")
        parser.add_argument("--limit", type=int, default=1)
        parser.add_argument("--model", default="")
        parser.add_argument("--llm-timeout", type=int, default=0)
        parser.add_argument("--llm-retries", type=int, default=0)
        parser.add_argument("--generation-max-tokens", type=int, default=2200)
        parser.add_argument("--verification-max-tokens", type=int, default=900)
        parser.add_argument("--force", action="store_true")
        parser.add_argument("--dry-run", action="store_true")

    def handle(self, *args, **options):
        ids = []
        for token in str(options["brief_ids"] or "").replace(",", " ").split():
            try:
                ids.append(int(token))
            except ValueError as exc:
                raise CommandError(f"Некорректный ID: {token}") from exc

        queryset = (
            ContentBrief.objects
            .filter(
                status=ContentBriefStatus.READY,
                project__is_enabled=True,
                template__is_enabled=True,
            )
            .select_related("project", "template")
            .order_by("created_at", "id")
        )
        if options["project"]:
            queryset = queryset.filter(project__key=options["project"])
        if ids:
            queryset = queryset.filter(id__in=ids)
        if not options["force"]:
            queryset = queryset.exclude(
                generated_items__status__in=[
                    GeneratedContentStatus.REVIEW,
                    GeneratedContentStatus.APPROVED,
                    GeneratedContentStatus.PUBLISHED,
                ]
            )

        briefs = list(queryset.distinct()[: max(options["limit"], 1)])
        created = 0
        failed = 0

        for brief in briefs:
            evidence_errors = validate_evidence_pack(brief)
            if evidence_errors:
                failed += 1
                self.stdout.write(
                    self.style.WARNING(
                        f"SKIP #{brief.pk}: {', '.join(evidence_errors)}"
                    )
                )
                continue

            system, prompt = build_universal_prompt(brief)
            if options["dry_run"]:
                self.stdout.write(f"\n--- Brief #{brief.pk} ---\n{prompt}")
                continue

            try:
                result = generate_with_ollama(
                    prompt=prompt,
                    system=system,
                    json_mode=True,
                    model=options["model"] or settings.OLLAMA_MODEL,
                    timeout_seconds=options["llm_timeout"] or None,
                    max_tokens=max(options["generation_max_tokens"], 500),
                    retries=max(options["llm_retries"], 0),
                )
                payload = parse_and_audit_generated_content(brief, result.text)
                policy = (
                    brief.project.policy
                    if isinstance(brief.project.policy, dict)
                    else {}
                )
                verification_required = (
                    policy.get("risk_level", "medium") != "low"
                )

                if verification_required:
                    verification_result = generate_with_ollama(
                        prompt=build_factual_verification_prompt(
                            brief,
                            payload["body"],
                        ),
                        system=(
                            "Ты выполняешь только консервативную "
                            "проверку фактической опоры текста."
                        ),
                        json_mode=True,
                        model=options["model"] or settings.OLLAMA_MODEL,
                        timeout_seconds=options["llm_timeout"] or None,
                        max_tokens=max(
                            options["verification_max_tokens"],
                            300,
                        ),
                        retries=max(options["llm_retries"], 0),
                    )
                    verification = parse_factual_verification(
                        verification_result.text
                    )
                    payload["qa_report"]["factual_verification"] = (
                        verification
                    )
                    if not verification["supported"]:
                        payload["qa_report"]["hard_issues"].append(
                            "llm-factual-verification-failed"
                        )
                        payload["qa_report"]["issues"].append(
                            "llm-factual-verification-failed"
                        )
                        payload["quality_score"] = max(
                            0,
                            payload["quality_score"] - 25,
                        )

                status = (
                    GeneratedContentStatus.ERROR
                    if payload["qa_report"]["hard_issues"]
                    else GeneratedContentStatus.REVIEW
                )

                with transaction.atomic():
                    item = GeneratedContent.objects.create(
                        brief=brief,
                        status=status,
                        llm_model=result.model,
                        llm_prompt=prompt,
                        llm_response_raw=json.dumps(
                            result.raw,
                            ensure_ascii=False,
                            indent=2,
                        ),
                        llm_elapsed_ms=result.elapsed_ms,
                        **payload,
                    )
                    if status == GeneratedContentStatus.REVIEW:
                        brief.status = ContentBriefStatus.USED
                        brief.used_at = timezone.now()
                        brief.save(update_fields=["status", "used_at", "updated_at"])

                created += 1
                self.stdout.write(
                    self.style.SUCCESS(
                        f"#{item.pk} {status} score={item.quality_score}: "
                        f"{item.title[:100]}"
                    )
                )
            except Exception as exc:
                failed += 1
                self.stderr.write(self.style.ERROR(f"ERROR #{brief.pk}: {exc}"))

        self.stdout.write(
            f"Briefs: {len(briefs)}; created: {created}; failed: {failed}"
        )
