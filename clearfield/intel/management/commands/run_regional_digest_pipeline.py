import os
import urllib.error
import urllib.request
from datetime import timedelta

from django.core.management import call_command
from django.core.management.base import (
    BaseCommand,
    CommandError,
)
from django.utils import timezone

from intel.models import (
    RegionalDigest,
    RegionalDigestStatus,
)


def check_model_health() -> str:
    """
    Проверяет доступность OpenAI-compatible llama.cpp
    endpoint до запуска долгого pipeline.
    """

    base_url = str(
        os.getenv("OLLAMA_BASE_URL") or ""
    ).strip().rstrip("/")

    if not base_url:
        raise CommandError(
            "OLLAMA_BASE_URL is not configured"
        )

    errors = []

    for suffix in (
        "/health",
        "/v1/models",
    ):
        url = base_url + suffix

        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": (
                    "clearfield-regional-digest/1.0"
                ),
            },
            method="GET",
        )

        try:
            with urllib.request.urlopen(
                request,
                timeout=15,
            ) as response:
                status = int(
                    getattr(response, "status", 200)
                )

                if 200 <= status < 300:
                    return url

                errors.append(
                    f"{url}: HTTP {status}"
                )

        except (
            urllib.error.URLError,
            urllib.error.HTTPError,
            TimeoutError,
            OSError,
        ) as exc:
            errors.append(
                f"{url}: {exc}"
            )

    raise CommandError(
        "LLM endpoint is unavailable: "
        + " | ".join(errors)
    )


class Command(BaseCommand):
    help = (
        "Автоматически выполняет полный региональный "
        "digest pipeline: semantic screening, grounded "
        "fact extraction и composition. Публикацию "
        "не выполняет."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--region",
            default="north_ossetia",
        )
        parser.add_argument(
            "--region-label",
            default="Северная Осетия",
        )
        parser.add_argument(
            "--region-query",
            default=(
                "рсо-алания,рсо алания,"
                "северная осетия,северной осетии,"
                "владикавказ"
            ),
        )
        parser.add_argument(
            "--topic",
            default="medicine",
        )
        parser.add_argument(
            "--days",
            type=int,
            default=90,
        )
        parser.add_argument(
            "--min-events",
            type=int,
            default=1,
        )
        parser.add_argument(
            "--max-events",
            type=int,
            default=6,
        )
        parser.add_argument(
            "--max-candidates",
            type=int,
            default=30,
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=10,
        )
        parser.add_argument(
            "--fact-retries",
            type=int,
            default=2,
        )
        parser.add_argument(
            "--compose-retries",
            type=int,
            default=3,
        )
        parser.add_argument(
            "--model",
            default="",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help=(
                "Запустить новый цикл, даже если за "
                "сегодня уже создан review/published-дайджест."
            ),
        )
        parser.add_argument(
            "--skip-model-health-check",
            action="store_true",
            help=(
                "Пропустить предварительную проверку "
                "доступности LLM endpoint."
            ),
        )

    def handle(self, *args, **options):
        region = str(options["region"]).strip()
        region_label = str(
            options["region_label"]
        ).strip()
        region_query = str(
            options["region_query"]
        ).strip()
        topic = str(options["topic"]).strip()
        model = str(options["model"]).strip()

        days = max(1, int(options["days"]))
        min_events = max(
            1,
            int(options["min_events"]),
        )
        max_events = max(
            min_events,
            int(options["max_events"]),
        )
        max_candidates = max(
            max_events,
            int(options["max_candidates"]),
        )
        batch_size = max(
            1,
            int(options["batch_size"]),
        )
        fact_retries = max(
            1,
            int(options["fact_retries"]),
        )
        compose_retries = max(
            1,
            int(options["compose_retries"]),
        )

        now = timezone.now()
        day_start = now.replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )

        if not options["force"]:
            existing_digest = (
                RegionalDigest.objects
                .filter(
                    region_label=region_label,
                    topic=topic,
                    status__in=(
                        RegionalDigestStatus.REVIEW,
                        RegionalDigestStatus.PUBLISHED,
                    ),
                    updated_at__gte=day_start,
                )
                .exclude(body="")
                .order_by("-updated_at", "-id")
                .first()
            )

            if existing_digest is not None:
                self.stdout.write(
                    self.style.WARNING(
                        "Pipeline skipped: a reviewed "
                        "digest already exists today."
                    )
                )
                self.stdout.write(
                    f"RegionalDigest #{existing_digest.id}"
                )
                self.stdout.write(
                    f"Updated: "
                    f"{existing_digest.updated_at.isoformat()}"
                )
                self.stdout.write(
                    f"Body chars: "
                    f"{len(existing_digest.body or '')}"
                )
                return

        if not options[
            "skip_model_health_check"
        ]:
            self.stdout.write(
                "=== MODEL HEALTH CHECK ==="
            )

            health_url = check_model_health()

            self.stdout.write(
                self.style.SUCCESS(
                    f"LLM endpoint available: "
                    f"{health_url}"
                )
            )

            self.stdout.write("")

        pipeline_started_at = timezone.now()

        self.stdout.write(
            "=== STAGE 1: SEMANTIC SCREENING ==="
        )

        screen_options = {
            "region": region,
            "region_label": region_label,
            "region_query": region_query,
            "topic": topic,
            "days": days,
            "min_events": min_events,
            "max_events": max_events,
            "max_candidates": max_candidates,
            "batch_size": batch_size,
            "max_screen_prompt_chars": 15000,
            "max_selection_prompt_chars": 16000,
            "min_medical_focus": 0.70,
            "min_regional_relevance": 0.70,
            "min_source_sufficiency": 0.55,
            "duplicate_threshold": 0.72,
            "execute": True,
            "show_decisions": True,
        }

        if model:
            screen_options["model"] = model

        call_command(
            "screen_regional_digest_candidates",
            **screen_options,
        )

        digest = (
            RegionalDigest.objects
            .filter(
                region_label=region_label,
                topic=topic,
                updated_at__gte=(
                    pipeline_started_at
                    - timedelta(minutes=2)
                ),
            )
            .order_by("-updated_at", "-id")
            .first()
        )

        if digest is None:
            raise CommandError(
                "Semantic screening completed, but "
                "RegionalDigest was not found."
            )

        if digest.status != RegionalDigestStatus.READY:
            raise CommandError(
                "Digest is not ready after screening: "
                f"#{digest.id}, status={digest.status}"
            )

        if digest.body:
            raise CommandError(
                "Semantic screening selected a digest "
                "that already contains generated body."
            )

        criteria = (
            dict(digest.criteria)
            if isinstance(digest.criteria, dict)
            else {}
        )

        selected_event_ids = sorted(
            set(
                digest.digest_items.values_list(
                    "event_id",
                    flat=True,
                )
            )
        )

        if not selected_event_ids:
            raise CommandError(
                "Semantic screening created a digest "
                "without selected events."
            )

        criteria_changed = False

        if (
            criteria.get(
                "semantic_screening_confirmed"
            )
            is not True
        ):
            criteria[
                "semantic_screening_confirmed"
            ] = True
            criteria_changed = True

        if (
            criteria.get("pipeline")
            != "semantic-regional-screen-v1-confirmed"
        ):
            criteria["pipeline"] = (
                "semantic-regional-screen-v1-confirmed"
            )
            criteria_changed = True

        if (
            criteria.get("selected_event_ids")
            != selected_event_ids
        ):
            criteria["selected_event_ids"] = (
                selected_event_ids
            )
            criteria_changed = True

        if criteria_changed:
            digest.criteria = criteria
            digest.save(
                update_fields=[
                    "criteria",
                    "updated_at",
                ]
            )

        current_event_ids = set(
            selected_event_ids
        )

        previous_digests = (
            RegionalDigest.objects
            .filter(
                region_label=region_label,
                topic=topic,
                status=(
                    RegionalDigestStatus.PUBLISHED
                ),
            )
            .exclude(id=digest.id)
            .prefetch_related("digest_items")
        )

        for previous_digest in previous_digests:
            previous_event_ids = set(
                previous_digest
                .digest_items
                .values_list(
                    "event_id",
                    flat=True,
                )
            )

            if (
                current_event_ids
                != previous_event_ids
            ):
                continue

            digest.status = (
                RegionalDigestStatus.REJECTED
            )
            digest.llm_error = (
                "Exact duplicate event set of "
                f"RegionalDigest "
                f"#{previous_digest.id}."
            )

            digest.save(
                update_fields=[
                    "status",
                    "llm_error",
                    "updated_at",
                ]
            )

            raise CommandError(
                "Duplicate regional digest blocked: "
                f"RegionalDigest #{digest.id} "
                f"repeats published "
                f"RegionalDigest "
                f"#{previous_digest.id}."
            )

        self.stdout.write(
            self.style.SUCCESS(
                f"Semantic screening created "
                f"RegionalDigest #{digest.id}"
            )
        )

        self.stdout.write("")
        self.stdout.write(
            "=== STAGE 2: GROUNDED FACT EXTRACTION ==="
        )

        fact_success = False
        fact_errors = []

        for attempt in range(
            1,
            fact_retries + 1,
        ):
            self.stdout.write(
                f"Fact extraction attempt "
                f"{attempt}/{fact_retries}"
            )

            try:
                fact_options = {
                    "digest_id": digest.id,
                    "execute": True,
                    "facts_only": True,
                }

                if model:
                    fact_options["model"] = model

                call_command(
                    "generate_regional_digest",
                    **fact_options,
                )

                digest.refresh_from_db()

                fact_pack = (
                    digest.source_map or {}
                ).get("grounded_fact_pack")

                facts = (
                    fact_pack.get("facts")
                    if isinstance(fact_pack, dict)
                    else None
                )

                if (
                    digest.status
                    == RegionalDigestStatus.READY
                    and isinstance(facts, list)
                    and len(facts) >= min_events * 2
                ):
                    fact_success = True
                    break

                raise CommandError(
                    "Fact extraction command completed "
                    "without a valid saved fact-pack."
                )

            except Exception as exc:
                fact_errors.append(str(exc))
                digest.refresh_from_db()

                self.stderr.write(
                    self.style.WARNING(
                        f"Fact extraction attempt "
                        f"{attempt} failed: {exc}"
                    )
                )

        if not fact_success:
            raise CommandError(
                "Fact extraction failed after retries: "
                + " | ".join(fact_errors)
            )

        self.stdout.write(
            self.style.SUCCESS(
                "Grounded fact-pack saved."
            )
        )

        self.stdout.write("")
        self.stdout.write(
            "=== STAGE 3: DIGEST COMPOSITION ==="
        )

        composition_success = False
        composition_errors = []
        retry_feedback = ""

        for attempt in range(
            1,
            compose_retries + 1,
        ):
            self.stdout.write(
                f"Composition attempt "
                f"{attempt}/{compose_retries}"
            )

            digest.refresh_from_db()

            try:
                compose_options = {
                    "digest_id": digest.id,
                    "execute": True,
                    "compose_only": True,
                    "retry_feedback": retry_feedback,
                }

                if (
                    digest.status
                    == RegionalDigestStatus.REVIEW
                    and digest.body
                ):
                    compose_options[
                        "replace_review"
                    ] = True

                if model:
                    compose_options["model"] = model

                call_command(
                    "generate_regional_digest",
                    **compose_options,
                )

                digest.refresh_from_db()

                if (
                    digest.status
                    == RegionalDigestStatus.REVIEW
                    and bool(digest.body)
                    and digest.published_at is None
                ):
                    composition_success = True
                    break

                raise CommandError(
                    "Composition completed without "
                    "a valid review digest."
                )

            except Exception as exc:
                error_text = str(exc)
                composition_errors.append(error_text)

                retry_feedback = (
                    "Предыдущая попытка отклонена валидатором: "
                    + error_text[:1800]
                    + "\nИсправь именно эту ошибку. "
                    + "Не копируй значения-примеры из JSON-схемы "
                    + "и не добавляй сведения вне fact_ids."
                )

                digest.refresh_from_db()

                self.stderr.write(
                    self.style.WARNING(
                        f"Composition attempt "
                        f"{attempt} failed: {exc}"
                    )
                )

        if not composition_success:
            raise CommandError(
                "Composition failed after retries: "
                + " | ".join(composition_errors)
            )

        source_map = (
            digest.source_map
            if isinstance(digest.source_map, dict)
            else {}
        )

        facts = (
            source_map
            .get("grounded_fact_pack", {})
            .get("facts", [])
        )

        blocks = source_map.get("blocks") or []

        self.stdout.write("")
        self.stdout.write(
            self.style.SUCCESS(
                "=== REGIONAL DIGEST PIPELINE COMPLETED ==="
            )
        )
        self.stdout.write(
            f"Digest ID: {digest.id}"
        )
        self.stdout.write(
            f"Status: {digest.status}"
        )
        self.stdout.write(
            f"Title: {digest.title}"
        )
        self.stdout.write(
            f"Events: {digest.digest_items.count()}"
        )
        self.stdout.write(
            f"Facts: {len(facts)}"
        )
        self.stdout.write(
            f"Blocks: {len(blocks)}"
        )
        self.stdout.write(
            f"Body chars: {len(digest.body or '')}"
        )
        self.stdout.write(
            f"Published at: {digest.published_at}"
        )

        if digest.published_at is not None:
            raise CommandError(
                "Safety violation: pipeline must not "
                "publish the digest automatically."
            )
