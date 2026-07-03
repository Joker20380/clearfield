from django.db import models
from django.utils import timezone


# =============================================================================
# CLASSIFIERS
# =============================================================================

class Region(models.TextChoices):
    EU = "EU", "Europe"
    WORLD = "WORLD", "World"
    RU = "RU", "Russia"


class Topic(models.TextChoices):
    ECONOMY = "economy", "Economy"
    POLITICS = "politics", "Politics"
    IT = "it", "IT"
    AUTO = "auto", "Auto"

    # Medical content pipeline
    MEDICINE = "medicine", "Medicine"
    LABS = "labs", "Laboratory diagnostics"


class SourceClass(models.TextChoices):
    AGENCY = "agency", "Agency"
    OFFICIAL = "official", "Official"
    STATS = "stats", "Stats"
    INDUSTRY = "industry", "Industry"
    COMMENTARY = "commentary", "Commentary"


class Cadence(models.TextChoices):
    HOT = "hot", "Hot (5–15m)"
    MEDIUM = "medium", "Medium (1–3h)"
    COLD = "cold", "Cold (6–24h)"


# =============================================================================
# SOURCE INGESTION
# =============================================================================

class Source(models.Model):
    name = models.CharField(max_length=200)
    url = models.URLField(unique=True)

    region = models.CharField(max_length=10, choices=Region.choices)
    topic = models.CharField(max_length=20, choices=Topic.choices)
    source_class = models.CharField(max_length=20, choices=SourceClass.choices)
    cadence = models.CharField(max_length=10, choices=Cadence.choices, default=Cadence.MEDIUM)

    is_enabled = models.BooleanField(default=True)
    last_fetch_at = models.DateTimeField(null=True, blank=True)

    etag = models.CharField(max_length=300, null=True, blank=True)
    last_modified = models.CharField(max_length=300, null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["region", "topic", "name"]
        indexes = [
            models.Index(fields=["region", "topic", "is_enabled"]),
            models.Index(fields=["cadence", "is_enabled"]),
        ]

    def __str__(self) -> str:
        return self.name


class FetchLog(models.Model):
    source = models.ForeignKey(Source, on_delete=models.CASCADE, related_name="fetch_logs")
    fetched_at = models.DateTimeField(default=timezone.now)

    status_code = models.IntegerField(null=True, blank=True)
    elapsed_ms = models.IntegerField(null=True, blank=True)
    bytes_received = models.IntegerField(null=True, blank=True)

    error = models.TextField(null=True, blank=True)

    class Meta:
        ordering = ["-fetched_at"]
        indexes = [
            models.Index(fields=["source", "-fetched_at"]),
            models.Index(fields=["status_code", "-fetched_at"]),
        ]

    def __str__(self) -> str:
        return f"{self.source_id} {self.status_code} {self.fetched_at:%Y-%m-%d %H:%M}"


class RawItem(models.Model):
    source = models.ForeignKey(Source, on_delete=models.CASCADE, related_name="items")

    guid = models.CharField(max_length=500, blank=True)
    url = models.URLField()
    title = models.TextField(blank=True)
    summary = models.TextField(blank=True)
    published_at = models.DateTimeField(null=True, blank=True)

    # Дедупликация внутри конкретного источника.
    item_hash = models.CharField(max_length=64, db_index=True)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-published_at", "-created_at"]
        constraints = [
            models.UniqueConstraint(
                fields=["source", "item_hash"],
                name="uniq_source_itemhash",
            )
        ]
        indexes = [
            models.Index(fields=["source", "-published_at"]),
            models.Index(fields=["created_at"]),
        ]

    def __str__(self) -> str:
        return (self.title or self.url or f"RawItem #{self.pk}")[:120]


class Article(models.Model):
    item = models.OneToOneField(RawItem, on_delete=models.CASCADE, related_name="article")

    final_url = models.URLField(blank=True)
    lang = models.CharField(max_length=12, blank=True)

    title = models.TextField(blank=True)
    text = models.TextField(blank=True)

    extracted_at = models.DateTimeField(null=True, blank=True)
    extract_error = models.TextField(blank=True)

    class Meta:
        ordering = ["-extracted_at"]
        indexes = [
            models.Index(fields=["lang"]),
            models.Index(fields=["extracted_at"]),
        ]

    def __str__(self) -> str:
        return f"Article for item {self.item_id}"


# =============================================================================
# EVENT LAYER
# =============================================================================

class Event(models.Model):
    EVIDENCE_CHOICES = [
        (0, "0: anonymous/insider"),
        (1, "1: media reprints"),
        (2, "2: has primary source"),
        (3, "3: multi-class confirmation"),
    ]

    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    # Стабильная витрина события.
    title = models.TextField(blank=True)
    summary = models.TextField(blank=True)

    # Классификация события.
    region = models.CharField(max_length=16, blank=True, db_index=True)
    topic = models.CharField(max_length=32, blank=True, db_index=True)

    evidence_level = models.IntegerField(choices=EVIDENCE_CHOICES, default=1)

    # Ключ склейки/дедупликации событий.
    cluster_key = models.CharField(max_length=64, db_index=True, unique=True)

    class Meta:
        ordering = ["-updated_at"]
        indexes = [
            models.Index(fields=["topic", "region", "-updated_at"]),
            models.Index(fields=["evidence_level", "-updated_at"]),
        ]

    def __str__(self):
        return f"Event #{self.id} L{self.evidence_level}: {(self.title or '')[:80]}"


class EventItem(models.Model):
    event = models.ForeignKey(Event, on_delete=models.CASCADE, related_name="items")
    item = models.OneToOneField("intel.RawItem", on_delete=models.CASCADE, related_name="event_item")

    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        indexes = [
            models.Index(fields=["event", "-created_at"]),
        ]

    def __str__(self):
        return f"EventItem event={self.event_id} item={self.item_id}"


# =============================================================================
# MEDICAL CONTENT PRODUCTION LAYER
# =============================================================================

class MedicalBriefStatus(models.TextChoices):
    DRAFT = "draft", "Draft"
    READY = "ready", "Ready"
    USED = "used", "Used"
    REJECTED = "rejected", "Rejected"


class MedicalNewsStatus(models.TextChoices):
    DRAFT = "draft", "Draft"
    REVIEW = "review", "Review"
    APPROVED = "approved", "Approved"
    PUBLISHED = "published", "Published"
    REJECTED = "rejected", "Rejected"
    ERROR = "error", "Error"


class MedicalBrief(models.Model):
    """
    Редакционное задание для генерации медицинской новости.

    Это промежуточный слой между Event и LLM.
    Важно: LLM должна писать не напрямую из Event, а из контролируемого brief.
    """

    event = models.ForeignKey(
        Event,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="medical_briefs",
    )

    title = models.TextField()
    angle = models.TextField(
        blank=True,
        help_text="Редакционный угол: как именно подать событие для пациентов.",
    )

    target_keyword = models.CharField(
        max_length=300,
        blank=True,
        help_text="Главная SEO-фраза.",
    )
    secondary_keywords = models.TextField(
        blank=True,
        help_text="Дополнительные ключевые фразы, по одной на строку.",
    )

    facts = models.TextField(
        blank=True,
        help_text="Подтверждённые факты, которые можно использовать в тексте.",
    )
    source_urls = models.TextField(
        blank=True,
        help_text="URL источников, по одному на строку.",
    )

    audience = models.CharField(
        max_length=200,
        default="пациенты медицинской лаборатории",
    )
    region_text = models.CharField(
        max_length=200,
        default="Владикавказ и Северная Осетия",
    )

    safety_notes = models.TextField(
        blank=True,
        help_text="Ограничения: что нельзя утверждать, обещать или рекомендовать.",
    )
    disclaimer_required = models.BooleanField(default=True)

    status = models.CharField(
        max_length=20,
        choices=MedicalBriefStatus.choices,
        default=MedicalBriefStatus.DRAFT,
        db_index=True,
    )

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    used_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["status", "-created_at"]),
            models.Index(fields=["event", "status"]),
        ]

    def __str__(self):
        return (self.title or f"MedicalBrief #{self.pk}")[:120]


class GeneratedMedicalNews(models.Model):
    """
    Черновик или готовая медицинская новость, созданная на основе MedicalBrief.

    На первом этапе НЕ публикуем автоматически.
    Сначала статус review, затем ручное approve.
    """

    brief = models.ForeignKey(
        MedicalBrief,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="generated_news",
    )

    title = models.CharField(max_length=300)
    slug = models.SlugField(max_length=255, blank=True, db_index=True)
    meta_description = models.CharField(max_length=320, blank=True)

    body = models.TextField(
        help_text="Основной текст новости. На первом этапе храним Markdown.",
    )

    source_note = models.TextField(
        blank=True,
        help_text="Краткое примечание об источниках и основании материала.",
    )
    image_topic = models.CharField(
        max_length=64,
        blank=True,
        db_index=True,
        help_text=(
            "Закрытый ключ визуальной темы, выбранный LLM "
            "для сопоставления с изображением."
        ),
    )

    disclaimer = models.TextField(
        default=(
            "Материал носит информационный характер и не заменяет консультацию врача. "
            "Интерпретацию результатов анализов должен проводить специалист с учётом "
            "жалоб, анамнеза и других данных пациента."
        ),
    )

    quality_score = models.IntegerField(
        default=0,
        help_text="Внутренняя оценка качества/готовности материала.",
    )

    status = models.CharField(
        max_length=20,
        choices=MedicalNewsStatus.choices,
        default=MedicalNewsStatus.REVIEW,
        db_index=True,
    )

    # LLM traceability
    llm_model = models.CharField(max_length=120, blank=True)
    llm_prompt = models.TextField(blank=True)
    llm_response_raw = models.TextField(blank=True)
    llm_elapsed_ms = models.IntegerField(null=True, blank=True)
    llm_error = models.TextField(blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    published_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["status", "-created_at"]),
            models.Index(fields=["slug"]),
            models.Index(fields=["published_at"]),
        ]

    def __str__(self):
        return self.title[:120]