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

    # Детерминированная SEO-привязка к реальной посадочной
    # странице анализа в каталоге КДЛ «Дзагуров».
    semantic_panel_id = models.PositiveBigIntegerField(
        "ID панели КДЛ «Дзагуров»",
        null=True,
        blank=True,
        db_index=True,
        help_text="ID панели в каталоге КДЛ «Дзагуров».",
    )
    semantic_panel_code = models.CharField(
        "Код анализа КДЛ «Дзагуров»",
        max_length=64,
        blank=True,
        help_text="Код анализа в каталоге КДЛ «Дзагуров».",
    )
    semantic_panel_title = models.TextField(
        "Название карточки анализа",
        blank=True,
        help_text="Название выбранной карточки анализа.",
    )
    semantic_panel_url = models.URLField(
        "URL карточки анализа",
        max_length=500,
        blank=True,
        help_text="Абсолютный URL выбранной карточки анализа.",
    )
    semantic_anchor = models.CharField(
        "Анкор внутренней ссылки",
        max_length=500,
        blank=True,
        help_text="Анкор внутренней ссылки на карточку анализа.",
    )
    semantic_score = models.IntegerField(
        "Оценка семантического совпадения",
        default=0,
        help_text="Итоговая оценка релевантности выбранной карточки.",
    )
    semantic_match_details = models.JSONField(
        "Детали семантического совпадения",
        default=dict,
        blank=True,
        help_text="Детали детерминированного сопоставления.",
    )
    semantic_feed_sha256 = models.CharField(
        "SHA256 семантического каталога",
        max_length=64,
        blank=True,
        help_text="SHA256 semantic feed, по которому выполнен подбор.",
    )
    semantic_assigned_at = models.DateTimeField(
        "Дата назначения посадочной страницы",
        null=True,
        blank=True,
        help_text="Дата последнего назначения посадочной страницы.",
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


# === REGIONAL DIGEST MODELS ===

class RegionalDigestStatus(models.TextChoices):
    DRAFT = "draft", "Draft"
    READY = "ready", "Ready"
    REVIEW = "review", "Review"
    PUBLISHED = "published", "Published"
    REJECTED = "rejected", "Rejected"
    ERROR = "error", "Error"


class RegionalDigest(models.Model):
    """
    Сводный материал, объединяющий несколько разных событий
    по региону, тематике и временному интервалу.
    """

    group_key = models.CharField(
        max_length=64,
        unique=True,
        db_index=True,
    )

    digest_type = models.CharField(
        max_length=32,
        default="regional",
        db_index=True,
    )

    region_code = models.CharField(
        max_length=32,
        blank=True,
        db_index=True,
    )

    region_label = models.CharField(
        max_length=200,
        blank=True,
    )

    region_query = models.TextField(
        blank=True,
        help_text=(
            "Маркеры региона через запятую, например: "
            "рсо-алания, северная осетия, владикавказ"
        ),
    )

    topic = models.CharField(
        max_length=32,
        blank=True,
        db_index=True,
    )

    period_start = models.DateTimeField(
        db_index=True,
    )

    period_end = models.DateTimeField(
        db_index=True,
    )

    criteria = models.JSONField(
        default=dict,
        blank=True,
    )

    evidence_pack = models.JSONField(
        default=dict,
        blank=True,
    )

    title = models.CharField(
        max_length=300,
        blank=True,
    )

    slug = models.SlugField(
        max_length=320,
        blank=True,
    )

    meta_description = models.TextField(
        blank=True,
    )

    body = models.TextField(
        blank=True,
    )

    source_map = models.JSONField(
        default=dict,
        blank=True,
    )

    status = models.CharField(
        max_length=20,
        choices=RegionalDigestStatus.choices,
        default=RegionalDigestStatus.DRAFT,
        db_index=True,
    )

    llm_model = models.CharField(
        max_length=500,
        blank=True,
    )

    llm_prompt = models.TextField(
        blank=True,
    )

    llm_response_raw = models.TextField(
        blank=True,
    )

    llm_elapsed_ms = models.PositiveIntegerField(
        null=True,
        blank=True,
    )

    llm_error = models.TextField(
        blank=True,
    )

    published_at = models.DateTimeField(
        null=True,
        blank=True,
    )

    created_at = models.DateTimeField(
        auto_now_add=True,
    )

    updated_at = models.DateTimeField(
        auto_now=True,
    )

    class Meta:
        ordering = ["-period_end", "-id"]
        indexes = [
            models.Index(
                fields=[
                    "region_code",
                    "topic",
                    "-period_end",
                ],
                name="intel_rd_region_topic_idx",
            ),
            models.Index(
                fields=["status", "-created_at"],
                name="intel_rd_status_created_idx",
            ),
        ]

    def __str__(self):
        label = self.region_label or self.region_code or "digest"
        return f"RegionalDigest #{self.pk}: {label}"


class RegionalDigestItem(models.Model):
    digest = models.ForeignKey(
        RegionalDigest,
        on_delete=models.CASCADE,
        related_name="digest_items",
    )

    event = models.ForeignKey(
        Event,
        on_delete=models.CASCADE,
        related_name="regional_digest_items",
    )

    position = models.PositiveIntegerField(
        default=0,
    )

    relevance_score = models.FloatField(
        default=0,
    )

    selection_reason = models.TextField(
        blank=True,
    )

    created_at = models.DateTimeField(
        auto_now_add=True,
    )

    class Meta:
        ordering = ["position", "id"]
        constraints = [
            models.UniqueConstraint(
                fields=["digest", "event"],
                name="uniq_regional_digest_event",
            ),
        ]
        indexes = [
            models.Index(
                fields=["digest", "position"],
                name="intel_rditem_position_idx",
            ),
        ]

    def __str__(self):
        return (
            f"Событие региональной новости: новость={self.digest_id} "
            f"событие={self.event_id}"
        )



# =============================================================================
# AUTOMOTIVE CONTENT PRODUCTION LAYER
# =============================================================================

class AutomotiveBriefStatus(models.TextChoices):
    DRAFT = "draft", "Draft"
    READY = "ready", "Ready"
    USED = "used", "Used"
    REJECTED = "rejected", "Rejected"


class AutomotiveNewsStatus(models.TextChoices):
    DRAFT = "draft", "Draft"
    REVIEW = "review", "Review"
    APPROVED = "approved", "Approved"
    PUBLISHED = "published", "Published"
    REJECTED = "rejected", "Rejected"
    ERROR = "error", "Error"


class AutomotiveBrief(models.Model):
    """
    Редакционное задание для отдельного автомобильного контура.

    Общими с медицинским контуром остаются только Source, RawItem,
    Event и EventItem.
    """

    event = models.ForeignKey(
        Event,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="automotive_briefs",
    )

    title = models.TextField()

    angle = models.TextField(
        blank=True,
        help_text=(
            "Редакционный угол материала для автовладельцев."
        ),
    )

    target_keyword = models.CharField(
        max_length=300,
        blank=True,
        help_text="Главная SEO-фраза.",
    )

    secondary_keywords = models.TextField(
        blank=True,
        help_text=(
            "Дополнительные SEO-фразы, по одной на строку."
        ),
    )

    facts = models.TextField(
        blank=True,
        help_text=(
            "Подтверждённые факты, разрешённые для генерации."
        ),
    )

    source_urls = models.TextField(
        blank=True,
        help_text=(
            "Ссылки на источники, по одной на строку."
        ),
    )

    audience = models.CharField(
        max_length=200,
        default=(
            "автовладельцы и клиенты автосервиса"
        ),
    )

    region_text = models.CharField(
        max_length=200,
        default="Владикавказ и Северная Осетия",
    )

    safety_notes = models.TextField(
        blank=True,
        help_text=(
            "Ограничения и утверждения, которые нельзя использовать."
        ),
    )

    disclaimer_required = models.BooleanField(
        default=True,
    )

    status = models.CharField(
        max_length=20,
        choices=AutomotiveBriefStatus.choices,
        default=AutomotiveBriefStatus.DRAFT,
        db_index=True,
    )

    created_at = models.DateTimeField(
        auto_now_add=True,
    )

    updated_at = models.DateTimeField(
        auto_now=True,
    )

    used_at = models.DateTimeField(
        null=True,
        blank=True,
    )

    class Meta:
        ordering = [
            "-created_at",
        ]
        indexes = [
            models.Index(
                fields=[
                    "status",
                    "-created_at",
                ],
            ),
            models.Index(
                fields=[
                    "event",
                    "status",
                ],
            ),
        ]

    def __str__(self):
        return (
            self.title
            or f"Автомобильное задание №{self.pk}"
        )[:120]


class GeneratedAutomotiveNews(models.Model):
    """
    Автомобильная публикация, созданная из AutomotiveBrief.

    Экспортируется только в отдельный automotive JSON feed.
    """

    brief = models.ForeignKey(
        AutomotiveBrief,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="generated_news",
    )

    title = models.CharField(
        max_length=300,
    )

    slug = models.SlugField(
        max_length=255,
        blank=True,
        db_index=True,
    )

    meta_description = models.CharField(
        max_length=320,
        blank=True,
    )

    body = models.TextField(
        help_text="Основной текст новости в Markdown.",
    )

    source_note = models.TextField(
        blank=True,
        help_text=(
            "Краткое примечание об источниках материала."
        ),
    )

    source_urls = models.JSONField(
        default=list,
        blank=True,
        help_text=(
            "Нормализованные ссылки на первичные источники."
        ),
    )

    image_topic = models.CharField(
        max_length=64,
        blank=True,
        db_index=True,
        help_text=(
            "Закрытый ключ автомобильной темы изображения."
        ),
    )

    disclaimer = models.TextField(
        default=(
            "Материал носит информационный характер. "
            "Точную причину неисправности и необходимость "
            "ремонта следует определять после диагностики "
            "автомобиля специалистом."
        ),
    )

    quality_score = models.PositiveSmallIntegerField(
        default=0,
        help_text=(
            "Внутренняя оценка качества материала."
        ),
    )

    status = models.CharField(
        max_length=20,
        choices=AutomotiveNewsStatus.choices,
        default=AutomotiveNewsStatus.REVIEW,
        db_index=True,
    )

    llm_model = models.CharField(
        max_length=120,
        blank=True,
    )

    llm_prompt = models.TextField(
        blank=True,
    )

    llm_response_raw = models.TextField(
        blank=True,
    )

    llm_elapsed_ms = models.PositiveIntegerField(
        null=True,
        blank=True,
    )

    llm_error = models.TextField(
        blank=True,
    )

    created_at = models.DateTimeField(
        auto_now_add=True,
    )

    updated_at = models.DateTimeField(
        auto_now=True,
    )

    published_at = models.DateTimeField(
        null=True,
        blank=True,
    )

    class Meta:
        ordering = [
            "-created_at",
        ]
        indexes = [
            models.Index(
                fields=[
                    "status",
                    "-created_at",
                ],
            ),
            models.Index(
                fields=[
                    "published_at",
                ],
            ),
        ]

    def __str__(self):
        return self.title[:120]


# =============================================================================
# CENTRALIZED RUSSIAN MODEL METADATA
# =============================================================================
#
# Русские названия для Django admin.
#
# Внутренние значения полей и статусов, хранящиеся в базе данных,
# не меняются. Меняются только отображаемые названия моделей,
# полей и вариантов выбора.
# =============================================================================

_INTEL_ADMIN_MODELS = (
    Source,
    FetchLog,
    RawItem,
    Article,
    Event,
    EventItem,
    MedicalBrief,
    GeneratedMedicalNews,
    RegionalDigest,
    RegionalDigestItem,
    AutomotiveBrief,
    GeneratedAutomotiveNews,
)


_RUSSIAN_MODEL_NAMES = {
    Source: (
        "Источник",
        "Источники",
    ),
    FetchLog: (
        "Журнал загрузки",
        "Журналы загрузки",
    ),
    RawItem: (
        "Исходный материал",
        "Исходные материалы",
    ),
    Article: (
        "Извлечённая статья",
        "Извлечённые статьи",
    ),
    Event: (
        "Событие",
        "События",
    ),
    EventItem: (
        "Материал события",
        "Материалы событий",
    ),
    MedicalBrief: (
        "Медицинское редакционное задание",
        "Медицинские редакционные задания",
    ),
    GeneratedMedicalNews: (
        "Сгенерированная медицинская новость",
        "Сгенерированные медицинские новости",
    ),
    RegionalDigest: (
        "Региональная SEO-новость",
        "Региональные SEO-новости",
    ),
    RegionalDigestItem: (
        "Событие региональной SEO-новости",
        "События региональных SEO-новостей",
    ),
    AutomotiveBrief: (
        "Автомобильное редакционное задание",
        "Автомобильные редакционные задания",
    ),
    GeneratedAutomotiveNews: (
        "Сгенерированная автомобильная новость",
        "Сгенерированные автомобильные новости",
    ),
}


_RUSSIAN_FIELD_NAMES = {
    "id": "Идентификатор",

    "name": "Название",
    "url": "Ссылка",
    "region": "Регион",
    "topic": "Тематика",
    "source_class": "Класс источника",
    "cadence": "Частота опроса",
    "is_enabled": "Источник включён",
    "last_fetch_at": "Последняя загрузка",
    "etag": "Метка версии ETag",
    "last_modified": "Дата изменения по HTTP",
    "created_at": "Создано",

    "source": "Источник",
    "fetched_at": "Время загрузки",
    "status_code": "Код ответа HTTP",
    "elapsed_ms": "Время выполнения, мс",
    "bytes_received": "Получено байт",
    "error": "Ошибка",

    "guid": "Уникальный идентификатор материала",
    "title": "Заголовок",
    "summary": "Краткое содержание",
    "published_at": "Дата публикации",
    "item_hash": "Хеш материала",

    "item": "Исходный материал",
    "final_url": "Итоговая ссылка",
    "lang": "Язык",
    "text": "Текст",
    "extracted_at": "Время извлечения",
    "extract_error": "Ошибка извлечения",

    "updated_at": "Обновлено",
    "evidence_level": "Уровень подтверждения",
    "cluster_key": "Ключ события",

    "event": "Связанное событие",
    "angle": "Редакционный ракурс",
    "target_keyword": "Главная SEO-фраза",
    "secondary_keywords": "Дополнительные SEO-фразы",

    "semantic_panel_id": "ID панели КДЛ «Дзагуров»",
    "semantic_panel_code": "Код анализа КДЛ «Дзагуров»",
    "semantic_panel_title": "Название карточки анализа",
    "semantic_panel_url": "URL карточки анализа",
    "semantic_anchor": "Анкор внутренней ссылки",
    "semantic_score": "Оценка семантического совпадения",
    "semantic_match_details": "Детали семантического совпадения",
    "semantic_feed_sha256": "SHA256 семантического каталога",
    "semantic_assigned_at": "Дата назначения посадочной страницы",

    "facts": "Подтверждённые факты",
    "source_urls": "Ссылки на источники",
    "audience": "Целевая аудитория",
    "region_text": "Регион материала",
    "safety_notes": "Ограничения безопасности",
    "disclaimer_required": "Требуется предупреждение",
    "status": "Статус",
    "used_at": "Использовано",

    "brief": "Редакционное задание",
    "slug": "Адресная часть",
    "meta_description": "Мета-описание",
    "body": "Текст новости",
    "source_note": "Примечание об источниках",
    "image_topic": "Тематика изображения",
    "disclaimer": "Предупреждение",
    "quality_score": "Оценка качества",
    "llm_model": "Модель искусственного интеллекта",
    "llm_prompt": "Запрос к модели",
    "llm_response_raw": "Исходный ответ модели",
    "llm_elapsed_ms": "Время генерации, мс",
    "llm_error": "Ошибка генерации",

    "group_key": "Ключ группы",
    "digest_type": "Тип дайджеста",
    "region_code": "Код региона",
    "region_label": "Название региона",
    "region_query": "Поисковый запрос региона",
    "period_start": "Начало периода",
    "period_end": "Конец периода",
    "criteria": "Критерии отбора",
    "evidence_pack": "Пакет подтверждений",
    "source_map": "Карта источников",

    "digest": "Региональная SEO-новость",
    "position": "Позиция",
    "relevance_score": "Оценка релевантности",
    "selection_reason": "Причина отбора",
}


_RUSSIAN_CHOICE_NAMES = {
    "EU": "Европа",
    "WORLD": "Мир",
    "RU": "Россия",

    "economy": "Экономика",
    "politics": "Политика",
    "it": "Информационные технологии",
    "auto": "Автомобили",
    "medicine": "Медицина",
    "labs": "Лабораторная диагностика",

    "agency": "Информационное агентство",
    "official": "Официальный источник",
    "stats": "Статистический источник",
    "industry": "Отраслевой источник",
    "commentary": "Экспертные комментарии",

    "hot": "Часто — каждые 5–15 минут",
    "medium": "Обычно — каждые 1–3 часа",
    "cold": "Редко — каждые 6–24 часа",

    0: "Анонимный или неподтверждённый источник",
    1: "Публикация средства массовой информации",
    2: "Есть первичный или официальный источник",
    3: "Подтверждено источниками разных типов",

    "draft": "Черновик",
    "ready": "Готово к генерации",
    "used": "Использовано",
    "review": "На проверке",
    "approved": "Одобрено",
    "published": "Опубликовано",
    "rejected": "Отклонено",
    "error": "Ошибка",
    "pending": "Ожидает обработки",
    "queued": "В очереди",
    "processing": "Обрабатывается",
    "completed": "Завершено",
    "failed": "Ошибка выполнения",

    "regional": "Региональная новость",
    "regional_digest": "Региональный дайджест",
    "regional_medical": "Региональный медицинский дайджест",
    "russia_medical": "Медицинский дайджест России",
    "north_ossetia": "Северная Осетия",
    "north_ossetia_medical": (
        "Медицинский дайджест Северной Осетии"
    ),
    "single": "Одиночная публикация",
    "daily": "Ежедневный дайджест",
    "weekly": "Еженедельный дайджест",
    "monthly": "Ежемесячный дайджест",
    "general": "Общая тематика",
}


for _model in _INTEL_ADMIN_MODELS:
    _singular, _plural = (
        _RUSSIAN_MODEL_NAMES[_model]
    )

    _model._meta.verbose_name = _singular
    _model._meta.verbose_name_plural = _plural

    for _field in _model._meta.fields:
        if (
            _field.name
            not in _RUSSIAN_FIELD_NAMES
        ):
            raise RuntimeError(
                "Не задана русская подпись поля: "
                f"{_model.__name__}."
                f"{_field.name}"
            )

        _field.verbose_name = (
            _RUSSIAN_FIELD_NAMES[
                _field.name
            ]
        )

        if not _field.choices:
            continue

        _translated_choices = []

        for _value, _label in _field.choices:
            if _value in _RUSSIAN_CHOICE_NAMES:
                _translated_label = (
                    _RUSSIAN_CHOICE_NAMES[
                        _value
                    ]
                )
            else:
                _label_text = str(_label)

                _has_cyrillic = any(
                    (
                        "а"
                        <= _character.lower()
                        <= "я"
                    )
                    or (
                        _character.lower()
                        == "ё"
                    )
                    for _character
                    in _label_text
                )

                if not _has_cyrillic:
                    raise RuntimeError(
                        "Не переведён вариант выбора: "
                        f"{_model.__name__}."
                        f"{_field.name}="
                        f"{_value!r}"
                    )

                _translated_label = (
                    _label_text
                )

            _translated_choices.append(
                (
                    _value,
                    _translated_label,
                )
            )

        _field.choices = (
            _translated_choices
        )


# =============================================================================
# UNIVERSAL CONTENT ENGINE
# =============================================================================


class ContentBriefStatus(models.TextChoices):
    DRAFT = "draft", "Черновик"
    READY = "ready", "Готово к генерации"
    USED = "used", "Использовано"
    REJECTED = "rejected", "Отклонено"


class GeneratedContentStatus(models.TextChoices):
    REVIEW = "review", "На проверке"
    APPROVED = "approved", "Одобрено"
    PUBLISHED = "published", "Опубликовано"
    REJECTED = "rejected", "Отклонено"
    ERROR = "error", "Ошибка"


class ContentProject(models.Model):
    key = models.SlugField("Ключ проекта", max_length=80, unique=True)
    name = models.CharField("Название", max_length=200)
    niche = models.CharField("Тематика", max_length=200)
    audience = models.CharField("Целевая аудитория", max_length=300)
    locale = models.CharField("Локаль", max_length=20, default="ru-RU")
    brand_name = models.CharField("Бренд", max_length=200, blank=True)
    site_url = models.URLField("Сайт", max_length=500, blank=True)
    system_prompt = models.TextField("Системные инструкции", blank=True)
    policy = models.JSONField(
        "Политика качества",
        default=dict,
        blank=True,
        help_text=(
            "Настройки риска, источников, экспертной проверки "
            "и запрещённых утверждений."
        ),
    )
    is_enabled = models.BooleanField("Проект включён", default=True)
    created_at = models.DateTimeField("Создано", auto_now_add=True)
    updated_at = models.DateTimeField("Обновлено", auto_now=True)

    class Meta:
        ordering = ["name"]
        verbose_name = "Контентный проект"
        verbose_name_plural = "Контентные проекты"

    def __str__(self):
        return self.name


class ContentTemplate(models.Model):
    project = models.ForeignKey(
        ContentProject,
        on_delete=models.CASCADE,
        related_name="templates",
        verbose_name="Проект",
    )
    key = models.SlugField("Ключ шаблона", max_length=80)
    name = models.CharField("Название", max_length=200)
    content_type = models.CharField(
        "Тип материала",
        max_length=50,
        default="evergreen_article",
    )
    instructions = models.TextField("Инструкции")
    min_chars = models.PositiveIntegerField("Минимум символов", default=1800)
    max_chars = models.PositiveIntegerField("Максимум символов", default=4000)
    min_sections = models.PositiveSmallIntegerField(
        "Минимум разделов",
        default=3,
    )
    expert_review_required = models.BooleanField(
        "Требуется экспертная проверка",
        default=True,
    )
    is_enabled = models.BooleanField("Шаблон включён", default=True)
    created_at = models.DateTimeField("Создано", auto_now_add=True)
    updated_at = models.DateTimeField("Обновлено", auto_now=True)

    class Meta:
        ordering = ["project", "name"]
        constraints = [
            models.UniqueConstraint(
                fields=["project", "key"],
                name="intel_unique_content_template",
            ),
        ]
        verbose_name = "Шаблон контента"
        verbose_name_plural = "Шаблоны контента"

    def __str__(self):
        return f"{self.project.name}: {self.name}"


class ContentSource(models.Model):
    project = models.ForeignKey(
        ContentProject,
        on_delete=models.CASCADE,
        related_name="content_sources",
        verbose_name="Проект",
    )
    name = models.CharField("Название", max_length=300)
    url = models.URLField("URL", max_length=1000)
    source_type = models.CharField(
        "Тип источника",
        max_length=50,
        default="primary",
    )
    trust_level = models.PositiveSmallIntegerField(
        "Уровень доверия",
        default=3,
    )
    is_enabled = models.BooleanField("Источник включён", default=True)
    last_title = models.CharField(
        "Последний заголовок",
        max_length=500,
        blank=True,
    )
    last_text = models.TextField("Последний извлечённый текст", blank=True)
    content_sha256 = models.CharField(
        "SHA256 содержимого",
        max_length=64,
        blank=True,
    )
    fetched_at = models.DateTimeField("Загружено", null=True, blank=True)
    fetch_error = models.TextField("Ошибка загрузки", blank=True)
    created_at = models.DateTimeField("Создано", auto_now_add=True)
    updated_at = models.DateTimeField("Обновлено", auto_now=True)

    class Meta:
        ordering = ["project", "-trust_level", "name"]
        constraints = [
            models.UniqueConstraint(
                fields=["project", "url"],
                name="intel_unique_project_content_source",
            ),
        ]
        verbose_name = "Источник evidence"
        verbose_name_plural = "Источники evidence"

    def __str__(self):
        return f"{self.project.name}: {self.name}"


class ContentBrief(models.Model):
    project = models.ForeignKey(
        ContentProject,
        on_delete=models.CASCADE,
        related_name="briefs",
        verbose_name="Проект",
    )
    template = models.ForeignKey(
        ContentTemplate,
        on_delete=models.PROTECT,
        related_name="briefs",
        verbose_name="Шаблон",
    )
    cluster_key = models.SlugField("Ключ кластера", max_length=160)
    title = models.CharField("Рабочий заголовок", max_length=300)
    primary_keyword = models.CharField("Главный запрос", max_length=300)
    secondary_keywords = models.JSONField(
        "Дополнительные запросы",
        default=list,
        blank=True,
    )
    search_intent = models.CharField(
        "Поисковое намерение",
        max_length=80,
        default="informational",
    )
    evidence_pack = models.JSONField(
        "Пакет доказательств",
        default=list,
        help_text=(
            "Список объектов: id, claim, source_url, source_title."
        ),
    )
    internal_links = models.JSONField(
        "Внутренние ссылки",
        default=list,
        blank=True,
        help_text="Список объектов: url, anchor.",
    )
    instructions = models.TextField("Дополнительные инструкции", blank=True)
    status = models.CharField(
        "Статус",
        max_length=20,
        choices=ContentBriefStatus.choices,
        default=ContentBriefStatus.DRAFT,
        db_index=True,
    )
    created_at = models.DateTimeField("Создано", auto_now_add=True)
    updated_at = models.DateTimeField("Обновлено", auto_now=True)
    used_at = models.DateTimeField("Использовано", null=True, blank=True)

    class Meta:
        ordering = ["-created_at"]
        constraints = [
            models.UniqueConstraint(
                fields=["project", "cluster_key"],
                name="intel_unique_content_cluster",
            ),
        ]
        indexes = [
            models.Index(fields=["project", "status", "-created_at"]),
        ]
        verbose_name = "Универсальное редакционное задание"
        verbose_name_plural = "Универсальные редакционные задания"

    def __str__(self):
        return self.title


class GeneratedContent(models.Model):
    brief = models.ForeignKey(
        ContentBrief,
        on_delete=models.PROTECT,
        related_name="generated_items",
        verbose_name="Задание",
    )
    title = models.CharField("Заголовок", max_length=300)
    slug = models.SlugField("URL", max_length=255, db_index=True)
    meta_description = models.CharField(
        "Мета-описание",
        max_length=320,
    )
    body = models.TextField("Текст Markdown")
    used_evidence_ids = models.JSONField(
        "Использованные доказательства",
        default=list,
    )
    source_urls = models.JSONField("Источники", default=list)
    qa_report = models.JSONField("Отчёт проверки", default=dict)
    quality_score = models.PositiveSmallIntegerField(
        "Оценка качества",
        default=0,
    )
    status = models.CharField(
        "Статус",
        max_length=20,
        choices=GeneratedContentStatus.choices,
        default=GeneratedContentStatus.REVIEW,
        db_index=True,
    )
    reviewer_name = models.CharField(
        "Проверил",
        max_length=200,
        blank=True,
    )
    reviewed_at = models.DateTimeField("Проверено", null=True, blank=True)
    llm_model = models.CharField("LLM", max_length=120, blank=True)
    llm_prompt = models.TextField("Запрос LLM", blank=True)
    llm_response_raw = models.TextField("Ответ LLM", blank=True)
    llm_elapsed_ms = models.PositiveIntegerField(
        "Время генерации, мс",
        null=True,
        blank=True,
    )
    llm_error = models.TextField("Ошибка", blank=True)
    created_at = models.DateTimeField("Создано", auto_now_add=True)
    updated_at = models.DateTimeField("Обновлено", auto_now=True)
    published_at = models.DateTimeField("Опубликовано", null=True, blank=True)

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["status", "-created_at"]),
            models.Index(fields=["brief", "status"]),
        ]
        verbose_name = "Универсальный материал"
        verbose_name_plural = "Универсальные материалы"

    def __str__(self):
        return self.title
