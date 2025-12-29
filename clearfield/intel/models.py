from django.db import models
from django.utils import timezone


class Region(models.TextChoices):
    EU = "EU", "Europe"
    WORLD = "WORLD", "World"
    RU = "RU", "Russia"


class Topic(models.TextChoices):
    ECONOMY = "economy", "Economy"
    POLITICS = "politics", "Politics"
    IT = "it", "IT"
    AUTO = "auto", "Auto"


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

    def __str__(self) -> str:
        return self.name


class FetchLog(models.Model):
    source = models.ForeignKey(Source, on_delete=models.CASCADE, related_name="fetch_logs")
    fetched_at = models.DateTimeField(default=timezone.now)

    status_code = models.IntegerField(null=True, blank=True)
    elapsed_ms = models.IntegerField(null=True, blank=True)
    bytes_received = models.IntegerField(null=True, blank=True)

    error = models.TextField(null=True, blank=True)

    def __str__(self) -> str:
        return f"{self.source_id} {self.status_code} {self.fetched_at:%Y-%m-%d %H:%M}"


class RawItem(models.Model):
    source = models.ForeignKey(Source, on_delete=models.CASCADE, related_name="items")

    guid = models.CharField(max_length=500, blank=True)
    url = models.URLField()
    title = models.TextField(blank=True)
    summary = models.TextField(blank=True)
    published_at = models.DateTimeField(null=True, blank=True)

    # дедуп ключ
    item_hash = models.CharField(max_length=64, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["source", "item_hash"], name="uniq_source_itemhash")
        ]

    def __str__(self) -> str:
        return self.title[:80]
