from django.contrib import admin
from .models import Source, FetchLog, RawItem


@admin.register(Source)
class SourceAdmin(admin.ModelAdmin):
    list_display = ("name", "region", "topic", "source_class", "cadence", "is_enabled", "last_fetch_at")
    list_filter = ("region", "topic", "source_class", "cadence", "is_enabled")
    search_fields = ("name", "url")
    ordering = ("region", "topic", "name")


@admin.register(FetchLog)
class FetchLogAdmin(admin.ModelAdmin):
    list_display = ("fetched_at", "source", "status_code", "elapsed_ms", "bytes_received")
    list_filter = ("status_code", "source__region", "source__topic")
    search_fields = ("source__name", "source__url")
    ordering = ("-fetched_at",)


@admin.register(RawItem)
class RawItemAdmin(admin.ModelAdmin):
    list_display = ("created_at", "source", "published_at", "title")
    list_filter = ("source__region", "source__topic", "source__source_class")
    search_fields = ("title", "url", "source__name")
    ordering = ("-published_at", "-created_at")
