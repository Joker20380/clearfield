from django.contrib import admin
from .models import Source, FetchLog, RawItem, Article



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

class ArticleInline(admin.StackedInline):
    model = Article
    extra = 0
    readonly_fields = ("extracted_at",)


@admin.register(RawItem)
class RawItemAdmin(admin.ModelAdmin):
    list_display = ("created_at", "source", "published_at", "title")
    list_filter = ("source__region", "source__topic", "source__source_class")
    search_fields = ("title", "url", "source__name")
    ordering = ("-published_at", "-created_at")
    inlines = [ArticleInline]



@admin.register(Article)
class ArticleAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "item",
        "short_title",
        "lang",
        "extracted_at",
        "has_error",
    )

    list_filter = ("lang",)
    search_fields = ("title", "text", "item__title", "item__url")
    readonly_fields = ("extracted_at",)

    def short_title(self, obj):
        if obj.title:
            return obj.title[:80]
        return "(no title)"

    short_title.short_description = "Title"

    def has_error(self, obj):
        return bool(obj.extract_error)

    has_error.boolean = True
    has_error.short_description = "Error"



