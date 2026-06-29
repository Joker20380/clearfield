from django.contrib import admin, messages
from django.urls import reverse
from django.utils import timezone
from django.utils.html import format_html

from .models import (
    Article,
    Event,
    EventItem,
    FetchLog,
    GeneratedMedicalNews,
    MedicalBrief,
    MedicalBriefStatus,
    MedicalNewsStatus,
    RawItem,
    Source,
)


# =============================================================================
# ADMIN SITE BRANDING
# =============================================================================

admin.site.site_header = "CLEARFIELD — медицинские новости"
admin.site.site_title = "CLEARFIELD"
admin.site.index_title = "Панель управления новостным конвейером"


# =============================================================================
# RUSSIAN ADMIN NAMES
# =============================================================================

def set_admin_names(model, singular: str, plural: str) -> None:
    """
    Локальная русификация названий моделей в Django admin.

    В идеале verbose_name лучше держать в models.py,
    но на текущем этапе оставляем всё в admin.py, как ты попросил.
    """
    model._meta.verbose_name = singular
    model._meta.verbose_name_plural = plural


set_admin_names(Source, "Источник", "Источники")
set_admin_names(FetchLog, "Лог загрузки", "Логи загрузки")
set_admin_names(RawItem, "Сырой материал", "Сырые материалы")
set_admin_names(Article, "Извлечённая статья", "Извлечённые статьи")
set_admin_names(Event, "Событие", "События")
set_admin_names(EventItem, "Материал события", "Материалы событий")
set_admin_names(MedicalBrief, "Медицинское задание", "Медицинские задания")
set_admin_names(GeneratedMedicalNews, "Медицинская новость", "Медицинские новости")


# =============================================================================
# HELPERS
# =============================================================================

def admin_change_link(obj, label=None):
    if not obj or not obj.pk:
        return "—"

    app_label = obj._meta.app_label
    model_name = obj._meta.model_name
    url = reverse(f"admin:{app_label}_{model_name}_change", args=[obj.pk])

    return format_html('<a href="{}">{}</a>', url, label or str(obj))


def short_text(value, limit=90):
    value = value or ""
    if len(value) <= limit:
        return value
    return f"{value[:limit]}..."


# =============================================================================
# SOURCE / FETCHING
# =============================================================================

@admin.register(Source)
class SourceAdmin(admin.ModelAdmin):
    list_display = (
        "name",
        "region",
        "topic",
        "source_class",
        "cadence",
        "is_enabled",
        "last_fetch_at",
    )
    list_filter = (
        "region",
        "topic",
        "source_class",
        "cadence",
        "is_enabled",
    )
    search_fields = (
        "name",
        "url",
    )
    readonly_fields = (
        "created_at",
        "last_fetch_at",
        "etag",
        "last_modified",
    )
    ordering = (
        "region",
        "topic",
        "name",
    )

    fieldsets = (
        ("Основное", {
            "fields": (
                "name",
                "url",
                "is_enabled",
            )
        }),
        ("Классификация", {
            "fields": (
                "region",
                "topic",
                "source_class",
                "cadence",
            )
        }),
        ("HTTP-кэш", {
            "fields": (
                "etag",
                "last_modified",
                "last_fetch_at",
            ),
            "classes": (
                "collapse",
            ),
        }),
        ("Служебное", {
            "fields": (
                "created_at",
            ),
            "classes": (
                "collapse",
            ),
        }),
    )

    actions = (
        "enable_sources",
        "disable_sources",
        "mark_as_medicine",
        "mark_as_labs",
    )

    @admin.action(description="Включить выбранные источники")
    def enable_sources(self, request, queryset):
        updated = queryset.update(is_enabled=True)
        self.message_user(request, f"Включено источников: {updated}", messages.SUCCESS)

    @admin.action(description="Отключить выбранные источники")
    def disable_sources(self, request, queryset):
        updated = queryset.update(is_enabled=False)
        self.message_user(request, f"Отключено источников: {updated}", messages.WARNING)

    @admin.action(description="Назначить тему: медицина")
    def mark_as_medicine(self, request, queryset):
        updated = queryset.update(topic="medicine")
        self.message_user(request, f"Обновлено источников: {updated}", messages.SUCCESS)

    @admin.action(description="Назначить тему: лабораторная диагностика")
    def mark_as_labs(self, request, queryset):
        updated = queryset.update(topic="labs")
        self.message_user(request, f"Обновлено источников: {updated}", messages.SUCCESS)


@admin.register(FetchLog)
class FetchLogAdmin(admin.ModelAdmin):
    list_display = (
        "fetched_at",
        "source_link",
        "status_code",
        "elapsed_ms",
        "bytes_received",
        "has_error",
    )
    list_filter = (
        "status_code",
        "source__region",
        "source__topic",
        "source__source_class",
    )
    search_fields = (
        "source__name",
        "source__url",
        "error",
    )
    readonly_fields = (
        "source",
        "fetched_at",
        "status_code",
        "elapsed_ms",
        "bytes_received",
        "error",
    )
    ordering = (
        "-fetched_at",
    )

    def source_link(self, obj):
        return admin_change_link(obj.source, obj.source.name)

    source_link.short_description = "Источник"

    def has_error(self, obj):
        return bool(obj.error)

    has_error.boolean = True
    has_error.short_description = "Ошибка"


# =============================================================================
# RAW ITEMS / ARTICLES
# =============================================================================

class ArticleInline(admin.StackedInline):
    model = Article
    extra = 0
    can_delete = False
    readonly_fields = (
        "final_url",
        "lang",
        "title",
        "text",
        "extracted_at",
        "extract_error",
    )

    fieldsets = (
        ("Извлечённый контент", {
            "fields": (
                "final_url",
                "lang",
                "title",
                "text",
            )
        }),
        ("Служебное", {
            "fields": (
                "extracted_at",
                "extract_error",
            ),
            "classes": (
                "collapse",
            ),
        }),
    )


@admin.register(RawItem)
class RawItemAdmin(admin.ModelAdmin):
    list_display = (
        "created_at",
        "published_at",
        "source_link",
        "short_title",
        "has_article",
        "has_event",
    )
    list_filter = (
        "source__region",
        "source__topic",
        "source__source_class",
        "source__is_enabled",
    )
    search_fields = (
        "title",
        "summary",
        "url",
        "guid",
        "source__name",
    )
    readonly_fields = (
        "source",
        "guid",
        "url",
        "title",
        "summary",
        "published_at",
        "item_hash",
        "created_at",
        "open_url",
    )
    ordering = (
        "-published_at",
        "-created_at",
    )
    inlines = (
        ArticleInline,
    )
    autocomplete_fields = (
        "source",
    )

    fieldsets = (
        ("Источник", {
            "fields": (
                "source",
                "open_url",
                "url",
                "guid",
            )
        }),
        ("Контент", {
            "fields": (
                "title",
                "summary",
                "published_at",
            )
        }),
        ("Служебное", {
            "fields": (
                "item_hash",
                "created_at",
            ),
            "classes": (
                "collapse",
            ),
        }),
    )

    def source_link(self, obj):
        return admin_change_link(obj.source, obj.source.name)

    source_link.short_description = "Источник"

    def short_title(self, obj):
        return short_text(obj.title, 100)

    short_title.short_description = "Заголовок"

    def open_url(self, obj):
        if not obj.url:
            return "—"
        return format_html('<a href="{}" target="_blank" rel="noopener">Открыть источник</a>', obj.url)

    open_url.short_description = "Ссылка"

    def has_article(self, obj):
        return hasattr(obj, "article")

    has_article.boolean = True
    has_article.short_description = "Статья"

    def has_event(self, obj):
        return hasattr(obj, "event_item")

    has_event.boolean = True
    has_event.short_description = "Событие"


@admin.register(Article)
class ArticleAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "item_link",
        "short_title",
        "lang",
        "extracted_at",
        "has_error",
    )
    list_filter = (
        "lang",
        "extracted_at",
    )
    search_fields = (
        "title",
        "text",
        "item__title",
        "item__url",
        "extract_error",
    )
    readonly_fields = (
        "item",
        "final_url",
        "lang",
        "title",
        "text",
        "extracted_at",
        "extract_error",
    )
    ordering = (
        "-extracted_at",
    )
    autocomplete_fields = (
        "item",
    )

    fieldsets = (
        ("Связанный материал", {
            "fields": (
                "item",
                "final_url",
            )
        }),
        ("Извлечённая статья", {
            "fields": (
                "lang",
                "title",
                "text",
            )
        }),
        ("Служебное", {
            "fields": (
                "extracted_at",
                "extract_error",
            ),
            "classes": (
                "collapse",
            ),
        }),
    )

    def item_link(self, obj):
        return admin_change_link(obj.item, short_text(obj.item.title, 80))

    item_link.short_description = "Сырой материал"

    def short_title(self, obj):
        if obj.title:
            return short_text(obj.title, 100)
        if obj.item and obj.item.title:
            return short_text(obj.item.title, 100)
        return "(без заголовка)"

    short_title.short_description = "Заголовок"

    def has_error(self, obj):
        return bool(obj.extract_error)

    has_error.boolean = True
    has_error.short_description = "Ошибка"


# =============================================================================
# EVENTS
# =============================================================================

class EventItemInline(admin.TabularInline):
    model = EventItem
    extra = 0
    autocomplete_fields = (
        "item",
    )
    readonly_fields = (
        "created_at",
        "item_title",
        "item_source",
    )

    fields = (
        "item",
        "item_title",
        "item_source",
        "created_at",
    )

    def item_title(self, obj):
        if not obj.item:
            return "—"
        return short_text(obj.item.title, 80)

    item_title.short_description = "Заголовок материала"

    def item_source(self, obj):
        if not obj.item or not obj.item.source:
            return "—"
        return obj.item.source.name

    item_source.short_description = "Источник"


class MedicalBriefInline(admin.TabularInline):
    model = MedicalBrief
    extra = 0
    readonly_fields = (
        "created_at",
        "updated_at",
        "used_at",
    )
    fields = (
        "title",
        "target_keyword",
        "status",
        "created_at",
        "used_at",
    )
    show_change_link = True


@admin.register(Event)
class EventAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "evidence_level",
        "region",
        "topic",
        "short_title",
        "medical_briefs_count",
        "updated_at",
    )
    list_filter = (
        "evidence_level",
        "region",
        "topic",
        "updated_at",
    )
    search_fields = (
        "title",
        "summary",
        "cluster_key",
        "items__item__title",
        "items__item__summary",
    )
    readonly_fields = (
        "created_at",
        "updated_at",
        "cluster_key",
    )
    ordering = (
        "-updated_at",
    )
    inlines = (
        EventItemInline,
        MedicalBriefInline,
    )

    fieldsets = (
        ("Событие", {
            "fields": (
                "title",
                "summary",
            )
        }),
        ("Классификация", {
            "fields": (
                "region",
                "topic",
                "evidence_level",
            )
        }),
        ("Служебное", {
            "fields": (
                "cluster_key",
                "created_at",
                "updated_at",
            ),
            "classes": (
                "collapse",
            ),
        }),
    )

    actions = (
        "mark_topic_medicine",
        "mark_topic_labs",
        "set_evidence_2",
        "set_evidence_3",
    )

    def short_title(self, obj):
        return short_text(obj.title, 100)

    short_title.short_description = "Заголовок"

    def medical_briefs_count(self, obj):
        return obj.medical_briefs.count()

    medical_briefs_count.short_description = "Мед. задания"

    @admin.action(description="Назначить тему события: медицина")
    def mark_topic_medicine(self, request, queryset):
        updated = queryset.update(topic="medicine")
        self.message_user(request, f"Обновлено событий: {updated}", messages.SUCCESS)

    @admin.action(description="Назначить тему события: лабораторная диагностика")
    def mark_topic_labs(self, request, queryset):
        updated = queryset.update(topic="labs")
        self.message_user(request, f"Обновлено событий: {updated}", messages.SUCCESS)

    @admin.action(description="Установить уровень доказательности 2")
    def set_evidence_2(self, request, queryset):
        updated = queryset.update(evidence_level=2)
        self.message_user(request, f"Обновлено событий: {updated}", messages.SUCCESS)

    @admin.action(description="Установить уровень доказательности 3")
    def set_evidence_3(self, request, queryset):
        updated = queryset.update(evidence_level=3)
        self.message_user(request, f"Обновлено событий: {updated}", messages.SUCCESS)


# =============================================================================
# MEDICAL BRIEFS
# =============================================================================

class GeneratedMedicalNewsInline(admin.TabularInline):
    model = GeneratedMedicalNews
    extra = 0
    readonly_fields = (
        "created_at",
        "updated_at",
        "published_at",
        "llm_elapsed_ms",
        "llm_model",
    )
    fields = (
        "title",
        "status",
        "quality_score",
        "llm_model",
        "llm_elapsed_ms",
        "created_at",
        "published_at",
    )
    show_change_link = True


@admin.register(MedicalBrief)
class MedicalBriefAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "status",
        "short_title",
        "target_keyword",
        "event_link",
        "generated_count",
        "created_at",
        "used_at",
    )
    list_filter = (
        "status",
        "disclaimer_required",
        "created_at",
        "updated_at",
    )
    search_fields = (
        "title",
        "angle",
        "target_keyword",
        "secondary_keywords",
        "facts",
        "source_urls",
        "event__title",
        "event__summary",
    )
    readonly_fields = (
        "created_at",
        "updated_at",
        "used_at",
        "event_link",
    )
    autocomplete_fields = (
        "event",
    )
    ordering = (
        "-created_at",
    )
    inlines = (
        GeneratedMedicalNewsInline,
    )

    fieldsets = (
        ("Редакционное задание", {
            "fields": (
                "status",
                "event",
                "event_link",
                "title",
                "angle",
            )
        }),
        ("SEO", {
            "fields": (
                "target_keyword",
                "secondary_keywords",
            )
        }),
        ("Факты и источники", {
            "fields": (
                "facts",
                "source_urls",
            )
        }),
        ("Аудитория и безопасность", {
            "fields": (
                "audience",
                "region_text",
                "safety_notes",
                "disclaimer_required",
            )
        }),
        ("Служебное", {
            "fields": (
                "created_at",
                "updated_at",
                "used_at",
            ),
            "classes": (
                "collapse",
            ),
        }),
    )

    actions = (
        "mark_ready",
        "mark_used",
        "mark_rejected",
        "reset_to_draft",
    )

    def short_title(self, obj):
        return short_text(obj.title, 100)

    short_title.short_description = "Заголовок задания"

    def event_link(self, obj):
        if not obj.event:
            return "—"
        return admin_change_link(obj.event, f"Событие #{obj.event_id}")

    event_link.short_description = "Связанное событие"

    def generated_count(self, obj):
        return obj.generated_news.count()

    generated_count.short_description = "Новостей"

    @admin.action(description="Отметить как готовые к генерации")
    def mark_ready(self, request, queryset):
        updated = queryset.update(status=MedicalBriefStatus.READY)
        self.message_user(request, f"Готово к генерации: {updated}", messages.SUCCESS)

    @admin.action(description="Отметить как использованные")
    def mark_used(self, request, queryset):
        updated = queryset.update(
            status=MedicalBriefStatus.USED,
            used_at=timezone.now(),
        )
        self.message_user(request, f"Отмечено как использованные: {updated}", messages.SUCCESS)

    @admin.action(description="Отклонить выбранные задания")
    def mark_rejected(self, request, queryset):
        updated = queryset.update(status=MedicalBriefStatus.REJECTED)
        self.message_user(request, f"Отклонено заданий: {updated}", messages.WARNING)

    @admin.action(description="Вернуть в черновики")
    def reset_to_draft(self, request, queryset):
        updated = queryset.update(
            status=MedicalBriefStatus.DRAFT,
            used_at=None,
        )
        self.message_user(request, f"Возвращено в черновики: {updated}", messages.SUCCESS)


# =============================================================================
# GENERATED MEDICAL NEWS
# =============================================================================

@admin.register(GeneratedMedicalNews)
class GeneratedMedicalNewsAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "status",
        "short_title",
        "quality_score",
        "brief_link",
        "llm_model",
        "llm_elapsed_ms",
        "created_at",
        "published_at",
    )
    list_filter = (
        "status",
        "quality_score",
        "llm_model",
        "created_at",
        "published_at",
    )
    search_fields = (
        "title",
        "slug",
        "meta_description",
        "body",
        "source_note",
        "llm_error",
        "brief__title",
        "brief__target_keyword",
    )
    readonly_fields = (
        "created_at",
        "updated_at",
        "published_at",
        "llm_model",
        "llm_prompt",
        "llm_response_raw",
        "llm_elapsed_ms",
        "llm_error",
        "brief_link",
    )
    autocomplete_fields = (
        "brief",
    )
    ordering = (
        "-created_at",
    )

    fieldsets = (
        ("Статус", {
            "fields": (
                "status",
                "quality_score",
                "brief",
                "brief_link",
            )
        }),
        ("SEO", {
            "fields": (
                "title",
                "slug",
                "meta_description",
            )
        }),
        ("Текст новости", {
            "fields": (
                "body",
                "source_note",
                "disclaimer",
            )
        }),
        ("LLM", {
            "fields": (
                "llm_model",
                "llm_elapsed_ms",
                "llm_error",
                "llm_prompt",
                "llm_response_raw",
            ),
            "classes": (
                "collapse",
            ),
        }),
        ("Даты", {
            "fields": (
                "created_at",
                "updated_at",
                "published_at",
            ),
            "classes": (
                "collapse",
            ),
        }),
    )

    actions = (
        "mark_review",
        "mark_approved",
        "mark_published",
        "mark_rejected",
        "mark_error",
    )

    def short_title(self, obj):
        return short_text(obj.title, 100)

    short_title.short_description = "Заголовок"

    def brief_link(self, obj):
        if not obj.brief:
            return "—"
        return admin_change_link(obj.brief, f"Задание #{obj.brief_id}")

    brief_link.short_description = "Задание"

    @admin.action(description="Отправить на проверку")
    def mark_review(self, request, queryset):
        updated = queryset.update(status=MedicalNewsStatus.REVIEW)
        self.message_user(request, f"Отправлено на проверку: {updated}", messages.SUCCESS)

    @admin.action(description="Утвердить выбранные новости")
    def mark_approved(self, request, queryset):
        updated = queryset.update(status=MedicalNewsStatus.APPROVED)
        self.message_user(request, f"Утверждено новостей: {updated}", messages.SUCCESS)

    @admin.action(description="Отметить как опубликованные")
    def mark_published(self, request, queryset):
        updated = queryset.update(
            status=MedicalNewsStatus.PUBLISHED,
            published_at=timezone.now(),
        )
        self.message_user(request, f"Отмечено как опубликованные: {updated}", messages.SUCCESS)

    @admin.action(description="Отклонить выбранные новости")
    def mark_rejected(self, request, queryset):
        updated = queryset.update(status=MedicalNewsStatus.REJECTED)
        self.message_user(request, f"Отклонено новостей: {updated}", messages.WARNING)

    @admin.action(description="Отметить как ошибку генерации")
    def mark_error(self, request, queryset):
        updated = queryset.update(status=MedicalNewsStatus.ERROR)
        self.message_user(request, f"Отмечено как ошибка: {updated}", messages.ERROR)