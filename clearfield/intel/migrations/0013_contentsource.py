from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("intel", "0012_rename_universal_content_indexes"),
    ]

    operations = [
        migrations.CreateModel(
            name="ContentSource",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("name", models.CharField(max_length=300, verbose_name="Название")),
                ("url", models.URLField(max_length=1000, verbose_name="URL")),
                ("source_type", models.CharField(default="primary", max_length=50, verbose_name="Тип источника")),
                ("trust_level", models.PositiveSmallIntegerField(default=3, verbose_name="Уровень доверия")),
                ("is_enabled", models.BooleanField(default=True, verbose_name="Источник включён")),
                ("last_title", models.CharField(blank=True, max_length=500, verbose_name="Последний заголовок")),
                ("last_text", models.TextField(blank=True, verbose_name="Последний извлечённый текст")),
                ("content_sha256", models.CharField(blank=True, max_length=64, verbose_name="SHA256 содержимого")),
                ("fetched_at", models.DateTimeField(blank=True, null=True, verbose_name="Загружено")),
                ("fetch_error", models.TextField(blank=True, verbose_name="Ошибка загрузки")),
                ("created_at", models.DateTimeField(auto_now_add=True, verbose_name="Создано")),
                ("updated_at", models.DateTimeField(auto_now=True, verbose_name="Обновлено")),
                ("project", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="content_sources", to="intel.contentproject", verbose_name="Проект")),
            ],
            options={
                "verbose_name": "Источник evidence",
                "verbose_name_plural": "Источники evidence",
                "ordering": ["project", "-trust_level", "name"],
            },
        ),
        migrations.AddConstraint(
            model_name="contentsource",
            constraint=models.UniqueConstraint(fields=("project", "url"), name="intel_unique_project_content_source"),
        ),
    ]
