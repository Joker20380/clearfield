from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("intel", "0011_universal_content_engine"),
    ]

    operations = [
        migrations.RenameIndex(
            model_name="contentbrief",
            old_name="intel_conte_project_99040e_idx",
            new_name="intel_conte_project_f6478c_idx",
        ),
        migrations.RenameIndex(
            model_name="generatedcontent",
            old_name="intel_gener_status_aa4d09_idx",
            new_name="intel_gener_status_7b0cde_idx",
        ),
        migrations.RenameIndex(
            model_name="generatedcontent",
            old_name="intel_gener_brief_i_6b7f84_idx",
            new_name="intel_gener_brief_i_e4851f_idx",
        ),
    ]
