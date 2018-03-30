from django.db import migrations

class Migration(migrations.Migration):

    dependencies = [
        ("cannula", "0013_auto_20180226_1056")
    ]

    operations = [
        migrations.RunSQL(
            [
                ("CREATE UNIQUE INDEX de_cc_ou_year_quarter_null_idx ON cannula_datavalue (data_element_id, category_combo_id, org_unit_id, year, quarter) WHERE month IS NULL;", None),
                ("CREATE UNIQUE INDEX de_cc_ou_year_null_null_idx ON cannula_datavalue (data_element_id, category_combo_id, org_unit_id, year) WHERE quarter IS NULL AND month IS NULL;", None),
            ],
            [
                ("DROP INDEX de_cc_ou_year_quarter_null_idx;", None),
                ("DROP INDEX de_cc_ou_year_null_null_idx;", None),
            ],
        ),
    ]
