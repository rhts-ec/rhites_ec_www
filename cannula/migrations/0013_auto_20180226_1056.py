# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('cannula', '0012_auto_20180213_1434'),
    ]

    operations = [
        migrations.AlterField(
            model_name='dataelement',
            name='alias',
            field=models.CharField(db_index=True, max_length=128, blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='dataelement',
            name='name',
            field=models.CharField(db_index=True, max_length=160, unique=True),
        ),
        migrations.AlterField(
            model_name='datavalue',
            name='month',
            field=models.CharField(db_index=True, max_length=7, blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='datavalue',
            name='quarter',
            field=models.CharField(db_index=True, max_length=7, blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='datavalue',
            name='year',
            field=models.CharField(db_index=True, max_length=4, blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='orgunit',
            name='name',
            field=models.CharField(db_index=True, max_length=64),
        ),
    ]
