# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('cannula', '0023_pmtcteid'),
    ]

    operations = [
        migrations.AddField(
            model_name='pmtcteid',
            name='ca5',
            field=models.IntegerField(verbose_name='105-2.1 A1:ANC 1st Visit for women >=25 Years', default=0),
        ),
        migrations.AlterField(
            model_name='pmtcteid',
            name='ca4',
            field=models.IntegerField(verbose_name='105-2.1 A1:ANC 1st Visit for women 20-24 Years', default=0),
        ),
    ]
