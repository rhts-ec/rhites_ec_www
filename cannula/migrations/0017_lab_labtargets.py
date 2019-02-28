# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('cannula', '0016_tbprev_tbprevtargets'),
    ]

    operations = [
        migrations.CreateModel(
            name='Lab',
            fields=[
                ('id', models.AutoField(verbose_name='ID', primary_key=True, serialize=False, auto_created=True)),
                ('period', models.DateField(verbose_name='YYYY-MM', max_length=200)),
                ('district', models.CharField(verbose_name='District', max_length=200)),
                ('subcounty', models.CharField(verbose_name='Subcounty', max_length=200)),
                ('healthfacility', models.CharField(verbose_name='Health Facility', max_length=200)),
                ('dataelement', models.CharField(verbose_name='Data Element', max_length=200)),
                ('dataelement_value', models.IntegerField(verbose_name='data element value', default=0)),
            ],
        ),
        migrations.CreateModel(
            name='LabTargets',
            fields=[
                ('id', models.AutoField(verbose_name='ID', primary_key=True, serialize=False, auto_created=True)),
                ('period', models.CharField(verbose_name='YYYY-MM', max_length=200)),
                ('district', models.CharField(verbose_name='District', max_length=200)),
                ('subcounty', models.CharField(verbose_name='Subcounty', max_length=200)),
                ('healthfacility', models.CharField(verbose_name='Health Facility', max_length=200)),
                ('dataelement', models.CharField(verbose_name='Data Element', max_length=200)),
                ('dataelement_target', models.IntegerField(verbose_name='data element value', default=0)),
            ],
        ),
    ]
