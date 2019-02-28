# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('cannula', '0024_auto_20190128_2000'),
    ]

    operations = [
        migrations.CreateModel(
            name='pmtcteid_targets',
            fields=[
                ('id', models.AutoField(verbose_name='ID', primary_key=True, serialize=False, auto_created=True)),
                ('district', models.CharField(verbose_name='District', max_length=200)),
                ('subcounty', models.CharField(verbose_name='Subcounty', max_length=200)),
                ('healthfacility', models.CharField(verbose_name='Health Facility', max_length=200)),
                ('mtct_pmtct_art', models.IntegerField(verbose_name='105-2.1 A17:HIV+ Pregnant Women already on ART before 1st ANC (ART-K)', default=0)),
                ('pmp', models.IntegerField(verbose_name='105-2.1 A19:Pregnant Women testing HIV+ on a retest (TRR+)', default=0)),
                ('hivplus_infants', models.IntegerField(verbose_name='105-2.1 A1:ANC 1st Visit for women 10-19 Years', default=0)),
                ('anci', models.IntegerField(verbose_name='105-2.1 A1:ANC 1st Visit for women 20-24 Years', default=0)),
                ('pmtctstartpos', models.IntegerField(verbose_name='105-2.1 A1:ANC 1st Visit for women >=25 Years', default=0)),
                ('pmtctstart', models.IntegerField(verbose_name='105-2.1 HIV+ Pregnant Women initiated on ART for EMTCT (ART)', default=0)),
            ],
        ),
    ]
