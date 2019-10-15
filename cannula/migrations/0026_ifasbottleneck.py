# -*- coding: utf-8 -*-
# from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('cannula', '0025_pmtcteid_targets'),
    ]

    operations = [
        migrations.CreateModel(
            name='IFASBottleneck',
            fields=[
                ('id', models.AutoField(verbose_name='ID', primary_key=True, serialize=False, auto_created=True)),
                ('bottleneck', models.CharField(verbose_name='Enter the name of the bottleneck', max_length=200)),
                ('level', models.CharField(verbose_name='Level', max_length=200)),
                ('when_identified', models.DateField(verbose_name='Please enter the date when the bottleneck was identified')),
                ('where_identified', models.IntegerField(default=1, choices=[(1, 'Bottleneck Workshop'), (2, 'Others'), (5, 'Do not Know')])),
                ('potential_solutions', models.CharField(verbose_name='Level', max_length=400)),
                ('efforts_to_address_bottleneck', models.CharField(verbose_name='Efforts to address the bottleneck', max_length=500)),
                ('next_steps', models.CharField(verbose_name='Next steps', max_length=500)),
                ('additional_bottleneck_identified', models.CharField(verbose_name='Additional bottleneck identified during efforts', max_length=500)),
                ('comments', models.CharField(verbose_name='Comments if any', max_length=500)),
            ],
        ),
    ]
