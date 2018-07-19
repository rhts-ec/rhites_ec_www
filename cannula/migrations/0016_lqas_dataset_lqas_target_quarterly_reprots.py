# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('cannula', '0015_merge'),
    ]

    operations = [
        migrations.CreateModel(
            name='lqas_dataset',
            fields=[
                ('id', models.AutoField(verbose_name='ID', primary_key=True, serialize=False, auto_created=True)),
                ('period', models.CharField(verbose_name='YYYY-MM', max_length=200)),
                ('district', models.CharField(verbose_name='District', max_length=200)),
                ('ca0', models.FloatField(verbose_name='% of individuals who were counselled and received an HIV test in last 12 months and know their resu', default=0)),
                ('ca1', models.FloatField(verbose_name='% of individuals who know how HIV transmission occur from an infected mother to child', default=0)),
                ('ca2', models.FloatField(verbose_name='% of individuals who know two key actions that reduce HIV transmission from an infected mother to her child', default=0)),
                ('ca3', models.FloatField(verbose_name='% of individuals who had sex with more than one sexual partner in the last 12 months', default=0)),
                ('ca4', models.FloatField(verbose_name='% of individuals who had sex with a non-marital or non-cohabiting sexual partner in the last 12 months', default=0)),
                ('ca5', models.FloatField(verbose_name='% of youth 15-24 years who perceive low or no risk of getting HIV/AIDS infection', default=0)),
                ('ca6', models.FloatField(verbose_name='% of youth who have had sexual intercourse before the age of 15 years', default=0)),
                ('ca7', models.FloatField(verbose_name='% of the male youth 15-24yrs who are circumcised', default=0)),
                ('ca9', models.FloatField(verbose_name='% of individuals who know that TB is curable disease', default=0)),
                ('ca10', models.FloatField(verbose_name='% of individuals who know at least two signs and symptoms of TB', default=0)),
                ('ca11', models.FloatField(verbose_name='% of individuals who know how TB is transmitted', default=0)),
                ('ca12', models.FloatField(verbose_name='% of individuals who know the risk of not completing TB treatment', default=0)),
                ('ca13', models.FloatField(verbose_name='% of mothers of children 0-23 months who received two or more doses of IPT2 during their last pregnancy ', default=0)),
                ('ca14', models.FloatField(verbose_name='% of children 0-59 months who slept under a ITN the night preceding the survey', default=0)),
                ('ca15', models.FloatField(verbose_name='% of mothers of children 0-59 months who know two or more ways to prevent malaria', default=0)),
                ('ca16', models.FloatField(verbose_name='% of mothers of children under five years who know two or more signs and  symptoms of malaria ', default=0)),
                ('ca17', models.FloatField(verbose_name='% of Households with at least one ITN', default=0)),
                ('ca18', models.FloatField(verbose_name='% of mothers of children 0-11 months who attended ANC at least 4 times during last pregnancy', default=0)),
                ('ca19', models.FloatField(verbose_name='% of mothers of children 0-11 months who were assisted by a trained health worker during delivery', default=0)),
                ('ca20', models.FloatField(verbose_name='% of women and men age 15 years and above with comprehensive knowledge of HIV', default=0)),
                ('ca21', models.FloatField(verbose_name='% of women in the reproductive age group 15-49 who known at least 3 methods of family planning and have used the method ', default=0)),
                ('ca22', models.FloatField(verbose_name='% of children aged 0-59 months who had a fever in the last two weeks and were tested for malaria ', default=0)),
                ('ca23', models.FloatField(verbose_name='% of children age 36-59 months who are developmentally on track in literacy-numeracy, physical, social-emotional, and learning domains, and the early child development index score (developmentally on track in at least three of these four domains)', default=0)),
                ('ca24', models.FloatField(verbose_name='% of women and men aged 15-49 who experienced sexual violence in the last 12 months', default=0)),
            ],
        ),
        migrations.CreateModel(
            name='lqas_target',
            fields=[
                ('id', models.AutoField(verbose_name='ID', primary_key=True, serialize=False, auto_created=True)),
                ('indicator_name', models.CharField(max_length=2000)),
                ('indicator_target', models.FloatField()),
            ],
        ),
        migrations.CreateModel(
            name='quarterly_reprots',
            fields=[
                ('id', models.AutoField(verbose_name='ID', primary_key=True, serialize=False, auto_created=True)),
                ('title', models.CharField(max_length=100)),
                ('pdf', models.FileField(upload_to='static/media')),
                ('uploaded_at', models.DateTimeField(auto_now_add=True)),
            ],
        ),
    ]
