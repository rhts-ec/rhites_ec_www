# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('cannula', '0017_lab_labtargets'),
    ]

    operations = [
        migrations.CreateModel(
            name='RMNCHAndMalaria',
            fields=[
                ('id', models.AutoField(verbose_name='ID', primary_key=True, serialize=False, auto_created=True)),
                ('reporting_period', models.DateField(verbose_name='YYYY-MM', max_length=200)),
                ('district', models.CharField(verbose_name='District', max_length=200)),
                ('subcounty', models.CharField(verbose_name='Subcounty', max_length=200)),
                ('healthfacility', models.CharField(verbose_name='Health Facility', max_length=200)),
                ('healthfacilitylevel', models.CharField(verbose_name='Health Facility', max_length=200)),
                ('ownership', models.CharField(verbose_name='Health Facility', max_length=200)),
                ('IPT2_N', models.IntegerField(verbose_name='IPT2_N', default=0)),
                ('IPT2_D', models.IntegerField(verbose_name='IPT2_D', default=0)),
                ('AMTSL_N', models.IntegerField(verbose_name='AMTSL_N', default=0)),
                ('AMTSL_D', models.IntegerField(verbose_name='AMTSL_D', default=0)),
                ('Asphyxia_N', models.IntegerField(verbose_name='Asphyxia_N', default=0)),
                ('Asphyxia_D', models.IntegerField(verbose_name='Asphyxia_D', default=0)),
                ('Diarrhea_N', models.IntegerField(verbose_name='Diarrhea_N', default=0)),
                ('Diarrhea_D', models.IntegerField(verbose_name='Diarrhea_D', default=0)),
                ('Pneumonia_N', models.IntegerField(verbose_name='Pneumonia_N', default=0)),
                ('Pneumonia_D', models.IntegerField(verbose_name='Pneumonia_D', default=0)),
                ('Ret_N', models.IntegerField(verbose_name='Ret_N', default=0)),
                ('Ret_D', models.IntegerField(verbose_name='Ret_D', default=0)),
                ('HEI_N', models.IntegerField(verbose_name='HEI_N', default=0)),
                ('HEI_D', models.IntegerField(verbose_name='HEI_D', default=0)),
                ('simple_malaria_N', models.IntegerField(verbose_name='simple_malaria_N', default=0)),
                ('simple_malaria_D', models.IntegerField(verbose_name='simple_malaria_D', default=0)),
                ('severe_malaria_N', models.IntegerField(verbose_name='severe_malaria_N', default=0)),
                ('severe_malaria_D', models.IntegerField(verbose_name='severe_malaria_D', default=0)),
                ('DMcondoms', models.IntegerField(verbose_name='DOS:Mcondoms', default=0)),
                ('DFcondoms', models.IntegerField(verbose_name='DOS:Fcondoms', default=0)),
                ('DMbeads', models.IntegerField(verbose_name='DOS:Mbeads', default=0)),
                ('DImplanon', models.IntegerField(verbose_name='DOS:Implanon', default=0)),
                ('DJadelle', models.IntegerField(verbose_name='DOS:Jadelle', default=0)),
                ('DIUD', models.IntegerField(verbose_name='DOS:IUD', default=0)),
                ('DDepoProvera', models.IntegerField(verbose_name='DOS:DepoProvera', default=0)),
                ('DSayana', models.IntegerField(verbose_name='DOS:Sayana', default=0)),
                ('DPills', models.IntegerField(verbose_name='DOS:Pills', default=0)),
                ('DEpills', models.IntegerField(verbose_name='DOS:Epills', default=0)),
            ],
        ),
    ]
