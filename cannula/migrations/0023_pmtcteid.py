# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('cannula', '0022_auto_20190117_0953'),
    ]

    operations = [
        migrations.CreateModel(
            name='pmtcteid',
            fields=[
                ('id', models.AutoField(verbose_name='ID', primary_key=True, serialize=False, auto_created=True)),
                ('period', models.DateField(verbose_name='YYYY-MM', max_length=200)),
                ('district', models.CharField(verbose_name='District', max_length=200)),
                ('subcounty', models.CharField(verbose_name='Subcounty', max_length=200)),
                ('healthfacility', models.CharField(verbose_name='Health Facility', max_length=200)),
                ('ca1', models.IntegerField(verbose_name='105-2.1 A17:HIV+ Pregnant Women already on ART before 1st ANC (ART-K)', default=0)),
                ('ca2', models.IntegerField(verbose_name='105-2.1 A19:Pregnant Women testing HIV+ on a retest (TRR+)', default=0)),
                ('ca3', models.IntegerField(verbose_name='105-2.1 A1:ANC 1st Visit for women 10-19 Years', default=0)),
                ('ca4', models.IntegerField(verbose_name='105-2.1 A1:ANC 1st Visit for women >=25 Years', default=0)),
                ('ca6', models.IntegerField(verbose_name='105-2.1 HIV+ Pregnant Women initiated on ART for EMTCT (ART)', default=0)),
                ('ca7', models.IntegerField(verbose_name='105-2.1 Pregnant Women newly tested for HIV this pregnancy(TR & TRR) 10-19 Years', default=0)),
                ('ca8', models.IntegerField(verbose_name='105-2.1 Pregnant Women newly tested for HIV this pregnancy(TR & TRR) 20-24 Years', default=0)),
                ('ca9', models.IntegerField(verbose_name='105-2.1 Pregnant Women newly tested for HIV this pregnancy(TR & TRR) >=25 Years', default=0)),
                ('ca10', models.IntegerField(verbose_name='105-2.1 Pregnant Women tested HIV+ for 1st time this pregnancy (TRR) at any visit 10-19 Years', default=0)),
                ('ca11', models.IntegerField(verbose_name='105-2.1 Pregnant Women tested HIV+ for 1st time this pregnancy (TRR) at any visit 20-24 Years', default=0)),
                ('ca12', models.IntegerField(verbose_name='105-2.1 Pregnant Women tested HIV+ for 1st time this pregnancy (TRR) at any visit >=25 Years', default=0)),
                ('ca13', models.IntegerField(verbose_name='105-2.1a Pregnant Women who knew status before 1st ANC (Total (TRK + TRRK))', default=0)),
                ('ca14', models.IntegerField(verbose_name='105-2.1b Pregnant Women who knew status before 1st ANC (HIV+(TRRK))', default=0)),
                ('ca15', models.IntegerField(verbose_name='105-2.2a Women testing HIV+ in labour (1st time this Pregnancy)', default=0)),
                ('ca16', models.IntegerField(verbose_name='105-2.2b Women testing HIV+ in labour (Retest this Pregnancy)', default=0)),
                ('ca17', models.IntegerField(verbose_name='105-2.3a Breastfeeding mothers newly testing HIV+(1st test)', default=0)),
                ('ca18', models.IntegerField(verbose_name='105-2.3b Breastfeeding mothers newly testing HIV+(retest)', default=0)),
                ('ca19', models.IntegerField(verbose_name='105-2.4a Exposed Infants Tested for HIV Below 18 Months(by 1st PCR)', default=0)),
                ('ca20', models.IntegerField(verbose_name='105-2.4b 1st DNA PCR result returned(HIV+)', default=0)),
                ('ca21', models.IntegerField(verbose_name='105-2.4c Exposed Infants Tested for HIV Below 18 Months(< 2 Months old)', default=0)),
                ('ca22', models.IntegerField(verbose_name='012 1.Total number of HEI in birth cohort (born 24 months previously)', default=0)),
                ('ca23', models.IntegerField(verbose_name='012 7.A. Outcomes for HIV exposed infants: Number of HEI being discharged at 18 months as Positive', default=0)),
                ('ca24', models.IntegerField(verbose_name='012 7. Outcomes for HIV exposed infants: Total Number of HEI being discharged at 18 months', default=0)),
                ('ca25', models.IntegerField(verbose_name='012 7.A. Outcomes for HIV exposed infants: Number of HEI being discharged at 18 months as Negative', default=0)),
                ('ca26', models.IntegerField(verbose_name='012 7.D. Outcomes for HIV exposed infants: Transferred out (Number of HEI who were transferred out before 18 months)', default=0)),
                ('ca27', models.IntegerField(verbose_name='012 7.E. Outcomes for HIV exposed infants:  Lost to follow (Number of HEI who are lost to follow-up before 18 months)', default=0)),
                ('ca28', models.IntegerField(verbose_name='012 7.F. Outcomes for HIV exposed infants: Died (Number of HEI who died before 18 months)', default=0)),
                ('ca29', models.IntegerField(verbose_name='012 7.G. Outcomes for HIV exposed infants: In care but no test done at 18 months', default=0)),
            ],
        ),
    ]
