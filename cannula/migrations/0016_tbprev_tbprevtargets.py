# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('cannula', '0015_merge'),
    ]

    operations = [
        migrations.CreateModel(
            name='TbPrev',
            fields=[
                ('id', models.AutoField(verbose_name='ID', primary_key=True, serialize=False, auto_created=True)),
                ('period', models.CharField(verbose_name='YYYY-MM', max_length=200)),
                ('district', models.CharField(verbose_name='District', max_length=200)),
                ('subcounty', models.CharField(verbose_name='Subcounty', max_length=200)),
                ('healthfacility', models.CharField(verbose_name='Health Facility', max_length=200)),
                ('tbprevc1d', models.IntegerField(verbose_name='tb_prev (d,agesex)  _10-14 years, female', default=0)),
                ('tbprevc2d', models.IntegerField(verbose_name='tb_prev (d,agesex)  __lt 10 years, female', default=0)),
                ('tbprevc3d', models.IntegerField(verbose_name='tb_prev (d,agesex)  __lt15 yrs, female', default=0)),
                ('tbprevc4d', models.IntegerField(verbose_name='tb_prev (d,agesex)  _15+ yrs, female', default=0)),
                ('tbprevc5d', models.IntegerField(verbose_name='tb_prev (d,agesex)  _female, 15-17 years', default=0)),
                ('tbprevc6d', models.IntegerField(verbose_name='tb_prev (d,agesex)  _female, 18-Â\xad24 years', default=0)),
                ('tbprevc7d', models.IntegerField(verbose_name='tb_prev (d,agesex)  _female, 25+ yrs', default=0)),
                ('tbprevc8d', models.IntegerField(verbose_name='tb_prev (d,agesex)  _10-14 years, male', default=0)),
                ('tbprevc9d', models.IntegerField(verbose_name='tb_prev (d,agesex)  __lt 10 years, male', default=0)),
                ('tbprevc10d', models.IntegerField(verbose_name='tb_prev (d,agesex)  __lt15 yrs, male', default=0)),
                ('tbprevc11d', models.IntegerField(verbose_name='tb_prev (d,agesex)  _15+ yrs, male', default=0)),
                ('tbprevc12d', models.IntegerField(verbose_name='tb_prev (d,agesex)  _male, 15-17 years', default=0)),
                ('tbprevc13d', models.IntegerField(verbose_name='tb_prev (d,agesex)  _male, 18-Â\xad24 years', default=0)),
                ('tbprevc14d', models.IntegerField(verbose_name='tb_prev (d,agesex)  _male, 25+ yrs', default=0)),
                ('tbprevc15d', models.IntegerField(verbose_name='tb_prev (d,agesex)  _ipt by newly enrolled on art, female', default=0)),
                ('tbprevc16d', models.IntegerField(verbose_name='tb_prev (d,agesex)  _ipt by newly enrolled on art, male', default=0)),
                ('tbprevc17d', models.IntegerField(verbose_name='tb_prev (d,agesex)  _ipt by newly enrolled on art, total', default=0)),
                ('tbprevc18d', models.IntegerField(verbose_name='tb_prev (d,agesex)  _ipt by previously enrolled on   art, female', default=0)),
                ('tbprevc19d', models.IntegerField(verbose_name='tb_prev (d,agesex)  _ipt by previously enrolled on   art, male', default=0)),
                ('tbprevc20d', models.IntegerField(verbose_name='tb_prev (d,agesex)  _ipt by previously enrolled on   art, total', default=0)),
                ('tbprevc21d', models.IntegerField(verbose_name='tb_prev (d,type of tb preventative (tpt)_alternative tpt regimen (eg, 3 month inh and rifapentine) by newly enrolled on art, female', default=0)),
                ('tbprevc22d', models.IntegerField(verbose_name='tb_prev (d,type of tb preventative (tpt)_alternative tpt regimen (eg, 3 month inh and rifapentine) by newly enrolled on art, male', default=0)),
                ('tbprevc23d', models.IntegerField(verbose_name='tb_prev (d,type of tb preventative (tpt)_alternative tpt regimen (eg, 3 month inh and rifapentine) by newly enrolled on art, total', default=0)),
                ('tbprevc24d', models.IntegerField(verbose_name='tb_prev (d,type of tb preventative (tpt)_alternative tpt regimen (eg, 3 month inh and rifapentine) by  previously enrolled on art, female', default=0)),
                ('tbprevc25d', models.IntegerField(verbose_name='tb_prev (d,type of tb preventative (tpt)_alternative tpt regimen (eg, 3 month inh and rifapentine) by  previously enrolled on art, male', default=0)),
                ('tbprevc26d', models.IntegerField(verbose_name='tb_prev (d,type of tb preventative (tpt)_alternative tpt regimen (eg, 3 month inh and rifapentine) by  previously enrolled on art, total', default=0)),
                ('tbprevc1n', models.IntegerField(verbose_name='tb_prev (n,agesex)  _10-14 years, female', default=0)),
                ('tbprevc2n', models.IntegerField(verbose_name='tb_prev (n,agesex)  __lt 10 years, female', default=0)),
                ('tbprevc3n', models.IntegerField(verbose_name='tb_prev (n,agesex)  __lt15 yrs, female', default=0)),
                ('tbprevc4n', models.IntegerField(verbose_name='tb_prev (n,agesex)  _15+ yrs, female', default=0)),
                ('tbprevc5n', models.IntegerField(verbose_name='tb_prev (n,agesex)  _female, 15-17 years', default=0)),
                ('tbprevc6n', models.IntegerField(verbose_name='tb_prev (n,agesex)  _female, 18-Â\xad24 years', default=0)),
                ('tbprevc7n', models.IntegerField(verbose_name='tb_prev (n,agesex)  _female, 25+ yrs', default=0)),
                ('tbprevc8n', models.IntegerField(verbose_name='tb_prev (n,agesex)  _10-14 years, male', default=0)),
                ('tbprevc9n', models.IntegerField(verbose_name='tb_prev (n,agesex)  __lt 10 years, male', default=0)),
                ('tbprevc10n', models.IntegerField(verbose_name='tb_prev (n,agesex)  __lt15 yrs, male', default=0)),
                ('tbprevc11n', models.IntegerField(verbose_name='tb_prev (n,agesex)  _15+ yrs, male', default=0)),
                ('tbprevc12n', models.IntegerField(verbose_name='tb_prev (n,agesex)  _male, 15-17 years', default=0)),
                ('tbprevc13n', models.IntegerField(verbose_name='tb_prev (n,agesex)  _male, 18-Â\xad24 years', default=0)),
                ('tbprevc14n', models.IntegerField(verbose_name='tb_prev (n,agesex)  _male, 25+ yrs', default=0)),
                ('tbprevc15n', models.IntegerField(verbose_name='tb_prev (n,agesex)  _ipt by newly enrolled on art, female', default=0)),
                ('tbprevc16n', models.IntegerField(verbose_name='tb_prev (n,agesex)  _ipt by newly enrolled on art, male', default=0)),
                ('tbprevc17n', models.IntegerField(verbose_name='tb_prev (n,agesex)  _ipt by newly enrolled on art, total', default=0)),
                ('tbprevc18n', models.IntegerField(verbose_name='tb_prev (n,agesex)  _ipt by previously enrolled on   art, female', default=0)),
                ('tbprevc19n', models.IntegerField(verbose_name='tb_prev (n,agesex)  _ipt by previously enrolled on   art, male', default=0)),
                ('tbprevc20n', models.IntegerField(verbose_name='tb_prev (n,agesex)  _ipt by previously enrolled on   art, total', default=0)),
                ('tbprevc21n', models.IntegerField(verbose_name='tb_prev (n,type of tb preventative (tpt)_alternative tpt regimen (eg, 3 month inh and rifapentine) by newly enrolled on art, female', default=0)),
                ('tbprevc22n', models.IntegerField(verbose_name='tb_prev (n,type of tb preventative (tpt)_alternative tpt regimen (eg, 3 month inh and rifapentine) by newly enrolled on art, male', default=0)),
                ('tbprevc23n', models.IntegerField(verbose_name='tb_prev (n,type of tb preventative (tpt)_alternative tpt regimen (eg, 3 month inh and rifapentine) by newly enrolled on art, total', default=0)),
                ('tbprevc24n', models.IntegerField(verbose_name='tb_prev (n,type of tb preventative (tpt)_alternative tpt regimen (eg, 3 month inh and rifapentine) by  previously enrolled on art, female', default=0)),
                ('tbprevc25n', models.IntegerField(verbose_name='tb_prev (n,type of tb preventative (tpt)_alternative tpt regimen (eg, 3 month inh and rifapentine) by  previously enrolled on art, male', default=0)),
                ('tbprevc26n', models.IntegerField(verbose_name='tb_prev (n,type of tb preventative (tpt)_alternative tpt regimen (eg, 3 month inh and rifapentine) by  previously enrolled on art, total', default=0)),
            ],
        ),
        migrations.CreateModel(
            name='TbPrevTargets',
            fields=[
                ('id', models.AutoField(verbose_name='ID', primary_key=True, serialize=False, auto_created=True)),
                ('district', models.CharField(verbose_name='District', max_length=200)),
                ('subcounty', models.CharField(verbose_name='Subcounty', max_length=200)),
                ('healthfacility', models.CharField(verbose_name='Health Facility', max_length=200)),
                ('tbprev_tc1n', models.IntegerField(verbose_name='TB_PREV (N) Numerator', default=0)),
                ('tbprev_tc2n', models.IntegerField(verbose_name='TB_PREV (N) 6-12 months IPT', default=0)),
                ('tbprev_tc3n', models.IntegerField(verbose_name='TB_PREV (N) Female, <15', default=0)),
                ('tbprev_tc4n', models.IntegerField(verbose_name='TB_PREV (N) Female, 15+', default=0)),
                ('tbprev_tc5n', models.IntegerField(verbose_name='TB_PREV (N) Male, <15', default=0)),
                ('tbprev_tc6n', models.IntegerField(verbose_name='TB_PREV (N) Male, 15+', default=0)),
                ('tbprev_tc1d', models.IntegerField(verbose_name='TB_PREV (D) Numerator', default=0)),
                ('tbprev_tc2d', models.IntegerField(verbose_name='TB_PREV (D) 6-12 months IPT', default=0)),
                ('tbprev_tc3d', models.IntegerField(verbose_name='TB_PREV (D) Female, <15', default=0)),
                ('tbprev_tc4d', models.IntegerField(verbose_name='TB_PREV (D) Female, 15+', default=0)),
                ('tbprev_tc5d', models.IntegerField(verbose_name='TB_PREV (D) Male, <15', default=0)),
                ('tbprev_tc6d', models.IntegerField(verbose_name='TB_PREV (D) Male, 15+', default=0)),
            ],
        ),
    ]