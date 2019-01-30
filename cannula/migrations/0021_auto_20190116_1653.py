# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('cannula', '0020_auto_20190116_1646'),
    ]

    operations = [
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='AMTSLD',
            new_name='AMTSL_D',
        ),
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='AMTSLN',
            new_name='AMTSL_N',
        ),
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='AsphyxiaD',
            new_name='Asphyxia_D',
        ),
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='AsphyxiaN',
            new_name='Asphyxia_N',
        ),
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='DiarrheaD',
            new_name='Diarrhea_D',
        ),
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='DiarrheaN',
            new_name='Diarrhea_N',
        ),
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='HEID',
            new_name='HEI_D',
        ),
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='HEIN',
            new_name='HEI_N',
        ),
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='IPT2D',
            new_name='IPT2_D',
        ),
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='IPT2N',
            new_name='IPT2_N',
        ),
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='PneumoniaD',
            new_name='Pneumonia_D',
        ),
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='PneumoniaN',
            new_name='Pneumonia_N',
        ),
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='RetD',
            new_name='Ret_D',
        ),
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='RetN',
            new_name='Ret_N',
        ),
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='reportingperiod',
            new_name='reporting_period',
        ),
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='severemalariaD',
            new_name='severe_malaria_D',
        ),
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='severemalariaN',
            new_name='severe_malaria_N',
        ),
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='simplemalariaD',
            new_name='simple_malaria_D',
        ),
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='simplemalariaN',
            new_name='simple_malaria_N',
        ),
    ]
