# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('cannula', '0018_rmnchandmalaria'),
    ]

    operations = [
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='AMTSL_D',
            new_name='AMTSLD',
        ),
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='AMTSL_N',
            new_name='AMTSLN',
        ),
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='Asphyxia_D',
            new_name='AsphyxiaD',
        ),
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='Asphyxia_N',
            new_name='AsphyxiaN',
        ),
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='Diarrhea_D',
            new_name='DiarrheaD',
        ),
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='Diarrhea_N',
            new_name='DiarrheaN',
        ),
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='HEI_D',
            new_name='HEID',
        ),
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='HEI_N',
            new_name='HEIN',
        ),
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='IPT2_D',
            new_name='IPT2D',
        ),
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='IPT2_N',
            new_name='IPT2N',
        ),
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='Pneumonia_D',
            new_name='PneumoniaD',
        ),
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='Pneumonia_N',
            new_name='PneumoniaN',
        ),
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='Ret_D',
            new_name='RetD',
        ),
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='Ret_N',
            new_name='RetN',
        ),
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='severe_malaria_D',
            new_name='severemalariaD',
        ),
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='severe_malaria_N',
            new_name='severemalariaN',
        ),
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='simple_malaria_D',
            new_name='simplemalariaD',
        ),
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='simple_malaria_N',
            new_name='simplemalariaN',
        ),
    ]
