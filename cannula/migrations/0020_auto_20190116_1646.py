# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('cannula', '0019_auto_20190116_1643'),
    ]

    operations = [
        migrations.RenameField(
            model_name='rmnchandmalaria',
            old_name='reporting_period',
            new_name='reportingperiod',
        ),
    ]
