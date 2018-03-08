# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('cannula', '0012_auto_20180213_1434'),
    ]

    operations = [
        migrations.RenameField(
            model_name='sourcedocument',
            old_name='file',
            new_name='file1',
        ),
    ]
