from django.forms import ModelForm
from django.contrib.auth.models import User

from . import models

class SourceDocumentForm(ModelForm):
	class Meta:
		model = models.SourceDocument
		fields = ['file1',]


class DataElementAliasForm(ModelForm):
    def __init__(self, *args, **kwargs):
        super(DataElementAliasForm, self).__init__(*args, **kwargs)
        self.fields['name'].widget.attrs['readonly'] = True

    class Meta:
        model = models.DataElement
        fields = ['name', 'alias']

class UserProfileForm(ModelForm):
    class Meta:
        model = User
        fields = ['first_name', 'last_name', 'email']
