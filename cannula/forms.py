from django.forms import ModelForm

from . import models

class SourceDocumentForm(ModelForm):
	class Meta:
		model = models.SourceDocument
		fields = ['file',]
