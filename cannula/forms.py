from django.forms import ModelForm
from django.contrib.auth.models import User
from django import forms
from .models import IFASBottleneck
from .enums import Where_Identified_CHOICES

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

class BottleneckInventory(forms.ModelForm):
    bottleneck = forms.CharField(label='Bottleneck')
    level = forms.CharField(label='Level')
    when_identified = forms.DateField(label='When Identified',input_formats=['%d/%m/%Y'],required=True)
    where_identified=forms.ChoiceField(choices = Where_Identified_CHOICES, label="Where Identified", initial='', widget=forms.Select(), required=True)
    potential_solutions = forms.CharField(label='Potential Solutions')
    efforts_to_address_bottleneck=forms.CharField(label="Efforts to address the bottleneck")
    next_steps=forms.CharField(label='Next steps')
    additional_bottleneck_identified=forms.CharField(label='Additional bottleneck identified during efforts')
    comments=forms.CharField(label='Comments if any')


    class Meta:
        model=IFASBottleneck
        fields=['bottleneck','when_identified','level','where_identified','potential_solutions','efforts_to_address_bottleneck','next_steps','additional_bottleneck_identified','comments']

    def clean_bottleneck(self):
        data=self.cleaned_data['bottleneck']
        #some ifs to do some cleaning
        return data
