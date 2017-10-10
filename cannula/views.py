from django.shortcuts import render

from .models import DataElement

def index(request):
    data_elements = DataElement.objects.order_by('name').all()
    return render(request, 'cannula/data_element_listing.html', {'data_elements': data_elements})

def data_element_summary(request, data_element_id):
    return render(request, 'cannula/summary.html', {})
