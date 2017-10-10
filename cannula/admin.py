from django.contrib import admin

from .models import SourceDocument, DataElement, DataValue, load_excel_to_datavalues

def load_document_values(modeladmin, request, queryset):
    import itertools

    for doc in queryset:
        all_values = load_excel_to_datavalues(doc)
        iter_data_values = itertools.chain.from_iterable(all_values.values())
        DataValue.objects.bulk_create(iter_data_values)

load_document_values.short_description = 'Load data values from document into DB'

class SourceDocumentAdmin(admin.ModelAdmin):
    readonly_fields = ('orig_filename',)
    list_display = ['uploaded_at', 'orig_filename']
    ordering = ['uploaded_at']
    actions = [load_document_values]

class DataElementAdmin(admin.ModelAdmin):
    list_display = ['name', 'value_type']

class DataValueAdmin(admin.ModelAdmin):
    list_display = ['data_element', 'category_str', 'site_str', 'month', 'quarter', 'year', 'numeric_value']
    list_filter = ('data_element__name',)
    search_fields = ['data_element__name', 'category_str', 'site_str']

admin.site.register(SourceDocument, SourceDocumentAdmin)
admin.site.register(DataElement, DataElementAdmin)
admin.site.register(DataValue, DataValueAdmin)
