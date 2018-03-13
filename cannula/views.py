from django.shortcuts import render, get_object_or_404, render_to_response, redirect
from django.db.models import Avg, Case, Count, F, Max, Min, Prefetch, Q, Sum, When
from django.db.models import Value, CharField
from django.db.models.functions import Substr
from django.contrib.auth.decorators import login_required
from django.http import Http404
from django.template import RequestContext
from django.core.urlresolvers import reverse

from datetime import date
from decimal import Decimal
from itertools import groupby, tee, chain, product

from . import dateutil, grabbag
from .grabbag import default_zero, all_not_none

from .models import DataElement, OrgUnit, DataValue, ValidationRule, SourceDocument
from .forms import SourceDocumentForm, DataElementAliasForm

from .dashboards import LegendSet

@login_required
def index(request):
    context = {
        'validation_rules': ValidationRule.objects.all().values_list('id', 'name')
    }
    return render(request, 'cannula/index.html', context)

@login_required
def data_elements(request):
    data_elements = DataElement.objects.order_by('name').all()
    return render(request, 'cannula/data_element_listing.html', {'data_elements': data_elements})

# avoid strange behaviour from itertools.groupby by evaluating all the group iterables as lists
def groupbylist(*args, **kwargs):
    return [[k, list(g)] for k, g in groupby(*args, **kwargs)]

def filter_empty_rows(grouped_vals):
    for row in grouped_vals:
        row_heading, row_values = row
        if any(v['numeric_sum'] for v in row_values):
            yield row

def month2quarter(month_num):
    return ((month_num-1)//3+1)

def make_excel_url(request_path):
    import os
    import urllib

    parts = urllib.parse.urlparse(request_path)
    a, b, path, *others = parts
    excel_path = ''.join([os.path.splitext(path)[0], '.xls'])
    return urllib.parse.urlunparse([a, b, excel_path, *others])

@login_required
def ipt_quarterly(request, output_format='HTML'):
    ipt_de_names = (
        '105-2.1 A6:First dose IPT (IPT1)',
        '105-2.1 A7:Second dose IPT (IPT2)',
    )

    this_day = date.today()
    this_year = this_day.year
    PREV_5YR_QTRS = ['%d-Q%d' % (y, q) for y in range(this_year, this_year-6, -1) for q in range(4, 0, -1)]
    DISTRICT_LIST = list(OrgUnit.objects.filter(level=1).order_by('name').values_list('name', flat=True))

    if 'period' in request.GET and request.GET['period'] in PREV_5YR_QTRS:
        filter_period=request.GET['period']
    else:
        filter_period = '%d-Q%d' % (this_year, month2quarter(this_day.month))

    period_desc = dateutil.DateSpan.fromquarter(filter_period).format()

    if 'district' in request.GET and request.GET['district'] in DISTRICT_LIST:
        filter_district = OrgUnit.objects.get(name=request.GET['district'])
    else:
        filter_district = None

    # get IPT1 and IPT2 without subcategory disaggregation
    qs = DataValue.objects.what(*ipt_de_names).filter(quarter=filter_period)
    if filter_district:
        qs = qs.where(filter_district)
    # use clearer aliases for the unwieldy names
    qs = qs.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'))
    qs = qs.annotate(period=F('quarter')) # TODO: review if this can still work with different periods
    qs = qs.order_by('district', 'subcounty', 'de_name', 'period')
    val_dicts = qs.values('district', 'subcounty', 'de_name', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    
    # all subcounties (or equivalent)
    qs_ou = OrgUnit.objects.filter(level=2).annotate(district=F('parent__name'), subcounty=F('name'))
    if filter_district:
        qs_ou = qs_ou.filter(Q(lft__gte=filter_district.lft) & Q(rght__lte=filter_district.rght))
    ou_list = qs_ou.values_list('district', 'subcounty')
    ou_headers = ['District', 'Subcounty']

    def val_fun(row, col):
        return { 'district': row[0], 'subcounty': row[1], 'de_name': col, 'numeric_sum': None }
    gen_raster = grabbag.rasterize(ou_list, ipt_de_names, val_dicts, lambda x: (x['district'], x['subcounty']), lambda x: x['de_name'], val_fun)
    val_dicts = list(gen_raster)

    # get list of subcategories for IPT2
    qs_ipt_subcat = DataValue.objects.what('105-2.1 A7:Second dose IPT (IPT2)').order_by('category_combo__name').values_list('de_name', 'category_combo__name').distinct()
    subcategory_names = tuple(qs_ipt_subcat)

    # get IPT2 with subcategory disaggregation
    qs2 = DataValue.objects.what('105-2.1 A7:Second dose IPT (IPT2)').filter(quarter=filter_period)
    if filter_district:
        qs2 = qs2.where(filter_district)
    # use clearer aliases for the unwieldy names
    qs2 = qs2.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'))
    qs2 = qs2.annotate(period=F('quarter')) # TODO: review if this can still work with different periods
    qs2 = qs2.annotate(cat_combo=F('category_combo__name'))
    qs2 = qs2.order_by('district', 'subcounty', 'de_name', 'period', 'cat_combo')
    val_dicts2 = qs2.values('district', 'subcounty', 'de_name', 'period', 'cat_combo').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))

    def val_with_subcat_fun(row, col):
        district, subcounty = row
        de_name, subcategory = col
        return { 'district': district, 'subcounty': subcounty, 'cat_combo': subcategory, 'de_name': de_name, 'numeric_sum': None }
    gen_raster = grabbag.rasterize(ou_list, subcategory_names, val_dicts2, lambda x: (x['district'], x['subcounty']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_dicts2 = list(gen_raster)

    # get expected pregnancies
    qs3 = DataValue.objects.what('Expected Pregnancies')
    if filter_district:
        qs3 = qs3.where(filter_district)
    # use clearer aliases for the unwieldy names
    qs3 = qs3.annotate(district=F('org_unit__parent__name'), subcounty=F('org_unit__name'))
    qs3 = qs3.annotate(period=F('year')) # TODO: review if this can still work with different periods
    qs3 = qs3.order_by('district', 'subcounty', 'de_name', 'period')
    val_dicts3 = qs3.values('district', 'subcounty', 'de_name', 'period').annotate(numeric_sum=(Sum('numeric_value')/4))

    gen_raster = grabbag.rasterize(ou_list, ('Expected Pregnancies',), val_dicts3, lambda x: (x['district'], x['subcounty']), lambda x: x['de_name'], val_fun)
    val_dicts3 = list(gen_raster)

    # combine the data and group by district and subcounty
    grouped_vals = groupbylist(sorted(chain(val_dicts3, val_dicts, val_dicts2), key=lambda x: (x['district'], x['subcounty'])), key=lambda x: (x['district'], x['subcounty']))
    if True:
        grouped_vals = list(filter_empty_rows(grouped_vals))
    
    # calculate the IPT rate for the IPT1/IPT2 values (without subcategories)
    for _group in grouped_vals:
        (district_subcounty, (preg_val, *other_vals)) = _group
        if preg_val['de_name'] == 'Expected Pregnancies':
            for val in other_vals:
                if val['de_name'] in ipt_de_names and 'cat_combo' not in val:
                    pregnancies_per_annum = preg_val['numeric_sum']
                    if pregnancies_per_annum and pregnancies_per_annum != 0 and val['numeric_sum']:
                        val['ipt_rate'] = val['numeric_sum']*100/pregnancies_per_annum
                    else:
                        val['ipt_rate'] = None

    data_element_names = list()
    data_element_names.insert(0, ('Expected Pregnancies', None))
    for de_n in ipt_de_names:
        data_element_names.append((de_n, None))
        data_element_names.append(('%', None))
    data_element_names.extend(subcategory_names)

    legend_sets = list()
    ipt_ls = LegendSet()
    ipt_ls.name = 'IPT rate'
    ipt_ls.add_interval('yellow', 0, 71)
    ipt_ls.add_interval('green', 71, None)
    ipt_ls.mappings[4] = True
    ipt_ls.mappings[6] = True
    legend_sets.append(ipt_ls)

    if output_format == 'EXCEL':
        from django.http import HttpResponse
        import openpyxl

        wb = openpyxl.workbook.Workbook()
        ws = wb.active # workbooks are created with at least one worksheet
        ws.title = 'Sheet1' # unfortunately it is named "Sheet" not "Sheet1"
        ws.page_setup.orientation = ws.ORIENTATION_LANDSCAPE
        ws.page_setup.paperSize = ws.PAPERSIZE_A4

        headers = ou_headers + data_element_names
        for i, name in enumerate(headers, start=1):
            c = ws.cell(row=1, column=i)
            if not isinstance(name, tuple):
                c.value = str(name)
            else:
                de, cat_combo = name
                if cat_combo is None:
                    c.value = str(de)
                else:
                    c.value = str(de) + '\n' + str(cat_combo)
        for i, g in enumerate(grouped_vals, start=2):
            ou_path, g_val_list = g
            for col_idx, ou in enumerate(ou_path, start=1):
                ws.cell(row=i, column=col_idx, value=ou)
            offset = 0
            for j, g_val in enumerate(g_val_list, start=len(ou_path)+1):
                ws.cell(row=i, column=j+offset, value=g_val['numeric_sum'])
                if 'ipt_rate' in g_val:
                    offset += 1
                    ws.cell(row=i, column=j+offset, value=g_val['ipt_rate'])

        for ls in legend_sets:
            # apply conditional formatting from LegendSet
            for rule in ls.openpyxl_rules():
                for cell_range in ls.excel_ranges():
                    ws.conditional_formatting.add(cell_range, rule)


        response = HttpResponse(openpyxl.writer.excel.save_virtual_workbook(wb), content_type='application/vnd.ms-excel')
        response['Content-Disposition'] = 'attachment; filename="malaria_ipt_scorecard.xlsx"'

        return response

    context = {
        'grouped_data': grouped_vals,
        'data_element_names': data_element_names,
        'legend_sets': legend_sets,
        'period_desc': period_desc,
        'period_list': PREV_5YR_QTRS,
        'district_list': DISTRICT_LIST,
    }

    if output_format == 'JSON':
        from django.http import JsonResponse
        
        return JsonResponse(context)

    return render(request, 'cannula/ipt_quarterly.html', context)

@login_required
def malaria_compliance(request):
    cases_de_names = (
        '105-1.3 OPD Malaria (Total)',
        '105-1.3 OPD Malaria Confirmed (Microscopic & RDT)',
    )

    this_day = date.today()
    this_year = this_day.year
    PREV_5YR_QTRS = ['%d-Q%d' % (y, q) for y in range(this_year, this_year-6, -1) for q in range(4, 0, -1)]
    DISTRICT_LIST = list(OrgUnit.objects.filter(level=1).order_by('name').values_list('name', flat=True))

    if 'start_period' in request.GET and request.GET['start_period'] in PREV_5YR_QTRS and 'end_period' in request.GET and request.GET['end_period']:
        start_quarter = request.GET['start_period']
        end_quarter = request.GET['end_period']
    else: # default to "immediate preceding quarter" and "this quarter"
        if this_day.month <= 3:
            start_year = this_year - 1
            start_month = (this_day.month - 3 + 12)
            end_month = this_day.month
        else:
            start_year = this_year
            start_month = this_day.month - 3
            end_month = this_day.month
        start_quarter = '%d-Q%d' % (start_year, month2quarter(start_month))
        end_quarter = '%d-Q%d' % (this_year, month2quarter(end_month))

    periods = dateutil.get_quarters(start_quarter, end_quarter)
    if start_quarter == end_quarter:
        periods = periods[:1]

    if 'district' in request.GET and request.GET['district'] in DISTRICT_LIST:
        filter_district = OrgUnit.objects.get(name=request.GET['district'])
    else:
        filter_district = None
    
    # all facilities (or equivalent)
    qs_ou = OrgUnit.objects.filter(level=3).annotate(district=F('parent__parent__name'), subcounty=F('parent__name'), facility=F('name'))
    if filter_district:
        qs_ou = qs_ou.filter(Q(lft__gte=filter_district.lft) & Q(rght__lte=filter_district.rght))
    ou_list = qs_ou.values_list('district', 'subcounty', 'facility')

    # get data values without subcategory disaggregation
    qs = DataValue.objects.what(*cases_de_names)
    if filter_district:
        qs = qs.where(filter_district)
    qs = qs.filter(quarter__gte=start_quarter).filter(quarter__lte=end_quarter)
    # use clearer aliases for the unwieldy names
    qs = qs.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs = qs.annotate(period=F('quarter')) # TODO: review if this can still work with different periods
    qs = qs.order_by('district', 'subcounty', 'facility', 'de_name', 'period')
    val_dicts = qs.values('district', 'subcounty', 'facility', 'de_name', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))

    def val_with_period_fun(row, col):
        district, subcounty, facility = row
        de_name, period = col
        return { 'district': district, 'subcounty': subcounty, 'facility': facility, 'period': period, 'de_name': de_name, 'numeric_sum': None }
    gen_raster = grabbag.rasterize(ou_list, tuple(product(cases_de_names, periods)), val_dicts, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['period']), val_with_period_fun)
    val_dicts = gen_raster

    # combine the data and group by district and subcounty
    grouped_vals = groupbylist(sorted(val_dicts, key=lambda x: (x['district'], x['subcounty'], x['facility'])), key=lambda x: (x['district'], x['subcounty'], x['facility']))
    if True:
        grouped_vals = list(filter_empty_rows(grouped_vals))

    for _group in grouped_vals:
        (district_subcounty_facility, other_vals) = _group
        malaria_totals = dict()
        for val in other_vals:
            if val['de_name'] == cases_de_names[0]:
                malaria_totals[val['period']] = val['numeric_sum']
            elif val['de_name'] == cases_de_names[1]:
                total_cases = malaria_totals.get(val['period'], 0)
                confirmed_cases = val['numeric_sum']
                if confirmed_cases and total_cases and total_cases != 0:
                    confirmed_rate = confirmed_cases * 100 / total_cases
                    val['rdt_rate'] = confirmed_rate
                else:
                    val['rdt_rate'] = None

    data_element_names = list()
    for de_n in cases_de_names:
        data_element_names.append((de_n, None))

    legend_sets = list()
    compliance_ls = LegendSet()
    compliance_ls.name = 'Compliance'
    compliance_ls.add_interval('green', 80, None)
    legend_sets.append(compliance_ls)

    context = {
        'grouped_data': grouped_vals,
        'data_element_names': data_element_names,
        'legend_sets': legend_sets,
        'start_period': start_quarter,
        'end_period': end_quarter,
        'periods': periods,
        'period_desc': dateutil.DateSpan.fromquarter(start_quarter).combine(dateutil.DateSpan.fromquarter(end_quarter)).format_long(),
        'period_list': PREV_5YR_QTRS,
        'district_list': DISTRICT_LIST,
    }

    return render(request, 'cannula/malaria_compliance.html', context)

@login_required
def data_workflow_new(request):
    if request.method == 'POST':
        form = SourceDocumentForm(request.POST, request.FILES)
        if form.is_valid():
            form.save()
            return redirect('data_workflow_listing')
    else:
        form = SourceDocumentForm()

    context = {
        'form': form,
    }

    return render_to_response('cannula/data_workflow_new.html', context, context_instance=RequestContext(request))

@login_required
def data_workflow_detail(request):
    from .models import load_excel_to_datavalues, load_excel_to_validations

    if 'wf_id' in request.GET:
        src_doc_id = int(request.GET['wf_id'])
        src_doc = get_object_or_404(SourceDocument, id=src_doc_id)

        if request.method == 'POST':
            if 'load_values' in request.POST:
                all_values = load_excel_to_datavalues(src_doc)
                for site_name, site_vals in all_values.items():
                    DataValue.objects.bulk_create(site_vals)
            elif 'load_validations' in request.POST:
                load_excel_to_validations(src_doc)

            #TODO: redirect with to detail page?

        qs_vals = DataValue.objects.filter(source_doc__id=src_doc_id).values('id')
        doc_elements = DataElement.objects.filter(data_values__id__in=qs_vals).distinct('id')
        doc_rules = ValidationRule.objects.filter(data_elements__data_values__id__in=qs_vals).distinct('id')
        num_values = qs_vals.count()
    else:
        raise Http404("Workflow does not exist or workflow id is missing/invalid")

    context = {
        'srcdoc': src_doc,
        'num_values': num_values,
        'data_elements': doc_elements,
        'validation_rules': doc_rules,
    }

    return render(request, 'cannula/data_workflow_detail.html', context)

@login_required
def data_workflow_listing(request):
    # TODO: filter based on user who uploaded file?
    docs = SourceDocument.objects.all().annotate(num_values=Count('data_values'))
    docs = docs.order_by('uploaded_at')

    context = {
        'workflows': docs,
    }
    return render(request, 'cannula/data_workflow_listing.html', context)

def dictfetchall(cursor):
    "Return all rows from a cursor as a dict"
    columns = [col[0] for col in cursor.description]
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]

@login_required
def validation_rule(request, output_format='HTML'):
    from django.db import connection
    cursor = connection.cursor()
    vr_id = int(request.GET['id'])
    vr = ValidationRule.objects.get(id=vr_id)
    DISTRICT_LIST = list(OrgUnit.objects.filter(level=1).order_by('name').values_list('name', flat=True))

    if 'district' in request.GET and request.GET['district'] in DISTRICT_LIST:
        filter_district = OrgUnit.objects.get(name=request.GET['district'])
    else:
        filter_district = None

    if filter_district:
        cursor.execute('SELECT * FROM %s WHERE district=\'%s\'' % (vr.view_name(), filter_district.name))
    else:
        cursor.execute('SELECT * FROM %s' % (vr.view_name(),))

    columns = [col[0] for col in cursor.description]
    de_name_map = dict()
    for de_id, de_name in DataElement.objects.all().values_list('id', 'name'):
        de_name_map['de_%d' % (de_id,)] = de_name
        columns = [c.replace('de_%d' % (de_id,), de_name) for c in columns] #TODO: can we include the alias, if there is one?
    results = dictfetchall(cursor)
    for r in results:
        r['data_values'] = dict()
        for k,v in r.items():
            if k in de_name_map:
                de_name = de_name_map[k]
                r['data_values'][de_name] = v
    if 'exclude_true' in request.GET:
        results = filter(lambda x: not x['de_calc_1'], results)

    validates_ls = LegendSet()
    validates_ls.add_interval('red', None, 1)
    validates_ls.add_interval('green', 1, None)
    validates_ls.mappings[4] = True

    if output_format == 'EXCEL':
        from django.http import HttpResponse
        import openpyxl

        wb = openpyxl.workbook.Workbook()
        ws = wb.active # workbooks are created with at least one worksheet
        ws.title = vr.expression().strip()[:31] # worksheet names length limit is 31
        ws.page_setup.orientation = ws.ORIENTATION_LANDSCAPE
        ws.page_setup.paperSize = ws.PAPERSIZE_A4

        def format_values(v_dict):
            return '\n'.join([ '%s: %s' % (k,v) for k,v in v_dict.items()])

        headers = ['Period', 'District', 'Subcounty', 'Facility', 'Validates?', 'Source Data']
        for i, name in enumerate(headers, start=1):
            c = ws.cell(row=1, column=i)
            if not isinstance(name, tuple):
                c.value = str(name)
            else:
                de, cat_combo = name
                if cat_combo is None:
                    c.value = str(de)
                else:
                    c.value = str(de) + '\n' + str(cat_combo)
        for i, res in enumerate(results, start=2):
            ws.cell(row=i, column=1, value=next(filter(lambda x: x is not None, (res['month'], res['quarter'], res['year'])), None))
            ws.cell(row=i, column=2, value=res['district'])
            ws.cell(row=i, column=3, value=res['subcounty'])
            ws.cell(row=i, column=4, value=res['facility'])
            ws.cell(row=i, column=5, value=res['de_calc_1'])
            ws.cell(row=i, column=6, value=format_values(res['data_values']))

        for rule in validates_ls.openpyxl_rules():
            # apply conditional formatting from LegendSet
            for xls_range in validates_ls.excel_ranges():
                ws.conditional_formatting.add(xls_range, rule)


        response = HttpResponse(openpyxl.writer.excel.save_virtual_workbook(wb), content_type='application/vnd.ms-excel')
        response['Content-Disposition'] = 'attachment; filename="%s_validation.xlsx"' % (vr.name.lower(),)

        return response

    context = {
        'results': results,
        'columns': columns,
        'rule': vr,
        'district_list': DISTRICT_LIST,
        'excel_url': make_excel_url(request.path)
    }

    return render(request, 'cannula/validation_rule.html', context)

@login_required
def data_element_alias(request):
    if 'de_id' in request.GET:
        de_id = int(request.GET['de_id'])
        de = get_object_or_404(DataElement, id=de_id)

        if request.method == 'POST':
            form = DataElementAliasForm(request.POST, instance=de)
            if form.is_valid():
                form.save()
                de_url = '%s?wf_id=%d' % (reverse('data_workflow_detail'), int(request.GET['wf_id']))
                return redirect(de_url, de)
        else:
            form = DataElementAliasForm(instance=de)

        context = {
            'form': form,
        }
    else:
        raise Http404("Data Element does not exist or data element id is missing/invalid")

    return render_to_response('cannula/data_element_edit_alias.html', context, context_instance=RequestContext(request))

@login_required
def hts_by_site(request, output_format='HTML'):
    this_day = date.today()
    this_year = this_day.year
    PREV_5YR_QTRS = ['%d-Q%d' % (y, q) for y in range(this_year, this_year-6, -1) for q in range(4, 0, -1)]
    DISTRICT_LIST = list(OrgUnit.objects.filter(level=1).order_by('name').values_list('name', flat=True))

    if 'period' in request.GET and request.GET['period'] in PREV_5YR_QTRS:
        filter_period=request.GET['period']
    else:
        filter_period = '%d-Q%d' % (this_year, month2quarter(this_day.month))

    period_desc = dateutil.DateSpan.fromquarter(filter_period).format()

    if 'district' in request.GET and request.GET['district'] in DISTRICT_LIST:
        filter_district = OrgUnit.objects.get(name=request.GET['district'])
    else:
        filter_district = None

    hts_de_names = (
        '105-4 Number of clients who have been linked to care',
        '105-4 Number of Individuals who received HIV test results',
        '105-4 Number of Individuals who tested HIV positive',
    )
    hts_short_names = (
        'Linked',
        'Tested',
        'HIV+',
    )
    subcategory_names = ['(<15, Female)', '(<15, Male)', '(15+, Female)', '(15+, Male)']
    de_positivity_meta = list(product(hts_de_names, subcategory_names))

    qs_positivity = DataValue.objects.what(*hts_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_positivity = qs_positivity.where(filter_district)

    cc_lt_15 = ['18 Mths-<5 Years', '5-<10 Years', '10-<15 Years']
    cc_ge_15 = ['15-<19 Years', '19-<49 Years', '>49 Years']
    #TODO: cc_lt_15_f = CategoryCombo.from_cat_names(['Female', '<15']) gives a CategoryCombo instance that makes the Case statement clearer/safer
    qs_positivity = qs_positivity.annotate(
        cat_combo=Case(
            When(Q(category_combo__categories__name__in=cc_lt_15) & Q(category_combo__name__contains='Female'), then=Value(subcategory_names[0])),
            When(Q(category_combo__categories__name__in=cc_lt_15) & ~Q(category_combo__name__contains='Female'), then=Value(subcategory_names[1])),
            When(Q(category_combo__categories__name__in=cc_ge_15) & Q(category_combo__name__contains='Female'), then=Value(subcategory_names[2])),
            When(Q(category_combo__categories__name__in=cc_ge_15) & ~Q(category_combo__name__contains='Female'), then=Value(subcategory_names[3])),
            default=None, output_field=CharField()
        )
    )
    qs_positivity = qs_positivity.exclude(cat_combo__iexact=None)

    qs_positivity = qs_positivity.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_positivity = qs_positivity.annotate(period=F('quarter'))
    qs_positivity = qs_positivity.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_positivity = qs_positivity.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    
    # # all facilities (or equivalent)
    qs_ou = OrgUnit.objects.filter(level=3).annotate(district=F('parent__parent__name'), subcounty=F('parent__name'), facility=F('name'))
    if filter_district:
        qs_ou = qs_ou.filter(Q(lft__gte=filter_district.lft) & Q(rght__lte=filter_district.rght))
    ou_list = list(qs_ou.values_list('district', 'subcounty', 'facility'))
    ou_headers = ['District', 'Subcounty', 'Facility']

    def val_with_subcat_fun(row, col):
        district, subcounty, facility = row
        de_name, subcategory = col
        return { 'district': district, 'subcounty': subcounty, 'facility': facility, 'cat_combo': subcategory, 'de_name': de_name, 'numeric_sum': None }
    gen_raster = grabbag.rasterize(ou_list, de_positivity_meta, val_positivity, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_positivity2 = list(gen_raster)

    pmtct_mother_de_names = (
        '105-2.1 Pregnant Women newly tested for HIV this pregnancy(TR & TRR)',
        '105-2.2a Women tested for HIV in labour (1st time this Pregnancy)',
        '105-2.3a Breastfeeding mothers tested for HIV(1st test)',
    )
    de_pmtct_mother_meta = list(product(('Pregnant Women tested for HIV',), (None,)))

    qs_pmtct_mother = DataValue.objects.what(*pmtct_mother_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_pmtct_mother = qs_pmtct_mother.where(filter_district)
    qs_pmtct_mother = qs_pmtct_mother.annotate(de_name=Value('Pregnant Women tested for HIV', output_field=CharField()))
    qs_pmtct_mother = qs_pmtct_mother.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_pmtct_mother = qs_pmtct_mother.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_pmtct_mother = qs_pmtct_mother.annotate(period=F('quarter'))
    qs_pmtct_mother = qs_pmtct_mother.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_pmtct_mother = qs_pmtct_mother.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))

    gen_raster = grabbag.rasterize(ou_list, de_pmtct_mother_meta, val_pmtct_mother, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_pmtct_mother2 = list(gen_raster)

    pmtct_mother_pos_de_names = (
        '105-2.1 A19:Pregnant Women testing HIV+ on a retest (TRR+)',
        '105-2.2a Women testing HIV+ in labour (1st time this Pregnancy)',
        '105-2.2b Women testing HIV+ in labour (Retest this Pregnancy)',
        '105-2.3a Breastfeeding mothers newly testing HIV+(1st test)',
        '105-2.3b Breastfeeding mothers newly testing HIV+(retest)',
    )
    de_pmtct_mother_pos_meta = list(product(('Pregnant Women testing HIV+',), (None,)))

    qs_pmtct_mother_pos = DataValue.objects.what(*pmtct_mother_pos_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_pmtct_mother_pos = qs_pmtct_mother_pos.where(filter_district)
    qs_pmtct_mother_pos = qs_pmtct_mother_pos.annotate(de_name=Value('Pregnant Women testing HIV+', output_field=CharField()))
    qs_pmtct_mother_pos = qs_pmtct_mother_pos.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_pmtct_mother_pos = qs_pmtct_mother_pos.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_pmtct_mother_pos = qs_pmtct_mother_pos.annotate(period=F('quarter'))
    qs_pmtct_mother_pos = qs_pmtct_mother_pos.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_pmtct_mother_pos = qs_pmtct_mother_pos.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))

    gen_raster = grabbag.rasterize(ou_list, de_pmtct_mother_pos_meta, val_pmtct_mother_pos, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_pmtct_mother_pos2 = list(gen_raster)

    pmtct_child_de_names = (
        '105-2.4a Exposed Infants Tested for HIV Below 18 Months(by 1st PCR) ',
        '105-2.4b 1st DNA PCR result returned(HIV+)',
        '105-2.4b 2nd DNA PCR result returned(HIV+)',
        '105-2.1a Male partners received HIV test results in eMTCT(Total)',
        '105-2.1b Male partners received HIV test results in eMTCT(HIV+)',
    )
    pmtct_child_short_names = (
        'PMTCT INFANT HIV+',
        'PMTCT CHILD PCR1 HIV+',
        'PMTCT CHILD PCR2 HIV+',
        'PMTCT MALE PARTNERS TESTED',
        'PMTCT MALE PARTNERS HIV+',
    )
    de_pmtct_child_meta = list(product(pmtct_child_de_names, (None,)))

    qs_pmtct_child = DataValue.objects.what(*pmtct_child_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_pmtct_child = qs_pmtct_child.where(filter_district)
    qs_pmtct_child = qs_pmtct_child.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_pmtct_child = qs_pmtct_child.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_pmtct_child = qs_pmtct_child.annotate(period=F('quarter'))
    qs_pmtct_child = qs_pmtct_child.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_pmtct_child = qs_pmtct_child.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_pmtct_child = list(val_pmtct_child)

    gen_raster = grabbag.rasterize(ou_list, de_pmtct_child_meta, val_pmtct_child, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_pmtct_child2 = list(gen_raster)

    target_de_names = (
        'HTC_TST_TARGET',
        'HTC_TST_POS_TARGET',
    )
    de_target_meta = list(product(target_de_names, subcategory_names))

    # targets are annual, so filter by year component of period and divide result by 4 to get quarter
    qs_target = DataValue.objects.what(*target_de_names).filter(year=filter_period[:4])
    if filter_district:
        qs_target = qs_target.where(filter_district)

    qs_target = qs_target.annotate(cat_combo=F('category_combo__name'))
    qs_target = qs_target.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_target = qs_target.annotate(period=F('quarter'))
    qs_target = qs_target.order_by('district', 'subcounty', 'facility', '-de_name', 'cat_combo', 'period') # note reversed order of data element names
    val_target = qs_target.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value')/4)

    gen_raster = grabbag.rasterize(ou_list, de_target_meta, val_target, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_target2 = list(gen_raster)

    # combine the data and group by district, subcounty and facility
    grouped_vals = groupbylist(sorted(chain(val_positivity2, val_pmtct_mother2, val_pmtct_mother_pos2, val_pmtct_child2, val_target2), key=lambda x: (x['district'], x['subcounty'], x['facility'])), key=lambda x: (x['district'], x['subcounty'], x['facility']))
    if True:
        grouped_vals = list(filter_empty_rows(grouped_vals))

    # perform calculations
    for _group in grouped_vals:
        (district_subcounty_facility, (linked_under15_f, linked_under15_m, linked_over15_f, linked_over15_m, tst_under15_f, tst_under15_m, tst_over15_f, tst_over15_m, pos_under15_f, pos_under15_m, pos_over15_f, pos_over15_m, tst_pregnant, pos_pregnant, pos_infant, pos_pcr1, pos_pcr2, tst_male_partner, pos_male_partner, *other_vals)) = _group
        
        calculated_vals = list()

        under15_f_sum = default_zero(tst_under15_f['numeric_sum']) + Decimal(default_zero(pos_infant['numeric_sum'])/2)
        under15_f_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'Tested',
            'cat_combo': '(<15, Female)',
            'numeric_sum': under15_f_sum,
        }
        calculated_vals.append(under15_f_val)
        
        under15_m_sum = default_zero(tst_under15_m['numeric_sum']) + Decimal(default_zero(pos_infant['numeric_sum'])/2)
        under15_m_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'Tested',
            'cat_combo': '(<15, Male)',
            'numeric_sum': under15_m_sum,
        }
        calculated_vals.append(under15_m_val)
        
        over15_f_sum = default_zero(tst_over15_f['numeric_sum']) + default_zero(tst_pregnant['numeric_sum'])
        over15_f_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'Tested',
            'cat_combo': '(15+, Female)',
            'numeric_sum': over15_f_sum,
        }
        calculated_vals.append(over15_f_val)
        
        over15_m_sum = default_zero(tst_over15_m['numeric_sum']) + default_zero(tst_male_partner['numeric_sum'])
        over15_m_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'Tested',
            'cat_combo': '(15+, Male)',
            'numeric_sum': over15_m_sum,
        }
        calculated_vals.append(over15_m_val)
        
        half_pos_pcr = Decimal(default_zero(pos_pcr1['numeric_sum']) + default_zero(pos_pcr1['numeric_sum']))/2
        pos_under15_f_sum = default_zero(pos_under15_f['numeric_sum']) + half_pos_pcr
        pos_under15_f_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'HIV+',
            'cat_combo': '(<15, Female)',
            'numeric_sum': pos_under15_f_sum,
        }
        calculated_vals.append(pos_under15_f_val)
        
        pos_under15_m_sum = default_zero(pos_under15_m['numeric_sum']) + half_pos_pcr
        pos_under15_m_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'HIV+',
            'cat_combo': '(<15, Male)',
            'numeric_sum': pos_under15_m_sum,
        }
        calculated_vals.append(pos_under15_m_val)
        
        pos_over15_f_sum = default_zero(pos_over15_f['numeric_sum']) + Decimal(default_zero(pos_pregnant['numeric_sum']))
        pos_over15_f_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'HIV+',
            'cat_combo': '(15+, Female)',
            'numeric_sum': pos_over15_f_sum,
        }
        calculated_vals.append(pos_over15_f_val)
        
        pos_over15_m_sum = default_zero(pos_over15_m['numeric_sum']) + Decimal(default_zero(pos_male_partner['numeric_sum']))
        pos_over15_m_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'HIV+',
            'cat_combo': '(15+, Male)',
            'numeric_sum': pos_over15_m_sum,
        }
        calculated_vals.append(pos_over15_m_val)

        tested_total = sum([under15_f_sum, under15_m_sum, over15_f_sum, over15_m_sum])
        pos_total = sum([pos_under15_f_sum, pos_under15_m_sum, pos_over15_f_sum, pos_over15_m_sum])
        tested_total_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'Tested',
            'cat_combo': None,
            'numeric_sum': tested_total,
        }
        pos_total_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'HIV+',
            'cat_combo': None,
            'numeric_sum': pos_total,
        }
        calculated_vals.append(tested_total_val)
        calculated_vals.append(pos_total_val)

        # copy linked to care totals over
        calculated_vals.append(linked_under15_f)
        calculated_vals.append(linked_under15_m)
        calculated_vals.append(linked_over15_f)
        calculated_vals.append(linked_over15_m)

        # calculate the percentages
        target_under15_f, target_under15_m, target_over15_f, target_over15_m, target_pos_under15_f, target_pos_under15_m, target_pos_over15_f, target_pos_over15_m, *further_vals = other_vals

        if all_not_none(under15_f_sum, target_under15_f['numeric_sum']) and target_under15_f['numeric_sum']:
            under15_f_percent = (under15_f_sum * 100) / target_under15_f['numeric_sum']
        else:
            under15_f_percent = None
        under15_f_percent_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'Tested (%)',
            'cat_combo': '(<15, Female)',
            'numeric_sum': under15_f_percent,
        }
        calculated_vals.append(under15_f_percent_val)

        if all_not_none(under15_m_sum, target_under15_m['numeric_sum']) and target_under15_m['numeric_sum']:
            under15_m_percent = (under15_m_sum * 100) / target_under15_m['numeric_sum']
        else:
            under15_m_percent = None
        under15_m_percent_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'Tested (%)',
            'cat_combo': '(<15, Male)',
            'numeric_sum': under15_m_percent,
        }
        calculated_vals.append(under15_m_percent_val)

        if all_not_none(over15_f_sum, target_over15_f['numeric_sum']) and target_over15_f['numeric_sum']:
            over15_f_percent = (over15_f_sum * 100) / target_over15_f['numeric_sum']
        else:
            over15_f_percent = None
        over15_f_percent_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'Tested (%)',
            'cat_combo': '(15+, Female)',
            'numeric_sum': over15_f_percent,
        }
        calculated_vals.append(over15_f_percent_val)

        if all_not_none(over15_m_sum, target_over15_m['numeric_sum']) and target_over15_m['numeric_sum']:
            over15_m_percent = (over15_m_sum * 100) / target_over15_m['numeric_sum']
        else:
            over15_m_percent = None
        over15_m_percent_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'Tested (%)',
            'cat_combo': '(15+, Male)',
            'numeric_sum': over15_m_percent,
        }
        calculated_vals.append(over15_m_percent_val)

        if all_not_none(pos_under15_f_sum, target_pos_under15_f['numeric_sum']) and target_pos_under15_f['numeric_sum']:
            pos_under15_f_percent = (pos_under15_f_sum * 100) / target_pos_under15_f['numeric_sum']
        else:
            pos_under15_f_percent = None
        pos_under15_f_percent_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'HIV+ (%)',
            'cat_combo': '(<15, Female)',
            'numeric_sum': pos_under15_f_percent,
        }
        calculated_vals.append(pos_under15_f_percent_val)

        if all_not_none(pos_under15_m_sum, target_pos_under15_m['numeric_sum']) and target_pos_under15_m['numeric_sum']:
            pos_under15_m_percent = (pos_under15_m_sum * 100) / target_pos_under15_m['numeric_sum']
        else:
            pos_under15_m_percent = None
        pos_under15_m_percent_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'HIV+ (%)',
            'cat_combo': '(<15, Male)',
            'numeric_sum': pos_under15_m_percent,
        }
        calculated_vals.append(pos_under15_m_percent_val)

        if all_not_none(pos_over15_f_sum, target_pos_over15_f['numeric_sum']) and target_pos_over15_f['numeric_sum']:
            pos_over15_f_percent = (pos_over15_f_sum * 100) / target_pos_over15_f['numeric_sum']
        else:
            pos_over15_f_percent = None
        pos_over15_f_percent_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'HIV+ (%)',
            'cat_combo': '(15+, Female)',
            'numeric_sum': pos_over15_f_percent,
        }
        calculated_vals.append(pos_over15_f_percent_val)

        if all_not_none(pos_over15_m_sum, target_pos_over15_m['numeric_sum']) and target_pos_over15_m['numeric_sum']:
            pos_over15_m_percent = (pos_over15_m_sum * 100) / target_pos_over15_m['numeric_sum']
        else:
            pos_over15_m_percent = None
        pos_over15_m_percent_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'HIV+ (%)',
            'cat_combo': '(15+, Male)',
            'numeric_sum': pos_over15_m_percent,
        }
        calculated_vals.append(pos_over15_m_percent_val)

        if all_not_none(linked_under15_f['numeric_sum'], pos_under15_f['numeric_sum']) and pos_under15_f['numeric_sum']:
            linked_under15_f_percent = (linked_under15_f['numeric_sum'] * 100) / pos_under15_f['numeric_sum']
        else:
            linked_under15_f_percent = None
        linked_under15_f_percent_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'Linked (%)',
            'cat_combo': '(<15, Female)',
            'numeric_sum': linked_under15_f_percent,
        }
        calculated_vals.append(linked_under15_f_percent_val)

        if all_not_none(linked_under15_m['numeric_sum'], pos_under15_m['numeric_sum']) and pos_under15_m['numeric_sum']:
            linked_under15_m_percent = (linked_under15_m['numeric_sum'] * 100) / pos_under15_m['numeric_sum']
        else:
            linked_under15_m_percent = None
        linked_under15_m_percent_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'Linked (%)',
            'cat_combo': '(<15, Male)',
            'numeric_sum': linked_under15_m_percent,
        }
        calculated_vals.append(linked_under15_m_percent_val)

        if all_not_none(linked_over15_f['numeric_sum'], pos_over15_f['numeric_sum']) and pos_over15_f['numeric_sum']:
            linked_over15_f_percent = (linked_over15_f['numeric_sum'] * 100) / pos_over15_f['numeric_sum']
        else:
            linked_over15_f_percent = None
        linked_over15_f_percent_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'Linked (%)',
            'cat_combo': '(15+, Female)',
            'numeric_sum': linked_over15_f_percent,
        }
        calculated_vals.append(linked_over15_f_percent_val)

        if all_not_none(linked_over15_m['numeric_sum'], pos_over15_m['numeric_sum']) and pos_over15_m['numeric_sum']:
            linked_over15_m_percent = (linked_over15_m['numeric_sum'] * 100) / pos_over15_m['numeric_sum']
        else:
            linked_over15_m_percent = None
        linked_over15_m_percent_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'Linked (%)',
            'cat_combo': '(15+, Male)',
            'numeric_sum': linked_over15_m_percent,
        }
        calculated_vals.append(linked_over15_m_percent_val)

        # _group[1].extend(calculated_vals)
        _group[1] = calculated_vals
    
    data_element_names = list()
    # data_element_names += list(product(hts_short_names, subcategory_names))
    # data_element_names += de_pmtct_mother_meta
    # data_element_names += de_pmtct_mother_pos_meta
    # data_element_names += list(product(pmtct_child_short_names, (None,)))
    # data_element_names += de_target_meta
    data_element_names += list(product(['Tested',], subcategory_names))
    data_element_names += list(product(['HIV+',], subcategory_names))
    data_element_names += list(product(['Tested',], [None,]))
    data_element_names += list(product(['HIV+',], [None,]))
    data_element_names += list(product(['Linked',], subcategory_names))
    data_element_names += list(product(['Tested (%)',], subcategory_names))
    data_element_names += list(product(['HIV+ (%)',], subcategory_names))
    data_element_names += list(product(['Linked (%)',], subcategory_names))

    legend_sets = list()
    test_and_pos_ls = LegendSet()
    test_and_pos_ls.name = 'Testing/Positivity'
    test_and_pos_ls.add_interval('red', 0, 75)
    test_and_pos_ls.add_interval('yellow', 75, 90)
    test_and_pos_ls.add_interval('green', 90, None)
    for i in range(17, 17+8):
        test_and_pos_ls.mappings[i] = True
    legend_sets.append(test_and_pos_ls)
    linked_ls = LegendSet()
    linked_ls.name = 'Link to Care'
    linked_ls.add_interval('red', 0, 80)
    linked_ls.add_interval('yellow', 80, 90)
    linked_ls.add_interval('green', 90, 100)
    for i in range(17+8, 17+8+4):
        linked_ls.mappings[i] = True
    legend_sets.append(linked_ls)

    if output_format == 'EXCEL':
        from django.http import HttpResponse
        import openpyxl

        wb = openpyxl.workbook.Workbook()
        ws = wb.active # workbooks are created with at least one worksheet
        ws.title = 'Sheet1' # unfortunately it is named "Sheet" not "Sheet1"
        ws.page_setup.orientation = ws.ORIENTATION_LANDSCAPE
        ws.page_setup.paperSize = ws.PAPERSIZE_A4

        headers = ou_headers + data_element_names
        for i, name in enumerate(headers, start=1):
            c = ws.cell(row=1, column=i)
            if not isinstance(name, tuple):
                c.value = str(name)
            else:
                de, cat_combo = name
                if cat_combo is None:
                    c.value = str(de)
                else:
                    c.value = str(de) + '\n' + str(cat_combo)
        for i, g in enumerate(grouped_vals, start=2):
            ou_path, g_val_list = g
            for col_idx, ou in enumerate(ou_path, start=1):
                ws.cell(row=i, column=col_idx, value=ou)
            for j, g_val in enumerate(g_val_list, start=len(ou_path)+1):
                ws.cell(row=i, column=j, value=g_val['numeric_sum'])

        for ls in legend_sets:
            for rule in ls.openpyxl_rules():
                for cell_range in ls.excel_ranges():
                    print(cell_range, ls)
                    ws.conditional_formatting.add(cell_range, rule)


        response = HttpResponse(openpyxl.writer.excel.save_virtual_workbook(wb), content_type='application/vnd.ms-excel')
        response['Content-Disposition'] = 'attachment; filename="hts_sites_scorecard.xlsx"'

        return response

    context = {
        'grouped_data': grouped_vals,
        'data_element_names': data_element_names,
        'legend_sets': legend_sets,
        'period_desc': period_desc,
        'period_list': PREV_5YR_QTRS,
        'district_list': DISTRICT_LIST,
    }

    return render(request, 'cannula/hts_sites.html', context)

@login_required
def hts_by_district(request, output_format='HTML'):
    this_day = date.today()
    this_year = this_day.year
    PREV_5YRS = ['%d' % (y,) for y in range(this_year, this_year-6, -1)]
    DISTRICT_LIST = list(OrgUnit.objects.filter(level=1).order_by('name').values_list('name', flat=True))

    if 'period' in request.GET and request.GET['period'] in PREV_5YRS:
        filter_period=request.GET['period']
    else:
        filter_period = '%d' % (this_year,)

    period_desc = filter_period

    if 'district' in request.GET and request.GET['district'] in DISTRICT_LIST:
        filter_district = OrgUnit.objects.get(name=request.GET['district'])
    else:
        filter_district = None

    hts_de_names = (
        '105-4 Number of clients who have been linked to care',
        '105-4 Number of Individuals who received HIV test results',
        '105-4 Number of Individuals who tested HIV positive',
    )
    hts_short_names = (
        'Linked',
        'Tested',
        'HIV+',
    )
    subcategory_names = ['(<15, Female)', '(<15, Male)', '(15+, Female)', '(15+, Male)']
    de_positivity_meta = list(product(hts_de_names, subcategory_names))

    qs_positivity = DataValue.objects.what(*hts_de_names).filter(year=filter_period)
    if filter_district:
        qs_positivity = qs_positivity.where(filter_district)

    cc_lt_15 = ['18 Mths-<5 Years', '5-<10 Years', '10-<15 Years']
    cc_ge_15 = ['15-<19 Years', '19-<49 Years', '>49 Years']
    #TODO: cc_lt_15_f = CategoryCombo.from_cat_names(['Female', '<15']) gives a CategoryCombo instance that makes the Case statement clearer/safer
    qs_positivity = qs_positivity.annotate(
        cat_combo=Case(
            When(Q(category_combo__categories__name__in=cc_lt_15) & Q(category_combo__name__contains='Female'), then=Value(subcategory_names[0])),
            When(Q(category_combo__categories__name__in=cc_lt_15) & ~Q(category_combo__name__contains='Female'), then=Value(subcategory_names[1])),
            When(Q(category_combo__categories__name__in=cc_ge_15) & Q(category_combo__name__contains='Female'), then=Value(subcategory_names[2])),
            When(Q(category_combo__categories__name__in=cc_ge_15) & ~Q(category_combo__name__contains='Female'), then=Value(subcategory_names[3])),
            default=None, output_field=CharField()
        )
    )
    qs_positivity = qs_positivity.exclude(cat_combo__iexact=None)

    qs_positivity = qs_positivity.annotate(district=F('org_unit__parent__parent__name'))
    qs_positivity = qs_positivity.annotate(period=F('year'))
    qs_positivity = qs_positivity.order_by('district', 'de_name', 'cat_combo', 'period')
    val_positivity = qs_positivity.values('district', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_positivity = list(val_positivity)
    
    # all districts (or equivalent)
    qs_ou = OrgUnit.objects.filter(level=1).annotate(district=F('name'))
    if filter_district:
        qs_ou = qs_ou.filter(Q(lft__gte=filter_district.lft) & Q(rght__lte=filter_district.rght))
    ou_list = list(v for v in qs_ou.values_list('district'))
    ou_headers = ['District',]

    def val_with_subcat_fun(row, col):
        district, = row
        de_name, subcategory = col
        return { 'district': district, 'cat_combo': subcategory, 'de_name': de_name, 'numeric_sum': None }
    gen_raster = grabbag.rasterize(ou_list, de_positivity_meta, val_positivity, lambda x: (x['district'],), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_positivity2 = list(gen_raster)

    pmtct_mother_de_names = (
        '105-2.1 Pregnant Women newly tested for HIV this pregnancy(TR & TRR)',
        '105-2.2a Women tested for HIV in labour (1st time this Pregnancy)',
        '105-2.3a Breastfeeding mothers tested for HIV(1st test)',
    )
    de_pmtct_mother_meta = list(product(('Pregnant Women tested for HIV',), (None,)))

    qs_pmtct_mother = DataValue.objects.what(*pmtct_mother_de_names).filter(year=filter_period)
    if filter_district:
        qs_pmtct_mother = qs_pmtct_mother.where(filter_district)
    qs_pmtct_mother = qs_pmtct_mother.annotate(de_name=Value('Pregnant Women tested for HIV', output_field=CharField()))
    qs_pmtct_mother = qs_pmtct_mother.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_pmtct_mother = qs_pmtct_mother.annotate(district=F('org_unit__parent__parent__name'))
    qs_pmtct_mother = qs_pmtct_mother.annotate(period=F('year'))
    qs_pmtct_mother = qs_pmtct_mother.order_by('district', 'de_name', 'cat_combo', 'period')
    val_pmtct_mother = qs_pmtct_mother.values('district', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))

    gen_raster = grabbag.rasterize(ou_list, de_pmtct_mother_meta, val_pmtct_mother, lambda x: (x['district'],), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_pmtct_mother2 = list(gen_raster)

    pmtct_mother_pos_de_names = (
        '105-2.1 A19:Pregnant Women testing HIV+ on a retest (TRR+)',
        '105-2.2a Women testing HIV+ in labour (1st time this Pregnancy)',
        '105-2.2b Women testing HIV+ in labour (Retest this Pregnancy)',
        '105-2.3a Breastfeeding mothers newly testing HIV+(1st test)',
        '105-2.3b Breastfeeding mothers newly testing HIV+(retest)',
    )
    de_pmtct_mother_pos_meta = list(product(('Pregnant Women testing HIV+',), (None,)))

    qs_pmtct_mother_pos = DataValue.objects.what(*pmtct_mother_pos_de_names).filter(year=filter_period)
    if filter_district:
        qs_pmtct_mother_pos = qs_pmtct_mother_pos.where(filter_district)
    qs_pmtct_mother_pos = qs_pmtct_mother_pos.annotate(de_name=Value('Pregnant Women testing HIV+', output_field=CharField()))
    qs_pmtct_mother_pos = qs_pmtct_mother_pos.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_pmtct_mother_pos = qs_pmtct_mother_pos.annotate(district=F('org_unit__parent__parent__name'))
    qs_pmtct_mother_pos = qs_pmtct_mother_pos.annotate(period=F('year'))
    qs_pmtct_mother_pos = qs_pmtct_mother_pos.order_by('district', 'de_name', 'cat_combo', 'period')
    val_pmtct_mother_pos = qs_pmtct_mother_pos.values('district', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))

    gen_raster = grabbag.rasterize(ou_list, de_pmtct_mother_pos_meta, val_pmtct_mother_pos, lambda x: (x['district'],), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_pmtct_mother_pos2 = list(gen_raster)

    pmtct_child_de_names = (
        '105-2.4a Exposed Infants Tested for HIV Below 18 Months(by 1st PCR) ',
        '105-2.4b 1st DNA PCR result returned(HIV+)',
        '105-2.4b 2nd DNA PCR result returned(HIV+)',
        '105-2.1a Male partners received HIV test results in eMTCT(Total)',
        '105-2.1b Male partners received HIV test results in eMTCT(HIV+)',
    )
    pmtct_child_short_names = (
        'PMTCT INFANT HIV+',
        'PMTCT CHILD PCR1 HIV+',
        'PMTCT CHILD PCR2 HIV+',
        'PMTCT MALE PARTNERS TESTED',
        'PMTCT MALE PARTNERS HIV+',
    )
    de_pmtct_child_meta = list(product(pmtct_child_de_names, (None,)))

    qs_pmtct_child = DataValue.objects.what(*pmtct_child_de_names).filter(year=filter_period)
    if filter_district:
        qs_pmtct_child = qs_pmtct_child.where(filter_district)
    qs_pmtct_child = qs_pmtct_child.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_pmtct_child = qs_pmtct_child.annotate(district=F('org_unit__parent__parent__name'))
    qs_pmtct_child = qs_pmtct_child.annotate(period=F('year'))
    qs_pmtct_child = qs_pmtct_child.order_by('district', 'de_name', 'cat_combo', 'period')
    val_pmtct_child = qs_pmtct_child.values('district', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))

    gen_raster = grabbag.rasterize(ou_list, de_pmtct_child_meta, val_pmtct_child, lambda x: (x['district'],), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_pmtct_child2 = list(gen_raster)

    target_de_names = (
        'HTC_TST_TARGET',
        'HTC_TST_POS_TARGET',
    )
    de_target_meta = list(product(target_de_names, subcategory_names))

    # targets are annual, so filter by year component of period
    qs_target = DataValue.objects.what(*target_de_names).filter(year=filter_period[:4])
    if filter_district:
        qs_target = qs_target.where(filter_district)

    qs_target = qs_target.annotate(cat_combo=F('category_combo__name'))
    qs_target = qs_target.annotate(district=F('org_unit__parent__parent__name'))
    qs_target = qs_target.annotate(period=F('year'))
    qs_target = qs_target.order_by('district', '-de_name', 'cat_combo', 'period') # note reversed order of data element names
    val_target = qs_target.values('district', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_target = list(val_target)

    gen_raster = grabbag.rasterize(ou_list, de_target_meta, val_target, lambda x: (x['district'],), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_target2 = list(gen_raster)

    # combine the data and group by district
    grouped_vals = groupbylist(sorted(chain(val_positivity2, val_pmtct_mother2, val_pmtct_mother_pos2, val_pmtct_child2, val_target2), key=lambda x: (x['district'],)), key=lambda x: (x['district'],))

    # perform calculations
    for _group in grouped_vals:
        (ou_path_list, (linked_under15_f, linked_under15_m, linked_over15_f, linked_over15_m, tst_under15_f, tst_under15_m, tst_over15_f, tst_over15_m, pos_under15_f, pos_under15_m, pos_over15_f, pos_over15_m, tst_pregnant, pos_pregnant, pos_infant, pos_pcr1, pos_pcr2, tst_male_partner, pos_male_partner, *other_vals)) = _group
        
        calculated_vals = list()

        under15_f_sum = default_zero(tst_under15_f['numeric_sum']) + Decimal(default_zero(pos_infant['numeric_sum'])/2)
        under15_f_val = {
            'district': ou_path_list[0],
            'de_name': 'Tested',
            'cat_combo': '(<15, Female)',
            'numeric_sum': under15_f_sum,
        }
        calculated_vals.append(under15_f_val)
        
        under15_m_sum = default_zero(tst_under15_m['numeric_sum']) + Decimal(default_zero(pos_infant['numeric_sum'])/2)
        under15_m_val = {
            'district': ou_path_list[0],
            'de_name': 'Tested',
            'cat_combo': '(<15, Male)',
            'numeric_sum': under15_m_sum,
        }
        calculated_vals.append(under15_m_val)
        
        over15_f_sum = default_zero(tst_over15_f['numeric_sum']) + default_zero(tst_pregnant['numeric_sum'])
        over15_f_val = {
            'district': ou_path_list[0],
            'de_name': 'Tested',
            'cat_combo': '(15+, Female)',
            'numeric_sum': over15_f_sum,
        }
        calculated_vals.append(over15_f_val)
        
        over15_m_sum = default_zero(tst_over15_m['numeric_sum']) + default_zero(tst_male_partner['numeric_sum'])
        over15_m_val = {
            'district': ou_path_list[0],
            'de_name': 'Tested',
            'cat_combo': '(15+, Male)',
            'numeric_sum': over15_m_sum,
        }
        calculated_vals.append(over15_m_val)
        
        half_pos_pcr = Decimal(default_zero(pos_pcr1['numeric_sum']) + default_zero(pos_pcr1['numeric_sum']))/2
        pos_under15_f_sum = default_zero(pos_under15_f['numeric_sum']) + half_pos_pcr
        pos_under15_f_val = {
            'district': ou_path_list[0],
            'de_name': 'HIV+',
            'cat_combo': '(<15, Female)',
            'numeric_sum': pos_under15_f_sum,
        }
        calculated_vals.append(pos_under15_f_val)
        
        pos_under15_m_sum = default_zero(pos_under15_m['numeric_sum']) + half_pos_pcr
        pos_under15_m_val = {
            'district': ou_path_list[0],
            'de_name': 'HIV+',
            'cat_combo': '(<15, Male)',
            'numeric_sum': pos_under15_m_sum,
        }
        calculated_vals.append(pos_under15_m_val)
        
        pos_over15_f_sum = default_zero(pos_over15_f['numeric_sum']) + Decimal(default_zero(pos_pregnant['numeric_sum']))
        pos_over15_f_val = {
            'district': ou_path_list[0],
            'de_name': 'HIV+',
            'cat_combo': '(15+, Female)',
            'numeric_sum': pos_over15_f_sum,
        }
        calculated_vals.append(pos_over15_f_val)
        
        pos_over15_m_sum = default_zero(pos_over15_m['numeric_sum']) + Decimal(default_zero(pos_male_partner['numeric_sum']))
        pos_over15_m_val = {
            'district': ou_path_list[0],
            'de_name': 'HIV+',
            'cat_combo': '(15+, Male)',
            'numeric_sum': pos_over15_m_sum,
        }
        calculated_vals.append(pos_over15_m_val)

        tested_total = sum([under15_f_sum, under15_m_sum, over15_f_sum, over15_m_sum])
        pos_total = sum([pos_under15_f_sum, pos_under15_m_sum, pos_over15_f_sum, pos_over15_m_sum])
        tested_total_val = {
            'district': ou_path_list[0],
            'de_name': 'Tested',
            'cat_combo': None,
            'numeric_sum': tested_total,
        }
        pos_total_val = {
            'district': ou_path_list[0],
            'de_name': 'HIV+',
            'cat_combo': None,
            'numeric_sum': pos_total,
        }
        calculated_vals.append(tested_total_val)
        calculated_vals.append(pos_total_val)

        # copy linked to care totals over
        calculated_vals.append(linked_under15_f)
        calculated_vals.append(linked_under15_m)
        calculated_vals.append(linked_over15_f)
        calculated_vals.append(linked_over15_m)

        # calculate the percentages
        target_under15_f, target_under15_m, target_over15_f, target_over15_m, target_pos_under15_f, target_pos_under15_m, target_pos_over15_f, target_pos_over15_m, *further_vals = other_vals

        if all_not_none(under15_f_sum, target_under15_f['numeric_sum']) and target_under15_f['numeric_sum']:
            under15_f_percent = (under15_f_sum * 100) / target_under15_f['numeric_sum']
        else:
            under15_f_percent = None
        under15_f_percent_val = {
            'district': ou_path_list[0],
            'de_name': 'Tested (%)',
            'cat_combo': '(<15, Female)',
            'numeric_sum': under15_f_percent,
        }
        calculated_vals.append(under15_f_percent_val)

        if all_not_none(under15_m_sum, target_under15_m['numeric_sum']) and target_under15_m['numeric_sum']:
            under15_m_percent = (under15_m_sum * 100) / target_under15_m['numeric_sum']
        else:
            under15_m_percent = None
        under15_m_percent_val = {
            'district': ou_path_list[0],
            'de_name': 'Tested (%)',
            'cat_combo': '(<15, Male)',
            'numeric_sum': under15_m_percent,
        }
        calculated_vals.append(under15_m_percent_val)

        if all_not_none(over15_f_sum, target_over15_f['numeric_sum']) and target_over15_f['numeric_sum']:
            over15_f_percent = (over15_f_sum * 100) / target_over15_f['numeric_sum']
        else:
            over15_f_percent = None
        over15_f_percent_val = {
            'district': ou_path_list[0],
            'de_name': 'Tested (%)',
            'cat_combo': '(15+, Female)',
            'numeric_sum': over15_f_percent,
        }
        calculated_vals.append(over15_f_percent_val)

        if all_not_none(over15_m_sum, target_over15_m['numeric_sum']) and target_over15_m['numeric_sum']:
            over15_m_percent = (over15_m_sum * 100) / target_over15_m['numeric_sum']
        else:
            over15_m_percent = None
        over15_m_percent_val = {
            'district': ou_path_list[0],
            'de_name': 'Tested (%)',
            'cat_combo': '(15+, Male)',
            'numeric_sum': over15_m_percent,
        }
        calculated_vals.append(over15_m_percent_val)

        if all_not_none(pos_under15_f_sum, target_pos_under15_f['numeric_sum']) and target_pos_under15_f['numeric_sum']:
            pos_under15_f_percent = (pos_under15_f_sum * 100) / target_pos_under15_f['numeric_sum']
        else:
            pos_under15_f_percent = None
        pos_under15_f_percent_val = {
            'district': ou_path_list[0],
            'de_name': 'HIV+ (%)',
            'cat_combo': '(<15, Female)',
            'numeric_sum': pos_under15_f_percent,
        }
        calculated_vals.append(pos_under15_f_percent_val)

        if all_not_none(pos_under15_m_sum, target_pos_under15_m['numeric_sum']) and target_pos_under15_m['numeric_sum']:
            pos_under15_m_percent = (pos_under15_m_sum * 100) / target_pos_under15_m['numeric_sum']
        else:
            pos_under15_m_percent = None
        pos_under15_m_percent_val = {
            'district': ou_path_list[0],
            'de_name': 'HIV+ (%)',
            'cat_combo': '(<15, Male)',
            'numeric_sum': pos_under15_m_percent,
        }
        calculated_vals.append(pos_under15_m_percent_val)

        if all_not_none(pos_over15_f_sum, target_pos_over15_f['numeric_sum']) and target_pos_over15_f['numeric_sum']:
            pos_over15_f_percent = (pos_over15_f_sum * 100) / target_pos_over15_f['numeric_sum']
        else:
            pos_over15_f_percent = None
        pos_over15_f_percent_val = {
            'district': ou_path_list[0],
            'de_name': 'HIV+ (%)',
            'cat_combo': '(15+, Female)',
            'numeric_sum': pos_over15_f_percent,
        }
        calculated_vals.append(pos_over15_f_percent_val)

        if all_not_none(pos_over15_m_sum, target_pos_over15_m['numeric_sum']) and target_pos_over15_m['numeric_sum']:
            pos_over15_m_percent = (pos_over15_m_sum * 100) / target_pos_over15_m['numeric_sum']
        else:
            pos_over15_m_percent = None
        pos_over15_m_percent_val = {
            'district': ou_path_list[0],
            'de_name': 'HIV+ (%)',
            'cat_combo': '(15+, Male)',
            'numeric_sum': pos_over15_m_percent,
        }
        calculated_vals.append(pos_over15_m_percent_val)

        if all_not_none(linked_under15_f['numeric_sum'], pos_under15_f['numeric_sum']) and pos_under15_f['numeric_sum']:
            linked_under15_f_percent = (linked_under15_f['numeric_sum'] * 100) / pos_under15_f['numeric_sum']
        else:
            linked_under15_f_percent = None
        linked_under15_f_percent_val = {
            'district': ou_path_list[0],
            'de_name': 'Linked (%)',
            'cat_combo': '(<15, Female)',
            'numeric_sum': linked_under15_f_percent,
        }
        calculated_vals.append(linked_under15_f_percent_val)

        if all_not_none(linked_under15_m['numeric_sum'], pos_under15_m['numeric_sum']) and pos_under15_m['numeric_sum']:
            linked_under15_m_percent = (linked_under15_m['numeric_sum'] * 100) / pos_under15_m['numeric_sum']
        else:
            linked_under15_m_percent = None
        linked_under15_m_percent_val = {
            'district': ou_path_list[0],
            'de_name': 'Linked (%)',
            'cat_combo': '(<15, Male)',
            'numeric_sum': linked_under15_m_percent,
        }
        calculated_vals.append(linked_under15_m_percent_val)

        if all_not_none(linked_over15_f['numeric_sum'], pos_over15_f['numeric_sum']) and pos_over15_f['numeric_sum']:
            linked_over15_f_percent = (linked_over15_f['numeric_sum'] * 100) / pos_over15_f['numeric_sum']
        else:
            linked_over15_f_percent = None
        linked_over15_f_percent_val = {
            'district': ou_path_list[0],
            'de_name': 'Linked (%)',
            'cat_combo': '(15+, Female)',
            'numeric_sum': linked_over15_f_percent,
        }
        calculated_vals.append(linked_over15_f_percent_val)

        if all_not_none(linked_over15_m['numeric_sum'], pos_over15_m['numeric_sum']) and pos_over15_m['numeric_sum']:
            linked_over15_m_percent = (linked_over15_m['numeric_sum'] * 100) / pos_over15_m['numeric_sum']
        else:
            linked_over15_m_percent = None
        linked_over15_m_percent_val = {
            'district': ou_path_list[0],
            'de_name': 'Linked (%)',
            'cat_combo': '(15+, Male)',
            'numeric_sum': linked_over15_m_percent,
        }
        calculated_vals.append(linked_over15_m_percent_val)

        # _group[1].extend(calculated_vals)
        _group[1] = calculated_vals
    
    data_element_names = list()
    
    # data_element_names += list(product(hts_short_names, subcategory_names))
    # data_element_names += de_pmtct_mother_meta
    # data_element_names += de_pmtct_mother_pos_meta
    # data_element_names += list(product(pmtct_child_short_names, (None,)))
    # data_element_names += de_target_meta

    data_element_names += list(product(['Tested',], subcategory_names))
    data_element_names += list(product(['HIV+',], subcategory_names))
    data_element_names += list(product(['Tested',], [None,]))
    data_element_names += list(product(['HIV+',], [None,]))
    data_element_names += list(product(['Linked',], subcategory_names))
    data_element_names += list(product(['Tested (%)',], subcategory_names))
    data_element_names += list(product(['HIV+ (%)',], subcategory_names))
    data_element_names += list(product(['Linked (%)',], subcategory_names))

    legend_sets = list()
    test_and_pos_ls = LegendSet()
    test_and_pos_ls.name = 'Testing/Positivity'
    test_and_pos_ls.add_interval('red', 0, 75)
    test_and_pos_ls.add_interval('yellow', 75, 90)
    test_and_pos_ls.add_interval('green', 90, None)
    for i in range(15, 15+8):
        test_and_pos_ls.mappings[i] = True
    legend_sets.append(test_and_pos_ls)
    linked_ls = LegendSet()
    linked_ls.name = 'Link to Care'
    linked_ls.add_interval('red', 0, 80)
    linked_ls.add_interval('yellow', 80, 90)
    linked_ls.add_interval('green', 90, 100)
    for i in range(15+8, 15+8+4):
        linked_ls.mappings[i] = True
    legend_sets.append(linked_ls)

    if output_format == 'EXCEL':
        from django.http import HttpResponse
        import openpyxl

        wb = openpyxl.workbook.Workbook()
        ws = wb.active # workbooks are created with at least one worksheet
        ws.title = 'Sheet1' # unfortunately it is named "Sheet" not "Sheet1"
        ws.page_setup.orientation = ws.ORIENTATION_LANDSCAPE
        ws.page_setup.paperSize = ws.PAPERSIZE_A4

        headers = ou_headers + data_element_names
        for i, name in enumerate(headers, start=1):
            c = ws.cell(row=1, column=i)
            if not isinstance(name, tuple):
                c.value = str(name)
            else:
                de, cat_combo = name
                if cat_combo is None:
                    c.value = str(de)
                else:
                    c.value = str(de) + '\n' + str(cat_combo)
        for i, g in enumerate(grouped_vals, start=2):
            ou_path, g_val_list = g
            for col_idx, ou in enumerate(ou_path, start=1):
                ws.cell(row=i, column=col_idx, value=ou)
            for j, g_val in enumerate(g_val_list, start=len(ou_path)+1):
                ws.cell(row=i, column=j, value=g_val['numeric_sum'])

        for ls in legend_sets:
            # apply conditional formatting from LegendSets
            for rule in ls.openpyxl_rules():
                for cell_range in ls.excel_ranges():
                    print(cell_range, ls)
                    ws.conditional_formatting.add(cell_range, rule)


        response = HttpResponse(openpyxl.writer.excel.save_virtual_workbook(wb), content_type='application/vnd.ms-excel')
        response['Content-Disposition'] = 'attachment; filename="hts_districts_scorecard.xlsx"'

        return response

    context = {
        'grouped_data': grouped_vals,
        'data_element_names': data_element_names,
        'legend_sets': legend_sets,
        'period_desc': period_desc,
        'period_list': PREV_5YRS,
        'district_list': DISTRICT_LIST,
    }

    return render(request, 'cannula/hts_districts.html', context)

@login_required
def vmmc_by_site(request, output_format='HTML'):
    this_day = date.today()
    this_year = this_day.year
    PREV_5YR_QTRS = ['%d-Q%d' % (y, q) for y in range(this_year, this_year-6, -1) for q in range(4, 0, -1)]
    DISTRICT_LIST = list(OrgUnit.objects.filter(level=1).order_by('name').values_list('name', flat=True))

    if 'period' in request.GET and request.GET['period'] in PREV_5YR_QTRS:
        filter_period=request.GET['period']
    else:
        filter_period = '%d-Q%d' % (this_year, month2quarter(this_day.month))

    period_desc = dateutil.DateSpan.fromquarter(filter_period).format()

    if 'district' in request.GET and request.GET['district'] in DISTRICT_LIST:
        filter_district = OrgUnit.objects.get(name=request.GET['district'])
    else:
        filter_district = None

    # # all facilities (or equivalent)
    qs_ou = OrgUnit.objects.filter(level=3).annotate(district=F('parent__parent__name'), subcounty=F('parent__name'), facility=F('name'))
    if filter_district:
        qs_ou = qs_ou.filter(Q(lft__gte=filter_district.lft) & Q(rght__lte=filter_district.rght))
    ou_list = list(qs_ou.values_list('district', 'subcounty', 'facility'))
    ou_headers = ['District', 'Subcounty', 'Facility']

    def val_with_subcat_fun(row, col):
        district, subcounty, facility = row
        de_name, subcategory = col
        return { 'district': district, 'subcounty': subcounty, 'facility': facility, 'cat_combo': subcategory, 'de_name': de_name, 'numeric_sum': None }

    targets_de_names = (
        'VMMC_CIRC_TARGET',
        'VMMC_DEVICE_TARGET',
        'VMMC_SURGICAL_TARGET',
    )
    targets_short_names = (
        'TARGET: VMMC_CIRC',
        'TARGET: Device-based',
        'TARGET: Surgical',
    )
    de_targets_meta = list(product(targets_de_names, (None,)))

    qs_targets = DataValue.objects.what(*targets_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_targets = qs_targets.where(filter_district)
    qs_targets = qs_targets.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_targets = qs_targets.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_targets = qs_targets.annotate(period=F('quarter'))
    qs_targets = qs_targets.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_targets = qs_targets.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_targets = list(val_targets)

    gen_raster = grabbag.rasterize(ou_list, de_targets_meta, val_targets, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_targets2 = list(gen_raster)

    method_de_names = (
        '105-5 Clients circumcised by circumcision Technique Device Based (DC)',
        '105-5 Clients circumcised by circumcision Technique Other VMMC techniques',
        '105-5 Clients circumcised by circumcision Technique Surgical(SC)',
    )
    method_short_names = (
        'Circumcised by technique - Device Based',
        'Circumcised by technique - Other',
        'Circumcised by technique - Surgical',
    )
    de_method_meta = list(product(method_de_names, (None,)))

    qs_method = DataValue.objects.what(*method_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_method = qs_method.where(filter_district)
    qs_method = qs_method.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_method = qs_method.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_method = qs_method.annotate(period=F('quarter'))
    qs_method = qs_method.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_method = qs_method.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))

    gen_raster = grabbag.rasterize(ou_list, de_method_meta, val_method, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_method2 = list(gen_raster)

    hiv_de_names = (
        '105-5 SMC Clients Counseled, Tested and Circumcised for HIV at SMC site HIV Negative',
        '105-5 SMC Clients Counseled, Tested and Circumcised for HIV at SMC site HIV Positive',
    )
    hiv_short_names = (
        'Circumcised by HIV status - Negative',
        'Circumcised by HIV status - Positive',
    )
    de_hiv_meta = list(product(hiv_de_names, (None,)))

    qs_hiv = DataValue.objects.what(*hiv_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_hiv = qs_hiv.where(filter_district)
    qs_hiv = qs_hiv.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_hiv = qs_hiv.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_hiv = qs_hiv.annotate(period=F('quarter'))
    qs_hiv = qs_hiv.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_hiv = qs_hiv.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))

    gen_raster = grabbag.rasterize(ou_list, de_hiv_meta, val_hiv, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_hiv2 = list(gen_raster)

    location_de_names = (
        '105-5 Number of Males Circumcised by Age group and Technique Facility, Device Based (DC)',
        '105-5 Number of Males Circumcised by Age group and Technique Facility, Surgical(SC)',
        '105-5 Number of Males Circumcised by Age group and Technique Outreach, Device Based (DC)',
        '105-5 Number of Males Circumcised by Age group and Technique Outreach, Surgical(SC)',
    )
    location_de_names2 = (
        '105-5 Number of Males Circumcised by Age group and Technique Facility',
        '105-5 Number of Males Circumcised by Age group and Technique Outreach',
    )
    location_prefix_len = len('105-5 Number of Males Circumcised by Age group and Technique Facility')
    location_short_names = (
        'Circumcised by site type - Static',
        'Circumcised by site type - Mobile',
    )
    de_location_meta = list(product(location_de_names2, (None,)))

    qs_location = DataValue.objects.what(*location_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_location = qs_location.where(filter_district)
    qs_location = qs_location.annotate(cat_combo=Value(None, output_field=CharField()))

    # drop the technique section from the returned data element name
    qs_location = qs_location.annotate(de_name=Substr('data_element__name', 1, location_prefix_len))

    qs_location = qs_location.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_location = qs_location.annotate(period=F('quarter'))
    qs_location = qs_location.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_location = qs_location.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))

    gen_raster = grabbag.rasterize(ou_list, de_location_meta, val_location, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_location2 = list(gen_raster)

    followup_de_names = (
        '105-5a Number of Clients Circumcised who Returned for Follow Up Visit within 6 weeks of SMC Procedure(Within 48 Hours)',
        '105-5b Number of Clients Circumcised who Returned for Follow Up Visit within 6 weeks of SMC Procedure(Within 7 Days)',
        '105-5c Number of Clients Circumcised who Returned for Follow Up Visit within 6 weeks of SMC Procedure(Beyond 7 Days)',
    )
    followup_short_names = (
        'Follow up - Within 48 hours',
        'Follow up - Within 7 days',
        'Follow up - Beyond 7 days',
    )
    de_followup_meta = list(product(followup_de_names, (None,)))

    qs_followup = DataValue.objects.what(*followup_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_followup = qs_followup.where(filter_district)
    qs_followup = qs_followup.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_followup = qs_followup.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_followup = qs_followup.annotate(period=F('quarter'))
    qs_followup = qs_followup.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_followup = qs_followup.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))

    gen_raster = grabbag.rasterize(ou_list, de_followup_meta, val_followup, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_followup2 = list(gen_raster)

    adverse_de_names = (
        '105-5 Clients Circumcised who Experienced one or more Adverse Events Moderate',
        '105-5 Clients Circumcised who Experienced one or more Adverse Events Severe',
    )
    adverse_short_names = (
        'Adverse Events - Moderate',
        'Adverse Events - Severe',
    )
    de_adverse_meta = list(product(adverse_de_names, (None,)))

    qs_adverse = DataValue.objects.what(*adverse_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_adverse = qs_adverse.where(filter_district)
    qs_adverse = qs_adverse.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_adverse = qs_adverse.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_adverse = qs_adverse.annotate(period=F('quarter'))
    qs_adverse = qs_adverse.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_adverse = qs_adverse.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))

    gen_raster = grabbag.rasterize(ou_list, de_adverse_meta, val_adverse, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_adverse2 = list(gen_raster)

    # combine the data and group by district, subcounty and facility
    grouped_vals = groupbylist(sorted(chain(val_targets2, val_hiv2, val_location2, val_method2, val_followup2, val_adverse2), key=lambda x: (x['district'], x['subcounty'], x['facility'])), key=lambda x: (x['district'], x['subcounty'], x['facility']))
    if True:
        grouped_vals = list(filter_empty_rows(grouped_vals))

    # perform calculations
    for _group in grouped_vals:
        (district_subcounty_facility, (target_total, target_device, target_surgical, hiv_negative, hiv_positive, location_facility, location_outreach, method_device, method_other, method_surgical, followup_48hrs, followup_7days, followup_plus7days, adverse_moderate, adverse_severe, *other_vals)) = _group
        
        calculated_vals = list()

        method_sum = default_zero(method_device['numeric_sum']) + default_zero(method_surgical['numeric_sum']) + default_zero(method_other['numeric_sum'])

        if all_not_none(target_total['numeric_sum'], method_sum) and target_total['numeric_sum']:
            target_total_percent = (method_sum * 100) / target_total['numeric_sum']
        else:
            target_total_percent = None
        target_total_percent_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'Perf% Circumcised',
            'cat_combo': None,
            'numeric_sum': target_total_percent,
        }
        calculated_vals.append(target_total_percent_val)

        if all_not_none(target_device['numeric_sum'], method_device['numeric_sum']) and target_device['numeric_sum']:
            target_device_percent = (method_device['numeric_sum'] * 100) / target_device['numeric_sum']
        else:
            target_device_percent = None
        target_device_percent_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'Perf% Circumcised DC',
            'cat_combo': None,
            'numeric_sum': target_device_percent,
        }
        calculated_vals.append(target_device_percent_val)

        if all_not_none(target_surgical['numeric_sum'], method_surgical['numeric_sum']) and target_surgical['numeric_sum']:
            target_surgical_percent = (method_surgical['numeric_sum'] * 100) / target_surgical['numeric_sum']
        else:
            target_surgical_percent = None
        target_surgical_percent_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'Perf% Circumcised Surgical',
            'cat_combo': None,
            'numeric_sum': target_surgical_percent,
        }
        calculated_vals.append(target_surgical_percent_val)

        if all_not_none(followup_48hrs['numeric_sum'], method_sum) and method_sum:
            followup_48hrs_percent = (followup_48hrs['numeric_sum'] * 100) / method_sum
        else:
            followup_48hrs_percent = None
        followup_48hrs_percent_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': '% who returned within 48 hours',
            'cat_combo': None,
            'numeric_sum': followup_48hrs_percent,
        }
        calculated_vals.append(followup_48hrs_percent_val)

        adverse_sum = default_zero(adverse_moderate['numeric_sum']) + default_zero(adverse_severe['numeric_sum'])

        if all_not_none(adverse_sum, method_sum) and method_sum:
            adverse_percent = (adverse_sum * 100) / method_sum
        else:
            adverse_percent = None
        adverse_percent_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': '% with at least one adverse event',
            'cat_combo': None,
            'numeric_sum': adverse_percent,
        }
        calculated_vals.append(adverse_percent_val)

        _group[1].extend(calculated_vals)

    data_element_names = list()
    data_element_names += list(product(targets_short_names, (None,)))
    data_element_names += list(product(hiv_short_names, (None,)))
    data_element_names += list(product(location_short_names, (None,)))
    data_element_names += list(product(method_short_names, (None,)))
    data_element_names += list(product(followup_short_names, (None,)))
    data_element_names += list(product(adverse_short_names, (None,)))

    data_element_names += list(product(['Perf% Circumcised'], (None,)))
    data_element_names += list(product(['Perf% Circumcised DC'], (None,)))
    data_element_names += list(product(['Perf% Circumcised Surgical'], (None,)))
    data_element_names += list(product(['% who returned within 48 hours'], (None,)))
    data_element_names += list(product(['% with at least one adverse event'], (None,)))

    legend_sets = list()
    vmmc_ls = LegendSet()
    vmmc_ls.name = 'Perf Circumcised'
    vmmc_ls.add_interval('orange', 0, 25)
    vmmc_ls.add_interval('yellow', 25, 40)
    vmmc_ls.add_interval('light-green', 50, 60)
    vmmc_ls.add_interval('green', 60, None)
    for i in range(18, 18+3):
        vmmc_ls.mappings[i] = True
    legend_sets.append(vmmc_ls)
    adverse_ls = LegendSet()
    adverse_ls.name = 'Adverse Events'
    adverse_ls.add_interval('red', 0.5, None)
    adverse_ls.mappings[22] = True
    legend_sets.append(adverse_ls)

    if output_format == 'EXCEL':
        from django.http import HttpResponse
        import openpyxl

        wb = openpyxl.workbook.Workbook()
        ws = wb.active # workbooks are created with at least one worksheet
        ws.title = 'Sheet1' # unfortunately it is named "Sheet" not "Sheet1"
        ws.page_setup.orientation = ws.ORIENTATION_LANDSCAPE
        ws.page_setup.paperSize = ws.PAPERSIZE_A4

        headers = ou_headers + data_element_names
        for i, name in enumerate(headers, start=1):
            c = ws.cell(row=1, column=i)
            if not isinstance(name, tuple):
                c.value = str(name)
            else:
                de, cat_combo = name
                if cat_combo is None:
                    c.value = str(de)
                else:
                    c.value = str(de) + '\n' + str(cat_combo)
        for i, g in enumerate(grouped_vals, start=2):
            ou_path, g_val_list = g
            for col_idx, ou in enumerate(ou_path, start=1):
                ws.cell(row=i, column=col_idx, value=ou)
            for j, g_val in enumerate(g_val_list, start=len(ou_path)+1):
                ws.cell(row=i, column=j, value=g_val['numeric_sum'])

        for ls in legend_sets:
            # apply conditional formatting from LegendSets
            for rule in ls.openpyxl_rules():
                for cell_range in ls.excel_ranges():
                    print(cell_range, ls)
                    ws.conditional_formatting.add(cell_range, rule)


        response = HttpResponse(openpyxl.writer.excel.save_virtual_workbook(wb), content_type='application/vnd.ms-excel')
        response['Content-Disposition'] = 'attachment; filename="vmmc_scorecard.xlsx"'

        return response

    context = {
        'grouped_data': grouped_vals,
        'data_element_names': data_element_names,
        'legend_sets': legend_sets,
        'period_desc': period_desc,
        'period_list': PREV_5YR_QTRS,
        'district_list': DISTRICT_LIST,
    }

    return render(request, 'cannula/vmmc_sites.html', context)

@login_required
def lab_by_site(request, output_format='HTML'):
    this_day = date.today()
    this_year = this_day.year
    PREV_5YR_QTRS = ['%d-Q%d' % (y, q) for y in range(this_year, this_year-6, -1) for q in range(4, 0, -1)]
    DISTRICT_LIST = list(OrgUnit.objects.filter(level=1).order_by('name').values_list('name', flat=True))

    if 'period' in request.GET and request.GET['period'] in PREV_5YR_QTRS:
        filter_period=request.GET['period']
    else:
        filter_period = '%d-Q%d' % (this_year, month2quarter(this_day.month))

    period_desc = dateutil.DateSpan.fromquarter(filter_period).format()

    if 'district' in request.GET and request.GET['district'] in DISTRICT_LIST:
        filter_district = OrgUnit.objects.get(name=request.GET['district'])
    else:
        filter_district = None

    # # all facilities (or equivalent)
    qs_ou = OrgUnit.objects.filter(level=3).annotate(district=F('parent__parent__name'), subcounty=F('parent__name'), facility=F('name'))
    if filter_district:
        qs_ou = qs_ou.filter(Q(lft__gte=filter_district.lft) & Q(rght__lte=filter_district.rght))
    ou_list = list(qs_ou.values_list('district', 'subcounty', 'facility'))
    ou_headers = ['District', 'Subcounty', 'Facility']

    def val_with_subcat_fun(row, col):
        district, subcounty, facility = row
        de_name, subcategory = col
        return { 'district': district, 'subcounty': subcounty, 'facility': facility, 'cat_combo': subcategory, 'de_name': de_name, 'numeric_sum': None }

    malaria_de_names = (
        '105-7.3 Lab Malaria Microscopy  Number Done',
        '105-7.3 Lab Malaria RDTs Number Done',
    )
    malaria_short_names = (
        'Malaria Microscopy Done',
        'Malaria RDTs Done',
    )
    de_malaria_meta = list(product(malaria_de_names, (None,)))

    qs_malaria = DataValue.objects.what(*malaria_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_malaria = qs_malaria.where(filter_district)
    qs_malaria = qs_malaria.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_malaria = qs_malaria.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_malaria = qs_malaria.annotate(period=F('quarter'))
    qs_malaria = qs_malaria.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_malaria = qs_malaria.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_malaria = list(val_malaria)

    gen_raster = grabbag.rasterize(ou_list, de_malaria_meta, val_malaria, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_malaria2 = list(gen_raster)

    hiv_determine_de_names = (
        '105-7.8 Lab Determine Clinical Diagnosis',
        '105-7.8 Lab Determine HCT',
        '105-7.8 Lab Determine PMTCT',
        '105-7.8 Lab Determine Quality Control',
        '105-7.8 Lab Determine SMC',
    )
    hiv_determine_short_names = (
        'HIV tests done using Determine',
    )
    de_hiv_determine_meta = list(product(['HIV tests done using Determine'], (None,)))

    qs_hiv_determine = DataValue.objects.what(*hiv_determine_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_hiv_determine = qs_hiv_determine.where(filter_district)
    qs_hiv_determine = qs_hiv_determine.annotate(de_name=Value('HIV tests done using Determine', output_field=CharField()))
    qs_hiv_determine = qs_hiv_determine.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_hiv_determine = qs_hiv_determine.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_hiv_determine = qs_hiv_determine.annotate(period=F('quarter'))
    qs_hiv_determine = qs_hiv_determine.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_hiv_determine = qs_hiv_determine.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_hiv_determine = list(val_hiv_determine)

    gen_raster = grabbag.rasterize(ou_list, de_hiv_determine_meta, val_hiv_determine, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_hiv_determine2 = list(gen_raster)

    hiv_statpak_de_names = (
        '105-7.8 Lab Stat pak  Clinical Diagnosis',
        '105-7.8 Lab Stat pak  HCT',
        '105-7.8 Lab Stat pak  PMTCT',
        '105-7.8 Lab Stat pak  Quality Control',
        '105-7.8 Lab Stat pak  SMC',
    )
    hiv_statpak_short_names = (
        'HIV tests done using Stat Pak',
    )
    de_hiv_statpak_meta = list(product(['HIV tests done using Stat Pak'], (None,)))

    qs_hiv_statpak = DataValue.objects.what(*hiv_statpak_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_hiv_statpak = qs_hiv_statpak.where(filter_district)
    qs_hiv_statpak = qs_hiv_statpak.annotate(de_name=Value('HIV tests done using Stat Pak', output_field=CharField()))
    qs_hiv_statpak = qs_hiv_statpak.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_hiv_statpak = qs_hiv_statpak.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_hiv_statpak = qs_hiv_statpak.annotate(period=F('quarter'))
    qs_hiv_statpak = qs_hiv_statpak.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_hiv_statpak = qs_hiv_statpak.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_hiv_statpak = list(val_hiv_statpak)

    gen_raster = grabbag.rasterize(ou_list, de_hiv_statpak_meta, val_hiv_statpak, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_hiv_statpak2 = list(gen_raster)

    hiv_unigold_de_names = (
        '105-7.8 Lab Unigold Clinical Diagnosis',
        '105-7.8 Lab Unigold HCT',
        '105-7.8 Lab Unigold PMTCT',
        '105-7.8 Lab Unigold Quality Control',
        '105-7.8 Lab Unigold SMC',
    )
    hiv_unigold_short_names = (
        'HIV tests done using Unigold',
    )
    de_hiv_unigold_meta = list(product(['HIV tests done using Unigold'], (None,)))

    qs_hiv_unigold = DataValue.objects.what(*hiv_unigold_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_hiv_unigold = qs_hiv_unigold.where(filter_district)
    qs_hiv_unigold = qs_hiv_unigold.annotate(de_name=Value('HIV tests done using Unigold', output_field=CharField()))
    qs_hiv_unigold = qs_hiv_unigold.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_hiv_unigold = qs_hiv_unigold.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_hiv_unigold = qs_hiv_unigold.annotate(period=F('quarter'))
    qs_hiv_unigold = qs_hiv_unigold.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_hiv_unigold = qs_hiv_unigold.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_hiv_unigold = list(val_hiv_unigold)

    gen_raster = grabbag.rasterize(ou_list, de_hiv_unigold_meta, val_hiv_unigold, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_hiv_unigold2 = list(gen_raster)

    tb_smear_de_names = (
        '105-7.6 Lab ZN for AFBs  Number Done',
    )
    tb_smear_short_names = (
        'TB Smear',
    )
    de_tb_smear_meta = list(product(tb_smear_de_names, (None,)))

    qs_tb_smear = DataValue.objects.what(*tb_smear_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_tb_smear = qs_tb_smear.where(filter_district)
    qs_tb_smear = qs_tb_smear.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_tb_smear = qs_tb_smear.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_tb_smear = qs_tb_smear.annotate(period=F('quarter'))
    qs_tb_smear = qs_tb_smear.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_tb_smear = qs_tb_smear.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_tb_smear = list(val_tb_smear)

    gen_raster = grabbag.rasterize(ou_list, de_tb_smear_meta, val_tb_smear, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_tb_smear2 = list(gen_raster)

    syphilis_de_names = (
        '105-7.4 Lab VDRL/RPR Number Done',
        '105-7.4 Lab TPHA  Number Done',
    )
    syphilis_short_names = (
        'Syphilis tests',
    )
    de_syphilis_meta = list(product(['Syphilis tests'], (None,)))

    qs_syphilis = DataValue.objects.what(*syphilis_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_syphilis = qs_syphilis.where(filter_district)
    qs_syphilis = qs_syphilis.annotate(de_name=Value('Syphilis tests', output_field=CharField()))
    qs_syphilis = qs_syphilis.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_syphilis = qs_syphilis.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_syphilis = qs_syphilis.annotate(period=F('quarter'))
    qs_syphilis = qs_syphilis.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_syphilis = qs_syphilis.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_syphilis = list(val_syphilis)

    gen_raster = grabbag.rasterize(ou_list, de_syphilis_meta, val_syphilis, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_syphilis2 = list(gen_raster)

    liver_de_names = (
        '105-7.7 Lab ALT Number Done',
        '105-7.7 Lab AST Number Done',
        '105-7.7 Lab Albumin  Number Done',
    )
    liver_short_names = (
        'LFTs',
    )
    de_liver_meta = list(product(['LFTs'], (None,)))

    qs_liver = DataValue.objects.what(*liver_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_liver = qs_liver.where(filter_district)
    qs_liver = qs_liver.annotate(de_name=Value('LFTs', output_field=CharField()))
    qs_liver = qs_liver.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_liver = qs_liver.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_liver = qs_liver.annotate(period=F('quarter'))
    qs_liver = qs_liver.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_liver = qs_liver.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_liver = list(val_liver)

    gen_raster = grabbag.rasterize(ou_list, de_liver_meta, val_liver, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_liver2 = list(gen_raster)

    renal_de_names = (
        '105-7.7 Lab Calcium  Number Done',
        '105-7.7 Lab Creatinine Number Done',
        '105-7.7 Lab Potassium Number Done',
        '105-7.7 Lab Sodium Number Done',
        '105-7.7 Lab Total Protein Number Done',
        '105-7.7 Lab Urea Number Done',
    )
    renal_short_names = (
        'RFTs',
    )
    de_renal_meta = list(product(['RFTs'], (None,)))

    qs_renal = DataValue.objects.what(*renal_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_renal = qs_renal.where(filter_district)
    qs_renal = qs_renal.annotate(de_name=Value('RFTs', output_field=CharField()))
    qs_renal = qs_renal.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_renal = qs_renal.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_renal = qs_renal.annotate(period=F('quarter'))
    qs_renal = qs_renal.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_renal = qs_renal.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_renal = list(val_renal)

    gen_raster = grabbag.rasterize(ou_list, de_renal_meta, val_renal, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_renal2 = list(gen_raster)

    other_haem_de_names = (
        'All Other Haematology - Lab - OPD  Number Done',
    )
    other_haem_short_names = (
        'All other Haematology',
    )
    de_other_haem_meta = list(product(other_haem_de_names, (None,)))

    qs_other_haem = DataValue.objects.what(*other_haem_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_other_haem = qs_other_haem.where(filter_district)
    qs_other_haem = qs_other_haem.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_other_haem = qs_other_haem.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_other_haem = qs_other_haem.annotate(period=F('quarter'))
    qs_other_haem = qs_other_haem.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_other_haem = qs_other_haem.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_other_haem = list(val_other_haem)

    gen_raster = grabbag.rasterize(ou_list, de_other_haem_meta, val_other_haem, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_other_haem2 = list(gen_raster)

    # combine the data and group by district, subcounty and facility
    grouped_vals = groupbylist(sorted(chain(val_malaria2, val_hiv_determine2, val_hiv_statpak2, val_hiv_unigold2, val_tb_smear2, val_syphilis2, val_liver2,val_renal2, val_other_haem2), key=lambda x: (x['district'], x['subcounty'], x['facility'])), key=lambda x: (x['district'], x['subcounty'], x['facility']))
    if True:
        grouped_vals = list(filter_empty_rows(grouped_vals))

    # perform calculations
    for _group in grouped_vals:
        (district_subcounty_facility, (malaria_microscopy, malaria_rdt, *other_vals)) = _group
        
        calculated_vals = list()

        malaria_sum = default_zero(malaria_microscopy['numeric_sum']) + default_zero(malaria_rdt['numeric_sum'])
        malaria_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'Malaria (Smear & RDTs)',
            'cat_combo': None,
            'numeric_sum': malaria_sum,
        }
        calculated_vals.append(malaria_val)

        _group[1].extend(calculated_vals)

    data_element_names = list()
    data_element_names += list(product(malaria_short_names, (None,)))
    data_element_names += list(product(hiv_determine_short_names, (None,)))
    data_element_names += list(product(hiv_statpak_short_names, (None,)))
    data_element_names += list(product(hiv_unigold_short_names, (None,)))
    data_element_names += list(product(tb_smear_short_names, (None,)))
    data_element_names += list(product(syphilis_short_names, (None,)))
    data_element_names += list(product(liver_short_names, (None,)))
    data_element_names += list(product(renal_short_names, (None,)))
    data_element_names += list(product(other_haem_short_names, (None,)))

    data_element_names += list(product(['Malaria (Smear & RDTs)'], (None,)))

    legend_sets = list()
    # lab_ls = LegendSet()
    # lab_ls.add_interval('orange', 0, 25)
    # lab_ls.add_interval('yellow', 25, 40)
    # lab_ls.add_interval('light-green', 50, 60)
    # lab_ls.add_interval('green', 60, None)
    # legend_sets.append(lab_ls)

    if output_format == 'EXCEL':
        from django.http import HttpResponse
        import openpyxl

        wb = openpyxl.workbook.Workbook()
        ws = wb.active # workbooks are created with at least one worksheet
        ws.title = 'Sheet1' # unfortunately it is named "Sheet" not "Sheet1"
        ws.page_setup.orientation = ws.ORIENTATION_LANDSCAPE
        ws.page_setup.paperSize = ws.PAPERSIZE_A4

        headers = ou_headers + data_element_names
        for i, name in enumerate(headers, start=1):
            c = ws.cell(row=1, column=i)
            if not isinstance(name, tuple):
                c.value = str(name)
            else:
                de, cat_combo = name
                if cat_combo is None:
                    c.value = str(de)
                else:
                    c.value = str(de) + '\n' + str(cat_combo)
        for i, g in enumerate(grouped_vals, start=2):
            ou_path, g_val_list = g
            for col_idx, ou in enumerate(ou_path, start=1):
                ws.cell(row=i, column=col_idx, value=ou)
            for j, g_val in enumerate(g_val_list, start=len(ou_path)+1):
                ws.cell(row=i, column=j, value=g_val['numeric_sum'])

        for ls in legend_sets:
            for rule in ls.openpyxl_rules():
                for cell_range in ls.excel_ranges():
                    print(cell_range, ls)
                    ws.conditional_formatting.add(cell_range, rule)


        response = HttpResponse(openpyxl.writer.excel.save_virtual_workbook(wb), content_type='application/vnd.ms-excel')
        response['Content-Disposition'] = 'attachment; filename="lab_scorecard.xlsx"'

        return response

    context = {
        'grouped_data': grouped_vals,
        'data_element_names': data_element_names,
        'legend_sets': legend_sets,
        'period_desc': period_desc,
        'period_list': PREV_5YR_QTRS,
        'district_list': DISTRICT_LIST,
    }

    return render(request, 'cannula/lab_sites.html', context)

@login_required
def fp_by_site(request, output_format='HTML'):
    this_day = date.today()
    this_year = this_day.year
    PREV_5YR_QTRS = ['%d-Q%d' % (y, q) for y in range(this_year, this_year-6, -1) for q in range(4, 0, -1)]
    DISTRICT_LIST = list(OrgUnit.objects.filter(level=1).order_by('name').values_list('name', flat=True))

    if 'period' in request.GET and request.GET['period'] in PREV_5YR_QTRS:
        filter_period=request.GET['period']
    else:
        filter_period = '%d-Q%d' % (this_year, month2quarter(this_day.month))

    period_desc = dateutil.DateSpan.fromquarter(filter_period).format()

    if 'district' in request.GET and request.GET['district'] in DISTRICT_LIST:
        filter_district = OrgUnit.objects.get(name=request.GET['district'])
    else:
        filter_district = None

    # # all facilities (or equivalent)
    qs_ou = OrgUnit.objects.filter(level=3).annotate(district=F('parent__parent__name'), subcounty=F('parent__name'), facility=F('name'))
    if filter_district:
        qs_ou = qs_ou.filter(Q(lft__gte=filter_district.lft) & Q(rght__lte=filter_district.rght))
    ou_list = list(qs_ou.values_list('district', 'subcounty', 'facility'))
    ou_headers = ['District', 'Subcounty', 'Facility']

    def val_with_subcat_fun(row, col):
        district, subcounty, facility = row
        de_name, subcategory = col
        return { 'district': district, 'subcounty': subcounty, 'facility': facility, 'cat_combo': subcategory, 'de_name': de_name, 'numeric_sum': None }

    condoms_new_de_names = (
        '105-2.5 Female Condom',
        '105-2.5 Male Condom',
    )
    condoms_new_short_names = (
        'New users - Condoms',
    )
    de_condoms_new_meta = list(product(condoms_new_short_names, (None,)))

    qs_condoms_new = DataValue.objects.what(*condoms_new_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_condoms_new = qs_condoms_new.where(filter_district)
    qs_condoms_new = qs_condoms_new.annotate(de_name=Value(condoms_new_short_names[0], output_field=CharField()))
    qs_condoms_new = qs_condoms_new.filter(category_combo__categories__name='New Users')
    qs_condoms_new = qs_condoms_new.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_condoms_new = qs_condoms_new.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_condoms_new = qs_condoms_new.annotate(period=F('quarter'))
    qs_condoms_new = qs_condoms_new.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_condoms_new = qs_condoms_new.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_condoms_new = list(val_condoms_new)

    gen_raster = grabbag.rasterize(ou_list, de_condoms_new_meta, val_condoms_new, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_condoms_new2 = list(gen_raster)

    fp_new_de_names = (
        '105-2.5 Injectable',
        '105-2.5 IUDs',
        '105-2.5 Natural',
        '105-2.7 Implant',
    )
    fp_new_short_names = (
        'New users - Injectables',
        'New users - IUDs',
        'New users - Natural methods',
        'New users - Implants',
    )
    de_fp_new_meta = list(product(fp_new_de_names, (None,)))

    qs_fp_new = DataValue.objects.what(*fp_new_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_fp_new = qs_fp_new.where(filter_district)
    qs_fp_new = qs_fp_new.filter(category_combo__categories__name='New Users')
    qs_fp_new = qs_fp_new.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_fp_new = qs_fp_new.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_fp_new = qs_fp_new.annotate(period=F('quarter'))
    qs_fp_new = qs_fp_new.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_fp_new = qs_fp_new.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_fp_new = list(val_fp_new)

    gen_raster = grabbag.rasterize(ou_list, de_fp_new_meta, val_fp_new, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_fp_new2 = list(gen_raster)

    oral_new_de_names = (
        '105-2.5 Oral: Microgynon',
        '105-2.5 Oral: Lo-Feminal',
        '105-2.5 Oral : Ovrette or Another POP',
    )
    oral_new_short_names = (
        'New users - Oral',
    )
    de_oral_new_meta = list(product(oral_new_short_names, (None,)))

    qs_oral_new = DataValue.objects.what(*oral_new_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_oral_new = qs_oral_new.where(filter_district)
    qs_oral_new = qs_oral_new.annotate(de_name=Value(oral_new_short_names[0], output_field=CharField()))
    qs_oral_new = qs_oral_new.filter(category_combo__categories__name='New Users')
    qs_oral_new = qs_oral_new.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_oral_new = qs_oral_new.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_oral_new = qs_oral_new.annotate(period=F('quarter'))
    qs_oral_new = qs_oral_new.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_oral_new = qs_oral_new.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_oral_new = list(val_oral_new)

    gen_raster = grabbag.rasterize(ou_list, de_oral_new_meta, val_oral_new, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_oral_new2 = list(gen_raster)

    other_new_de_names = (
        '105-2.5 Other Method',
    )
    other_new_short_names = (
        'New users - Other methods',
    )
    de_other_new_meta = list(product(other_new_short_names, (None,)))

    qs_other_new = DataValue.objects.what(*other_new_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_other_new = qs_other_new.where(filter_district)
    qs_other_new = qs_other_new.annotate(de_name=Value(other_new_short_names[0], output_field=CharField()))
    qs_other_new = qs_other_new.filter(category_combo__categories__name='New Users')
    qs_other_new = qs_other_new.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_other_new = qs_other_new.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_other_new = qs_other_new.annotate(period=F('quarter'))
    qs_other_new = qs_other_new.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_other_new = qs_other_new.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_other_new = list(val_other_new)

    gen_raster = grabbag.rasterize(ou_list, de_other_new_meta, val_other_new, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_other_new2 = list(gen_raster)

    sterile_new_de_names = (
        '105-2.7 Female Sterilisation (TubeLigation)',
        '105-2.7 Male Sterilisation (Vasectomy)',
    )
    sterile_new_short_names = (
        'New users - Sterilisation (male and female)',
    )
    de_sterile_new_meta = list(product(sterile_new_short_names, (None,)))

    qs_sterile_new = DataValue.objects.what(*sterile_new_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_sterile_new = qs_sterile_new.where(filter_district)
    qs_sterile_new = qs_sterile_new.annotate(de_name=Value(sterile_new_short_names[0], output_field=CharField()))
    qs_sterile_new = qs_sterile_new.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_sterile_new = qs_sterile_new.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_sterile_new = qs_sterile_new.annotate(period=F('quarter'))
    qs_sterile_new = qs_sterile_new.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_sterile_new = qs_sterile_new.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_sterile_new = list(val_sterile_new)

    gen_raster = grabbag.rasterize(ou_list, de_sterile_new_meta, val_sterile_new, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_sterile_new2 = list(gen_raster)

    condoms_revisit_de_names = (
        '105-2.5 Female Condom',
        '105-2.5 Male Condom',
    )
    condoms_revisit_short_names = (
        'Revisits - Condoms',
    )
    de_condoms_revisit_meta = list(product(condoms_revisit_short_names, (None,)))

    qs_condoms_revisit = DataValue.objects.what(*condoms_revisit_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_condoms_revisit = qs_condoms_revisit.where(filter_district)
    qs_condoms_revisit = qs_condoms_revisit.annotate(de_name=Value(condoms_revisit_short_names[0], output_field=CharField()))
    qs_condoms_revisit = qs_condoms_revisit.filter(category_combo__categories__name='Revisits')
    qs_condoms_revisit = qs_condoms_revisit.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_condoms_revisit = qs_condoms_revisit.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_condoms_revisit = qs_condoms_revisit.annotate(period=F('quarter'))
    qs_condoms_revisit = qs_condoms_revisit.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_condoms_revisit = qs_condoms_revisit.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_condoms_revisit = list(val_condoms_revisit)

    gen_raster = grabbag.rasterize(ou_list, de_condoms_revisit_meta, val_condoms_revisit, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_condoms_revisit2 = list(gen_raster)

    fp_revisit_de_names = (
        '105-2.5 Injectable',
        '105-2.5 IUDs',
        '105-2.5 Natural',
        '105-2.7 Implant',
    )
    fp_revisit_short_names = (
        'Revisits - Injectables',
        'Revisits - IUDs',
        'Revisits - Natural methods',
        'Revisits - Implants',
    )
    de_fp_revisit_meta = list(product(fp_revisit_de_names, (None,)))

    qs_fp_revisit = DataValue.objects.what(*fp_revisit_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_fp_revisit = qs_fp_revisit.where(filter_district)
    qs_fp_revisit = qs_fp_revisit.filter(category_combo__categories__name='Revisits')
    qs_fp_revisit = qs_fp_revisit.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_fp_revisit = qs_fp_revisit.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_fp_revisit = qs_fp_revisit.annotate(period=F('quarter'))
    qs_fp_revisit = qs_fp_revisit.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_fp_revisit = qs_fp_revisit.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_fp_revisit = list(val_fp_revisit)

    gen_raster = grabbag.rasterize(ou_list, de_fp_revisit_meta, val_fp_revisit, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_fp_revisit2 = list(gen_raster)

    oral_revisit_de_names = (
        '105-2.5 Oral: Microgynon',
        '105-2.5 Oral: Lo-Feminal',
        '105-2.5 Oral : Ovrette or Another POP',
    )
    oral_revisit_short_names = (
        'Revisits - Oral',
    )
    de_oral_revisit_meta = list(product(oral_revisit_short_names, (None,)))

    qs_oral_revisit = DataValue.objects.what(*oral_revisit_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_oral_revisit = qs_oral_revisit.where(filter_district)
    qs_oral_revisit = qs_oral_revisit.annotate(de_name=Value(oral_revisit_short_names[0], output_field=CharField()))
    qs_oral_revisit = qs_oral_revisit.filter(category_combo__categories__name='Revisits')
    qs_oral_revisit = qs_oral_revisit.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_oral_revisit = qs_oral_revisit.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_oral_revisit = qs_oral_revisit.annotate(period=F('quarter'))
    qs_oral_revisit = qs_oral_revisit.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_oral_revisit = qs_oral_revisit.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_oral_revisit = list(val_oral_revisit)

    gen_raster = grabbag.rasterize(ou_list, de_oral_revisit_meta, val_oral_revisit, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_oral_revisit2 = list(gen_raster)

    other_revisit_de_names = (
        '105-2.5 Other Method',
    )
    other_revisit_short_names = (
        'Revisits - Other methods',
    )
    de_other_revisit_meta = list(product(other_revisit_short_names, (None,)))

    qs_other_revisit = DataValue.objects.what(*other_revisit_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_other_revisit = qs_other_revisit.where(filter_district)
    qs_other_revisit = qs_other_revisit.annotate(de_name=Value(other_revisit_short_names[0], output_field=CharField()))
    qs_other_revisit = qs_other_revisit.filter(category_combo__categories__name='Revisits')
    qs_other_revisit = qs_other_revisit.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_other_revisit = qs_other_revisit.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_other_revisit = qs_other_revisit.annotate(period=F('quarter'))
    qs_other_revisit = qs_other_revisit.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_other_revisit = qs_other_revisit.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_other_revisit = list(val_other_revisit)

    gen_raster = grabbag.rasterize(ou_list, de_other_revisit_meta, val_other_revisit, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_other_revisit2 = list(gen_raster)

    hiv_new_de_names = (
        '105-2.5 Number HIV+ FP users',
    )
    hiv_new_short_names = (
        'New users - HIV+',
    )
    de_hiv_new_meta = list(product(hiv_new_short_names, (None,)))

    qs_hiv_new = DataValue.objects.what(*hiv_new_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_hiv_new = qs_hiv_new.where(filter_district)
    qs_hiv_new = qs_hiv_new.annotate(de_name=Value(hiv_new_short_names[0], output_field=CharField()))
    qs_hiv_new = qs_hiv_new.filter(category_combo__categories__name='New Users')
    qs_hiv_new = qs_hiv_new.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_hiv_new = qs_hiv_new.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_hiv_new = qs_hiv_new.annotate(period=F('quarter'))
    qs_hiv_new = qs_hiv_new.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_hiv_new = qs_hiv_new.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_hiv_new = list(val_hiv_new)

    gen_raster = grabbag.rasterize(ou_list, de_hiv_new_meta, val_hiv_new, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_hiv_new2 = list(gen_raster)

    hiv_revisit_de_names = (
        '105-2.5 Number HIV+ FP users',
    )
    hiv_revisit_short_names = (
        'Revisits - HIV+',
    )
    de_hiv_revisit_meta = list(product(hiv_revisit_short_names, (None,)))

    qs_hiv_revisit = DataValue.objects.what(*hiv_revisit_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_hiv_revisit = qs_hiv_revisit.where(filter_district)
    qs_hiv_revisit = qs_hiv_revisit.annotate(de_name=Value(hiv_revisit_short_names[0], output_field=CharField()))
    qs_hiv_revisit = qs_hiv_revisit.filter(category_combo__categories__name='Revisits')
    qs_hiv_revisit = qs_hiv_revisit.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_hiv_revisit = qs_hiv_revisit.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_hiv_revisit = qs_hiv_revisit.annotate(period=F('quarter'))
    qs_hiv_revisit = qs_hiv_revisit.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_hiv_revisit = qs_hiv_revisit.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_hiv_revisit = list(val_hiv_revisit)

    gen_raster = grabbag.rasterize(ou_list, de_hiv_revisit_meta, val_hiv_revisit, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_hiv_revisit2 = list(gen_raster)

    # combine the data and group by district, subcounty and facility
    grouped_vals = groupbylist(sorted(chain(val_condoms_new2, val_fp_new2, val_oral_new2, val_other_new2, val_sterile_new2, val_condoms_revisit2, val_fp_revisit2, val_oral_revisit2, val_other_revisit2, val_hiv_new2, val_hiv_revisit2), key=lambda x: (x['district'], x['subcounty'], x['facility'])), key=lambda x: (x['district'], x['subcounty'], x['facility']))
    if True:
        grouped_vals = list(filter_empty_rows(grouped_vals))

    # perform calculations
    for _group in grouped_vals:
        (district_subcounty_facility, (condom_new, inject_new, iud_new, natural_new, implant_new, oral_new, other_new, sterile_new, condom_revisit, inject_revisit, iud_revisit, natural_revisit, implant_revisit, oral_revisit, other_revisit, hiv_new, hiv_revisit, *other_vals)) = _group
        
        calculated_vals = list()

        total_new_sum = default_zero(condom_new['numeric_sum']) + default_zero(inject_new['numeric_sum']) + default_zero(iud_new['numeric_sum']) + default_zero(natural_new['numeric_sum']) + default_zero(implant_new['numeric_sum']) + default_zero(oral_new['numeric_sum']) + default_zero(other_new['numeric_sum']) + default_zero(sterile_new['numeric_sum'])
        total_new_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'New Users - TOTAL',
            'cat_combo': None,
            'numeric_sum': total_new_sum,
        }
        calculated_vals.append(total_new_val)

        total_revisit_sum = default_zero(condom_revisit['numeric_sum']) + default_zero(inject_revisit['numeric_sum']) + default_zero(iud_revisit['numeric_sum']) + default_zero(natural_revisit['numeric_sum']) + default_zero(implant_revisit['numeric_sum']) + default_zero(oral_revisit['numeric_sum']) + default_zero(other_revisit['numeric_sum'])
        total_revisit_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'Revisits - TOTAL',
            'cat_combo': None,
            'numeric_sum': total_revisit_sum,
        }
        calculated_vals.append(total_revisit_val)

        _group[1].extend(calculated_vals)

    data_element_names = list()
    data_element_names += list(product(condoms_new_short_names, (None,)))
    data_element_names += list(product(fp_new_short_names, (None,)))
    data_element_names += list(product(oral_new_short_names, (None,)))
    data_element_names += list(product(other_new_short_names, (None,)))
    data_element_names += list(product(sterile_new_short_names, (None,)))
    data_element_names += list(product(condoms_revisit_short_names, (None,)))
    data_element_names += list(product(fp_revisit_short_names, (None,)))
    data_element_names += list(product(oral_revisit_short_names, (None,)))
    data_element_names += list(product(other_revisit_short_names, (None,)))
    data_element_names += list(product(hiv_new_short_names, (None,)))
    data_element_names += list(product(hiv_revisit_short_names, (None,)))

    data_element_names += list(product(['New Users - TOTAL'], (None,)))
    data_element_names += list(product(['Revisits - TOTAL'], (None,)))

    legend_sets = list()
    # fp_ls = LegendSet()
    # fp_ls.add_interval('orange', 0, 25)
    # fp_ls.add_interval('yellow', 25, 40)
    # fp_ls.add_interval('light-green', 50, 60)
    # fp_ls.add_interval('green', 60, None)
    # legend_sets.append(fp_ls)

    if output_format == 'EXCEL':
        from django.http import HttpResponse
        import openpyxl

        wb = openpyxl.workbook.Workbook()
        ws = wb.active # workbooks are created with at least one worksheet
        ws.title = 'Sheet1' # unfortunately it is named "Sheet" not "Sheet1"
        ws.page_setup.orientation = ws.ORIENTATION_LANDSCAPE
        ws.page_setup.paperSize = ws.PAPERSIZE_A4

        headers = ou_headers + data_element_names
        for i, name in enumerate(headers, start=1):
            c = ws.cell(row=1, column=i)
            if not isinstance(name, tuple):
                c.value = str(name)
            else:
                de, cat_combo = name
                if cat_combo is None:
                    c.value = str(de)
                else:
                    c.value = str(de) + '\n' + str(cat_combo)
        for i, g in enumerate(grouped_vals, start=2):
            ou_path, g_val_list = g
            for col_idx, ou in enumerate(ou_path, start=1):
                ws.cell(row=i, column=col_idx, value=ou)
            for j, g_val in enumerate(g_val_list, start=len(ou_path)+1):
                ws.cell(row=i, column=j, value=g_val['numeric_sum'])

        for ls in legend_sets:
            # apply conditional formatting from LegendSets
            for rule in ls.openpyxl_rules():
                for cell_range in ls.excel_ranges():
                    print(cell_range, ls)
                    ws.conditional_formatting.add(cell_range, rule)


        response = HttpResponse(openpyxl.writer.excel.save_virtual_workbook(wb), content_type='application/vnd.ms-excel')
        response['Content-Disposition'] = 'attachment; filename="family_planning_sites_scorecard.xlsx"'

        return response

    context = {
        'grouped_data': grouped_vals,
        'data_element_names': data_element_names,
        'legend_sets': legend_sets,
        'period_desc': period_desc,
        'period_list': PREV_5YR_QTRS,
        'district_list': DISTRICT_LIST,
    }

    return render(request, 'cannula/fp_sites.html', context)

@login_required
def fp_cyp_by_site(request, output_format='HTML'):
    this_day = date.today()
    this_year = this_day.year
    PREV_5YR_QTRS = ['%d-Q%d' % (y, q) for y in range(this_year, this_year-6, -1) for q in range(4, 0, -1)]
    DISTRICT_LIST = list(OrgUnit.objects.filter(level=1).order_by('name').values_list('name', flat=True))

    if 'period' in request.GET and request.GET['period'] in PREV_5YR_QTRS:
        filter_period=request.GET['period']
    else:
        filter_period = '%d-Q%d' % (this_year, month2quarter(this_day.month))

    period_desc = dateutil.DateSpan.fromquarter(filter_period).format()

    if 'district' in request.GET and request.GET['district'] in DISTRICT_LIST:
        filter_district = OrgUnit.objects.get(name=request.GET['district'])
    else:
        filter_district = None

    # # all facilities (or equivalent)
    qs_ou = OrgUnit.objects.filter(level=3).annotate(district=F('parent__parent__name'), subcounty=F('parent__name'), facility=F('name'))
    if filter_district:
        qs_ou = qs_ou.filter(Q(lft__gte=filter_district.lft) & Q(rght__lte=filter_district.rght))
    ou_list = list(qs_ou.values_list('district', 'subcounty', 'facility'))
    ou_headers = ['District', 'Subcounty', 'Facility']

    def val_with_subcat_fun(row, col):
        district, subcounty, facility = row
        de_name, subcategory = col
        return { 'district': district, 'subcounty': subcounty, 'facility': facility, 'cat_combo': subcategory, 'de_name': de_name, 'numeric_sum': None }

    oral_de_names = (
        '105-2.5 Oral: Microgynon',
        '105-2.5 Oral: Lo-Feminal',
        '105-2.5 Oral : Ovrette or Another POP',
    )
    oral_short_names = (
        'Oral dispensed (cycles)',
    )
    de_oral_meta = list(product(oral_short_names, (None,)))

    qs_oral = DataValue.objects.what(*oral_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_oral = qs_oral.where(filter_district)
    qs_oral = qs_oral.annotate(de_name=Value(oral_short_names[0], output_field=CharField()))
    qs_oral = qs_oral.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_oral = qs_oral.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_oral = qs_oral.annotate(period=F('quarter'))
    qs_oral = qs_oral.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_oral = qs_oral.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_oral = list(val_oral)

    gen_raster = grabbag.rasterize(ou_list, de_oral_meta, val_oral, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_oral2 = list(gen_raster)

    condoms_de_names = (
        '105-2.5 Female Condom',
        '105-2.5 Male Condom',
    )
    condoms_short_names = (
        'Condoms dispensed (pieces)',
    )
    de_condoms_meta = list(product(condoms_short_names, (None,)))

    qs_condoms = DataValue.objects.what(*condoms_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_condoms = qs_condoms.where(filter_district)
    qs_condoms = qs_condoms.annotate(de_name=Value(condoms_short_names[0], output_field=CharField()))
    qs_condoms = qs_condoms.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_condoms = qs_condoms.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_condoms = qs_condoms.annotate(period=F('quarter'))
    qs_condoms = qs_condoms.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_condoms = qs_condoms.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_condoms = list(val_condoms)

    gen_raster = grabbag.rasterize(ou_list, de_condoms_meta, val_condoms, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_condoms2 = list(gen_raster)

    implants_new_de_names = (
        '105-2.7 Implant',
    )
    implants_new_short_names = (
        'New users - Implants',
    )
    de_implants_new_meta = list(product(implants_new_short_names, (None,)))

    qs_implants_new = DataValue.objects.what(*implants_new_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_implants_new = qs_implants_new.where(filter_district)
    qs_implants_new = qs_implants_new.annotate(de_name=Value(implants_new_short_names[0], output_field=CharField()))
    qs_implants_new = qs_implants_new.filter(category_combo__categories__name='New Users')
    qs_implants_new = qs_implants_new.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_implants_new = qs_implants_new.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_implants_new = qs_implants_new.annotate(period=F('quarter'))
    qs_implants_new = qs_implants_new.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_implants_new = qs_implants_new.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_implants_new = list(val_implants_new)

    gen_raster = grabbag.rasterize(ou_list, de_implants_new_meta, val_implants_new, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_implants_new2 = list(gen_raster)

    injectable_de_names = (
        '105-2.5 Injectable',
    )
    injectable_short_names = (
        'Injectable dispensed (doses)',
    )
    de_injectable_meta = list(product(injectable_short_names, (None,)))

    qs_injectable = DataValue.objects.what(*injectable_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_injectable = qs_injectable.where(filter_district)
    qs_injectable = qs_injectable.annotate(de_name=Value(injectable_short_names[0], output_field=CharField()))
    qs_injectable = qs_injectable.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_injectable = qs_injectable.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_injectable = qs_injectable.annotate(period=F('quarter'))
    qs_injectable = qs_injectable.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_injectable = qs_injectable.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_injectable = list(val_injectable)

    gen_raster = grabbag.rasterize(ou_list, de_injectable_meta, val_injectable, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_injectable2 = list(gen_raster)

    iud_de_names = (
        '105-2.5 IUDs',
    )
    iud_short_names = (
        'IUD inserted',
    )
    de_iud_meta = list(product(iud_short_names, (None,)))

    qs_iud = DataValue.objects.what(*iud_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_iud = qs_iud.where(filter_district)
    qs_iud = qs_iud.annotate(de_name=Value(iud_short_names[0], output_field=CharField()))
    qs_iud = qs_iud.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_iud = qs_iud.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_iud = qs_iud.annotate(period=F('quarter'))
    qs_iud = qs_iud.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_iud = qs_iud.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_iud = list(val_iud)

    gen_raster = grabbag.rasterize(ou_list, de_iud_meta, val_iud, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_iud2 = list(gen_raster)

    sterile_new_de_names = (
        '105-2.7 Female Sterilisation (TubeLigation)',
        '105-2.7 Male Sterilisation (Vasectomy)',
    )
    sterile_new_short_names = (
        'New users - Sterilisation (male and female)',
    )
    de_sterile_new_meta = list(product(sterile_new_short_names, (None,)))

    qs_sterile_new = DataValue.objects.what(*sterile_new_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_sterile_new = qs_sterile_new.where(filter_district)
    qs_sterile_new = qs_sterile_new.annotate(de_name=Value(sterile_new_short_names[0], output_field=CharField()))
    qs_sterile_new = qs_sterile_new.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_sterile_new = qs_sterile_new.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_sterile_new = qs_sterile_new.annotate(period=F('quarter'))
    qs_sterile_new = qs_sterile_new.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_sterile_new = qs_sterile_new.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_sterile_new = list(val_sterile_new)

    gen_raster = grabbag.rasterize(ou_list, de_sterile_new_meta, val_sterile_new, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_sterile_new2 = list(gen_raster)

    natural_de_names = (
        '105-2.5 Natural',
    )
    natural_short_names = (
        'Natural methods',
    )
    de_natural_meta = list(product(natural_short_names, (None,)))

    qs_natural = DataValue.objects.what(*natural_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_natural = qs_natural.where(filter_district)
    qs_natural = qs_natural.annotate(de_name=Value(natural_short_names[0], output_field=CharField()))
    qs_natural = qs_natural.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_natural = qs_natural.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_natural = qs_natural.annotate(period=F('quarter'))
    qs_natural = qs_natural.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_natural = qs_natural.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_natural = list(val_natural)

    gen_raster = grabbag.rasterize(ou_list, de_natural_meta, val_natural, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_natural2 = list(gen_raster)

    emergency_de_names = (
        '105-2.6 Emergency contraceptives  No. Dispensed by CBD',
        '105-2.6 Emergency contraceptives  No. Dispensed at Unit',
        '105-2.6 Emergency contraceptives  No. Disp. At Outreach',
    )
    emergency_short_names = (
        'Emergency contraceptives dispensed (doses)',
    )
    de_emergency_meta = list(product(emergency_short_names, (None,)))

    qs_emergency = DataValue.objects.what(*emergency_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_emergency = qs_emergency.where(filter_district)
    qs_emergency = qs_emergency.annotate(de_name=Value(emergency_short_names[0], output_field=CharField()))
    qs_emergency = qs_emergency.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_emergency = qs_emergency.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_emergency = qs_emergency.annotate(period=F('quarter'))
    qs_emergency = qs_emergency.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_emergency = qs_emergency.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_emergency = list(val_emergency)

    gen_raster = grabbag.rasterize(ou_list, de_emergency_meta, val_emergency, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_emergency2 = list(gen_raster)

    # combine the data and group by district, subcounty and facility
    grouped_vals = groupbylist(sorted(chain(val_oral2, val_condoms2, val_implants_new2, val_injectable2, val_iud2, val_sterile_new2, val_natural2, val_emergency2), key=lambda x: (x['district'], x['subcounty'], x['facility'])), key=lambda x: (x['district'], x['subcounty'], x['facility']))
    if True:
        grouped_vals = list(filter_empty_rows(grouped_vals))

    # perform calculations
    for _group in grouped_vals:
        (district_subcounty_facility, (oral, condoms, implants_new, injectable, iud, sterile_new, natural, emergency, *other_vals)) = _group
        
        calculated_vals = list()

        if all_not_none(oral['numeric_sum']):
            cyp_oral = oral['numeric_sum'] / 15
        else:
            cyp_oral = None
        cyp_oral_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'CYPs Oral',
            'cat_combo': None,
            'numeric_sum': cyp_oral,
        }
        calculated_vals.append(cyp_oral_val)

        if all_not_none(condoms['numeric_sum']):
            cyp_condoms = condoms['numeric_sum'] / 120
        else:
            cyp_condoms = None
        cyp_condoms_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'CYPs Condoms',
            'cat_combo': None,
            'numeric_sum': cyp_condoms,
        }
        calculated_vals.append(cyp_condoms_val)

        if all_not_none(implants_new['numeric_sum']):
            cyp_implants = implants_new['numeric_sum'] * Decimal(2.5)
        else:
            cyp_implants = None
        cyp_implants_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'CYPs Implants',
            'cat_combo': None,
            'numeric_sum': cyp_implants,
        }
        calculated_vals.append(cyp_implants_val)

        if all_not_none(injectable['numeric_sum']):
            cyp_injectable = injectable['numeric_sum'] / 4
        else:
            cyp_injectable = None
        cyp_injectable_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'CYPs Injectable',
            'cat_combo': None,
            'numeric_sum': cyp_injectable,
        }
        calculated_vals.append(cyp_injectable_val)

        if all_not_none(iud['numeric_sum']):
            cyp_iud = iud['numeric_sum'] * Decimal(4.6)
        else:
            cyp_iud = None
        cyp_iud_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'CYPs IUD',
            'cat_combo': None,
            'numeric_sum': cyp_iud,
        }
        calculated_vals.append(cyp_iud_val)

        if all_not_none(sterile_new['numeric_sum']):
            cyp_sterile = sterile_new['numeric_sum'] * Decimal(10)
        else:
            cyp_sterile = None
        cyp_sterile_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'CYPs sterile',
            'cat_combo': None,
            'numeric_sum': cyp_sterile,
        }
        calculated_vals.append(cyp_sterile_val)

        if all_not_none(natural['numeric_sum']):
            cyp_natural = natural['numeric_sum'] / 4
        else:
            cyp_natural = None
        cyp_natural_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'CYPs Natural Methods',
            'cat_combo': None,
            'numeric_sum': cyp_natural,
        }
        calculated_vals.append(cyp_natural_val)

        if all_not_none(emergency['numeric_sum']):
            cyp_emergency = emergency['numeric_sum'] / 20
        else:
            cyp_emergency = None
        cyp_emergency_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'CYPs Emergency Contraceptives',
            'cat_combo': None,
            'numeric_sum': cyp_emergency,
        }
        calculated_vals.append(cyp_emergency_val)

        _group[1].extend(calculated_vals)

    data_element_names = list()
    data_element_names += list(product(oral_short_names, (None,)))
    data_element_names += list(product(condoms_short_names, (None,)))
    data_element_names += list(product(implants_new_short_names, (None,)))
    data_element_names += list(product(injectable_short_names, (None,)))
    data_element_names += list(product(iud_short_names, (None,)))
    data_element_names += list(product(sterile_new_short_names, (None,)))
    data_element_names += list(product(natural_short_names, (None,)))
    data_element_names += list(product(emergency_short_names, (None,)))

    data_element_names += list(product(['CYPs Oral'], (None,)))
    data_element_names += list(product(['CYPs Condoms'], (None,)))
    data_element_names += list(product(['CYPs Implants'], (None,)))
    data_element_names += list(product(['CYPs Injectable'], (None,)))
    data_element_names += list(product(['CYPs IUD'], (None,)))
    data_element_names += list(product(['CYP Sterilisation'], (None,)))
    data_element_names += list(product(['CYPs Natural Methods'], (None,)))
    data_element_names += list(product(['CYPs Emergency contraceptives'], (None,)))

    legend_sets = list()
    # fp_cyp_ls = LegendSet()
    # fp_cyp_ls.name = 'FP CYP'
    # fp_cyp_ls.add_interval('orange', 0, 25)
    # fp_cyp_ls.add_interval('yellow', 25, 40)
    # fp_cyp_ls.add_interval('light-green', 50, 60)
    # fp_cyp_ls.add_interval('green', 60, None)
    # legend_sets.append(fp_cyp_ls)

    if output_format == 'EXCEL':
        from django.http import HttpResponse
        import openpyxl

        wb = openpyxl.workbook.Workbook()
        ws = wb.active # workbooks are created with at least one worksheet
        ws.title = 'Sheet1' # unfortunately it is named "Sheet" not "Sheet1"
        ws.page_setup.orientation = ws.ORIENTATION_LANDSCAPE
        ws.page_setup.paperSize = ws.PAPERSIZE_A4

        headers = ou_headers + data_element_names
        for i, name in enumerate(headers, start=1):
            c = ws.cell(row=1, column=i)
            if not isinstance(name, tuple):
                c.value = str(name)
            else:
                de, cat_combo = name
                if cat_combo is None:
                    c.value = str(de)
                else:
                    c.value = str(de) + '\n' + str(cat_combo)
        for i, g in enumerate(grouped_vals, start=2):
            ou_path, g_val_list = g
            for col_idx, ou in enumerate(ou_path, start=1):
                ws.cell(row=i, column=col_idx, value=ou)
            for j, g_val in enumerate(g_val_list, start=len(ou_path)+1):
                ws.cell(row=i, column=j, value=g_val['numeric_sum'])

        for ls in legend_sets:
            # apply conditional formatting from LegendSets
            for rule in ls.openpyxl_rules():
                for cell_range in ls.excel_ranges():
                    print(cell_range, ls)
                    ws.conditional_formatting.add(cell_range, rule)


        response = HttpResponse(openpyxl.writer.excel.save_virtual_workbook(wb), content_type='application/vnd.ms-excel')
        response['Content-Disposition'] = 'attachment; filename="fp_cyp_sites_scorecard.xlsx"'

        return response

    context = {
        'grouped_data': grouped_vals,
        'ou_headers': ou_headers,
        'data_element_names': data_element_names,
        'legend_sets': legend_sets,
        'period_desc': period_desc,
        'period_list': PREV_5YR_QTRS,
        'district_list': DISTRICT_LIST,
        'excel_url': make_excel_url(request.path)
    }

    return render(request, 'cannula/fp_cyp_sites.html', context)

@login_required
def fp_cyp_by_district(request, output_format='HTML'):
    this_day = date.today()
    this_year = this_day.year
    PREV_5YR_QTRS = ['%d-Q%d' % (y, q) for y in range(this_year, this_year-6, -1) for q in range(4, 0, -1)]
    DISTRICT_LIST = list(OrgUnit.objects.filter(level=1).order_by('name').values_list('name', flat=True))

    if 'period' in request.GET and request.GET['period'] in PREV_5YR_QTRS:
        filter_period=request.GET['period']
    else:
        filter_period = '%d-Q%d' % (this_year, month2quarter(this_day.month))

    period_desc = dateutil.DateSpan.fromquarter(filter_period).format()

    if 'district' in request.GET and request.GET['district'] in DISTRICT_LIST:
        filter_district = OrgUnit.objects.get(name=request.GET['district'])
    else:
        filter_district = None

    # # all districts (or equivalent)
    qs_ou = OrgUnit.objects.filter(level=1).annotate(district=F('name'))
    if filter_district:
        qs_ou = qs_ou.filter(Q(lft__gte=filter_district.lft) & Q(rght__lte=filter_district.rght))
    ou_list = list(qs_ou.values_list('district'))
    ou_headers = ['District',]

    def val_with_subcat_fun(row, col):
        district, = row
        de_name, subcategory = col
        return { 'district': district, 'cat_combo': subcategory, 'de_name': de_name, 'numeric_sum': None }

    def get_ou_path(val):
        return (val['district'],)

    oral_de_names = (
        '105-2.5 Oral: Microgynon',
        '105-2.5 Oral: Lo-Feminal',
        '105-2.5 Oral : Ovrette or Another POP',
    )
    oral_short_names = (
        'Oral dispensed (cycles)',
    )
    de_oral_meta = list(product(oral_short_names, (None,)))

    qs_oral = DataValue.objects.what(*oral_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_oral = qs_oral.where(filter_district)
    qs_oral = qs_oral.annotate(de_name=Value(oral_short_names[0], output_field=CharField()))
    qs_oral = qs_oral.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_oral = qs_oral.annotate(district=F('org_unit__parent__parent__name'))
    qs_oral = qs_oral.annotate(period=F('quarter'))
    qs_oral = qs_oral.order_by('district', 'de_name', 'cat_combo', 'period')
    val_oral = qs_oral.values('district', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_oral = list(val_oral)

    gen_raster = grabbag.rasterize(ou_list, de_oral_meta, val_oral, get_ou_path, lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_oral2 = list(gen_raster)

    condoms_de_names = (
        '105-2.5 Female Condom',
        '105-2.5 Male Condom',
    )
    condoms_short_names = (
        'Condoms dispensed (pieces)',
    )
    de_condoms_meta = list(product(condoms_short_names, (None,)))

    qs_condoms = DataValue.objects.what(*condoms_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_condoms = qs_condoms.where(filter_district)
    qs_condoms = qs_condoms.annotate(de_name=Value(condoms_short_names[0], output_field=CharField()))
    qs_condoms = qs_condoms.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_condoms = qs_condoms.annotate(district=F('org_unit__parent__parent__name'))
    qs_condoms = qs_condoms.annotate(period=F('quarter'))
    qs_condoms = qs_condoms.order_by('district', 'de_name', 'cat_combo', 'period')
    val_condoms = qs_condoms.values('district', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_condoms = list(val_condoms)

    gen_raster = grabbag.rasterize(ou_list, de_condoms_meta, val_condoms, get_ou_path, lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_condoms2 = list(gen_raster)

    implants_new_de_names = (
        '105-2.7 Implant',
    )
    implants_new_short_names = (
        'New users - Implants',
    )
    de_implants_new_meta = list(product(implants_new_short_names, (None,)))

    qs_implants_new = DataValue.objects.what(*implants_new_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_implants_new = qs_implants_new.where(filter_district)
    qs_implants_new = qs_implants_new.annotate(de_name=Value(implants_new_short_names[0], output_field=CharField()))
    qs_implants_new = qs_implants_new.filter(category_combo__categories__name='New Users')
    qs_implants_new = qs_implants_new.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_implants_new = qs_implants_new.annotate(district=F('org_unit__parent__parent__name'))
    qs_implants_new = qs_implants_new.annotate(period=F('quarter'))
    qs_implants_new = qs_implants_new.order_by('district', 'de_name', 'cat_combo', 'period')
    val_implants_new = qs_implants_new.values('district', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_implants_new = list(val_implants_new)

    gen_raster = grabbag.rasterize(ou_list, de_implants_new_meta, val_implants_new, get_ou_path, lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_implants_new2 = list(gen_raster)

    injectable_de_names = (
        '105-2.5 Injectable',
    )
    injectable_short_names = (
        'Injectable dispensed (doses)',
    )
    de_injectable_meta = list(product(injectable_short_names, (None,)))

    qs_injectable = DataValue.objects.what(*injectable_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_injectable = qs_injectable.where(filter_district)
    qs_injectable = qs_injectable.annotate(de_name=Value(injectable_short_names[0], output_field=CharField()))
    qs_injectable = qs_injectable.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_injectable = qs_injectable.annotate(district=F('org_unit__parent__parent__name'))
    qs_injectable = qs_injectable.annotate(period=F('quarter'))
    qs_injectable = qs_injectable.order_by('district', 'de_name', 'cat_combo', 'period')
    val_injectable = qs_injectable.values('district', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_injectable = list(val_injectable)

    gen_raster = grabbag.rasterize(ou_list, de_injectable_meta, val_injectable, get_ou_path, lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_injectable2 = list(gen_raster)

    iud_de_names = (
        '105-2.5 IUDs',
    )
    iud_short_names = (
        'IUD inserted',
    )
    de_iud_meta = list(product(iud_short_names, (None,)))

    qs_iud = DataValue.objects.what(*iud_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_iud = qs_iud.where(filter_district)
    qs_iud = qs_iud.annotate(de_name=Value(iud_short_names[0], output_field=CharField()))
    qs_iud = qs_iud.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_iud = qs_iud.annotate(district=F('org_unit__parent__parent__name'))
    qs_iud = qs_iud.annotate(period=F('quarter'))
    qs_iud = qs_iud.order_by('district', 'de_name', 'cat_combo', 'period')
    val_iud = qs_iud.values('district', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_iud = list(val_iud)

    gen_raster = grabbag.rasterize(ou_list, de_iud_meta, val_iud, get_ou_path, lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_iud2 = list(gen_raster)

    sterile_new_de_names = (
        '105-2.7 Female Sterilisation (TubeLigation)',
        '105-2.7 Male Sterilisation (Vasectomy)',
    )
    sterile_new_short_names = (
        'New users - Sterilisation (male and female)',
    )
    de_sterile_new_meta = list(product(sterile_new_short_names, (None,)))

    qs_sterile_new = DataValue.objects.what(*sterile_new_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_sterile_new = qs_sterile_new.where(filter_district)
    qs_sterile_new = qs_sterile_new.annotate(de_name=Value(sterile_new_short_names[0], output_field=CharField()))
    qs_sterile_new = qs_sterile_new.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_sterile_new = qs_sterile_new.annotate(district=F('org_unit__parent__parent__name'))
    qs_sterile_new = qs_sterile_new.annotate(period=F('quarter'))
    qs_sterile_new = qs_sterile_new.order_by('district', 'de_name', 'cat_combo', 'period')
    val_sterile_new = qs_sterile_new.values('district', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_sterile_new = list(val_sterile_new)

    gen_raster = grabbag.rasterize(ou_list, de_sterile_new_meta, val_sterile_new, get_ou_path, lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_sterile_new2 = list(gen_raster)

    natural_de_names = (
        '105-2.5 Natural',
    )
    natural_short_names = (
        'Natural methods',
    )
    de_natural_meta = list(product(natural_short_names, (None,)))

    qs_natural = DataValue.objects.what(*natural_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_natural = qs_natural.where(filter_district)
    qs_natural = qs_natural.annotate(de_name=Value(natural_short_names[0], output_field=CharField()))
    qs_natural = qs_natural.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_natural = qs_natural.annotate(district=F('org_unit__parent__parent__name'))
    qs_natural = qs_natural.annotate(period=F('quarter'))
    qs_natural = qs_natural.order_by('district', 'de_name', 'cat_combo', 'period')
    val_natural = qs_natural.values('district', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_natural = list(val_natural)

    gen_raster = grabbag.rasterize(ou_list, de_natural_meta, val_natural, get_ou_path, lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_natural2 = list(gen_raster)

    emergency_de_names = (
        '105-2.6 Emergency contraceptives  No. Dispensed by CBD',
        '105-2.6 Emergency contraceptives  No. Dispensed at Unit',
        '105-2.6 Emergency contraceptives  No. Disp. At Outreach',
    )
    emergency_short_names = (
        'Emergency contraceptives dispensed (doses)',
    )
    de_emergency_meta = list(product(emergency_short_names, (None,)))

    qs_emergency = DataValue.objects.what(*emergency_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_emergency = qs_emergency.where(filter_district)
    qs_emergency = qs_emergency.annotate(de_name=Value(emergency_short_names[0], output_field=CharField()))
    qs_emergency = qs_emergency.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_emergency = qs_emergency.annotate(district=F('org_unit__parent__parent__name'))
    qs_emergency = qs_emergency.annotate(period=F('quarter'))
    qs_emergency = qs_emergency.order_by('district', 'de_name', 'cat_combo', 'period')
    val_emergency = qs_emergency.values('district', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_emergency = list(val_emergency)

    gen_raster = grabbag.rasterize(ou_list, de_emergency_meta, val_emergency, get_ou_path, lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_emergency2 = list(gen_raster)

    # combine the data and group by district, subcounty and facility
    grouped_vals = groupbylist(sorted(chain(val_oral2, val_condoms2, val_implants_new2, val_injectable2, val_iud2, val_sterile_new2, val_natural2, val_emergency2), key=lambda x: (x['district'],)), key=lambda x: (x['district'],))
    if True:
        grouped_vals = list(filter_empty_rows(grouped_vals))

    # perform calculations
    for _group in grouped_vals:
        (ou_path, (oral, condoms, implants_new, injectable, iud, sterile_new, natural, emergency, *other_vals)) = _group
        
        calculated_vals = list()

        if all_not_none(oral['numeric_sum']):
            cyp_oral = oral['numeric_sum'] / 15
        else:
            cyp_oral = None
        cyp_oral_val = {
            'district': ou_path[0],
            'de_name': 'CYPs Oral',
            'cat_combo': None,
            'numeric_sum': cyp_oral,
        }
        calculated_vals.append(cyp_oral_val)

        if all_not_none(condoms['numeric_sum']):
            cyp_condoms = condoms['numeric_sum'] / 120
        else:
            cyp_condoms = None
        cyp_condoms_val = {
            'district': ou_path[0],
            'de_name': 'CYPs Condoms',
            'cat_combo': None,
            'numeric_sum': cyp_condoms,
        }
        calculated_vals.append(cyp_condoms_val)

        if all_not_none(implants_new['numeric_sum']):
            cyp_implants = implants_new['numeric_sum'] * Decimal(2.5)
        else:
            cyp_implants = None
        cyp_implants_val = {
            'district': ou_path[0],
            'de_name': 'CYPs Implants',
            'cat_combo': None,
            'numeric_sum': cyp_implants,
        }
        calculated_vals.append(cyp_implants_val)

        if all_not_none(injectable['numeric_sum']):
            cyp_injectable = injectable['numeric_sum'] / 4
        else:
            cyp_injectable = None
        cyp_injectable_val = {
            'district': ou_path[0],
            'de_name': 'CYPs Injectable',
            'cat_combo': None,
            'numeric_sum': cyp_injectable,
        }
        calculated_vals.append(cyp_injectable_val)

        if all_not_none(iud['numeric_sum']):
            cyp_iud = iud['numeric_sum'] * Decimal(4.6)
        else:
            cyp_iud = None
        cyp_iud_val = {
            'district': ou_path[0],
            'de_name': 'CYPs IUD',
            'cat_combo': None,
            'numeric_sum': cyp_iud,
        }
        calculated_vals.append(cyp_iud_val)

        if all_not_none(sterile_new['numeric_sum']):
            cyp_sterile = sterile_new['numeric_sum'] * Decimal(10)
        else:
            cyp_sterile = None
        cyp_sterile_val = {
            'district': ou_path[0],
            'de_name': 'CYPs sterile',
            'cat_combo': None,
            'numeric_sum': cyp_sterile,
        }
        calculated_vals.append(cyp_sterile_val)

        if all_not_none(natural['numeric_sum']):
            cyp_natural = natural['numeric_sum'] / 4
        else:
            cyp_natural = None
        cyp_natural_val = {
            'district': ou_path[0],
            'de_name': 'CYPs Natural Methods',
            'cat_combo': None,
            'numeric_sum': cyp_natural,
        }
        calculated_vals.append(cyp_natural_val)

        if all_not_none(emergency['numeric_sum']):
            cyp_emergency = emergency['numeric_sum'] / 20
        else:
            cyp_emergency = None
        cyp_emergency_val = {
            'district': ou_path[0],
            'de_name': 'CYPs Emergency Contraceptives',
            'cat_combo': None,
            'numeric_sum': cyp_emergency,
        }
        calculated_vals.append(cyp_emergency_val)

        _group[1].extend(calculated_vals)

    data_element_names = list()
    data_element_names += list(product(oral_short_names, (None,)))
    data_element_names += list(product(condoms_short_names, (None,)))
    data_element_names += list(product(implants_new_short_names, (None,)))
    data_element_names += list(product(injectable_short_names, (None,)))
    data_element_names += list(product(iud_short_names, (None,)))
    data_element_names += list(product(sterile_new_short_names, (None,)))
    data_element_names += list(product(natural_short_names, (None,)))
    data_element_names += list(product(emergency_short_names, (None,)))

    data_element_names += list(product(['CYPs Oral'], (None,)))
    data_element_names += list(product(['CYPs Condoms'], (None,)))
    data_element_names += list(product(['CYPs Implants'], (None,)))
    data_element_names += list(product(['CYPs Injectable'], (None,)))
    data_element_names += list(product(['CYPs IUD'], (None,)))
    data_element_names += list(product(['CYP Sterilisation'], (None,)))
    data_element_names += list(product(['CYPs Natural Methods'], (None,)))
    data_element_names += list(product(['CYPs Emergency contraceptives'], (None,)))

    legend_sets = list()
    # fp_cyp_ls = LegendSet()
    # fp_cyp_ls.name = 'FP CYP'
    # fp_cyp_ls.add_interval('orange', 0, 25)
    # fp_cyp_ls.add_interval('yellow', 25, 40)
    # fp_cyp_ls.add_interval('light-green', 50, 60)
    # fp_cyp_ls.add_interval('green', 60, None)
    # legend_sets.append(fp_cyp_ls)

    if output_format == 'EXCEL':
        from django.http import HttpResponse
        import openpyxl

        wb = openpyxl.workbook.Workbook()
        ws = wb.active # workbooks are created with at least one worksheet
        ws.title = 'Sheet1' # unfortunately it is named "Sheet" not "Sheet1"
        ws.page_setup.orientation = ws.ORIENTATION_LANDSCAPE
        ws.page_setup.paperSize = ws.PAPERSIZE_A4

        headers = ou_headers + data_element_names
        for i, name in enumerate(headers, start=1):
            c = ws.cell(row=1, column=i)
            if not isinstance(name, tuple):
                c.value = str(name)
            else:
                de, cat_combo = name
                if cat_combo is None:
                    c.value = str(de)
                else:
                    c.value = str(de) + '\n' + str(cat_combo)
        for i, g in enumerate(grouped_vals, start=2):
            ou_path, g_val_list = g
            for col_idx, ou in enumerate(ou_path, start=1):
                ws.cell(row=i, column=col_idx, value=ou)
            for j, g_val in enumerate(g_val_list, start=len(ou_path)+1):
                ws.cell(row=i, column=j, value=g_val['numeric_sum'])

        for ls in legend_sets:
            # apply conditional formatting from LegendSets
            for rule in ls.openpyxl_rules():
                for cell_range in ls.excel_ranges():
                    print(cell_range, ls)
                    ws.conditional_formatting.add(cell_range, rule)


        response = HttpResponse(openpyxl.writer.excel.save_virtual_workbook(wb), content_type='application/vnd.ms-excel')
        response['Content-Disposition'] = 'attachment; filename="fp_cyp_districts_scorecard.xlsx"'

        return response

    context = {
        'grouped_data': grouped_vals,
        'ou_headers': ou_headers,
        'data_element_names': data_element_names,
        'legend_sets': legend_sets,
        'period_desc': period_desc,
        'period_list': PREV_5YR_QTRS,
        'district_list': DISTRICT_LIST,
        'excel_url': make_excel_url(request.path)
    }

    return render(request, 'cannula/fp_cyp_districts.html', context)

@login_required
def tb_by_site(request, output_format='HTML'):
    this_day = date.today()
    this_year = this_day.year
    PREV_5YR_QTRS = ['%d-Q%d' % (y, q) for y in range(this_year, this_year-6, -1) for q in range(4, 0, -1)]
    DISTRICT_LIST = list(OrgUnit.objects.filter(level=1).order_by('name').values_list('name', flat=True))

    if 'period' in request.GET and request.GET['period'] in PREV_5YR_QTRS:
        filter_period=request.GET['period']
    else:
        filter_period = '%d-Q%d' % (this_year, month2quarter(this_day.month))

    period_desc = dateutil.DateSpan.fromquarter(filter_period).format()

    if 'district' in request.GET and request.GET['district'] in DISTRICT_LIST:
        filter_district = OrgUnit.objects.get(name=request.GET['district'])
    else:
        filter_district = None

    # # all facilities (or equivalent)
    qs_ou = OrgUnit.objects.filter(level=3).annotate(district=F('parent__parent__name'), subcounty=F('parent__name'), facility=F('name'))
    if filter_district:
        qs_ou = qs_ou.filter(Q(lft__gte=filter_district.lft) & Q(rght__lte=filter_district.rght))
    ou_list = list(qs_ou.values_list('district', 'subcounty', 'facility'))
    ou_headers = ['District', 'Subcounty', 'Facility']

    def val_with_subcat_fun(row, col):
        district, subcounty, facility = row
        de_name, subcategory = col
        return { 'district': district, 'subcounty': subcounty, 'facility': facility, 'cat_combo': subcategory, 'de_name': de_name, 'numeric_sum': None }

    targets_de_names = (
        'TB_STAT (D, DSD) TARGET: New/Relapsed TB default',
    )
    targets_short_names = (
        'TARGET: New/Relapsed TB default',
    )
    de_targets_meta = list(product(targets_short_names, (None,)))

    qs_targets = DataValue.objects.what(*targets_de_names)
    if filter_district:
        qs_targets = qs_targets.where(filter_district)
    qs_targets = qs_targets.when(filter_period)
    qs_targets = qs_targets.annotate(de_name=Value(targets_short_names[0], output_field=CharField()))
    qs_targets = qs_targets.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_targets = qs_targets.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_targets = qs_targets.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_targets = qs_targets.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_targets = list(val_targets)

    gen_raster = grabbag.rasterize(ou_list, de_targets_meta, val_targets, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_targets2 = list(gen_raster)

    notif_new_de_names = (
        '106a 3.1.a.1 Bacteriologically confirmed, PTB (P-BC) [Cases] New',
        '106a 3.1.a.1 Bacteriologically confirmed, PTB (P-BC) [Cases] Relapse',
        '106a 3.1.a.2 Clinically diagnosed PTB, (P-CD) [Cases] New',
        '106a 3.1.a.2 Clinically diagnosed PTB, (P-CD) [Cases] Relapse',
        '106a 3.1.a.3 EPTB, (bacteriologically or clinically diagnosed) [Cases] New',
        '106a 3.1.a.3 EPTB, (bacteriologically or clinically diagnosed) [Cases] Relapse',
    )
    notif_new_short_names = (
        'Notification (New and Relapse)',
    )
    de_notif_new_meta = list(product(notif_new_short_names, (None,)))

    qs_notif_new = DataValue.objects.what(*notif_new_de_names)
    if filter_district:
        qs_notif_new = qs_notif_new.where(filter_district)
    qs_notif_new = qs_notif_new.when(filter_period)
    qs_notif_new = qs_notif_new.annotate(de_name=Value(notif_new_short_names[0], output_field=CharField()))
    qs_notif_new = qs_notif_new.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_notif_new = qs_notif_new.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_notif_new = qs_notif_new.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_notif_new = qs_notif_new.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_notif_new = list(val_notif_new)

    gen_raster = grabbag.rasterize(ou_list, de_notif_new_meta, val_notif_new, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_notif_new2 = list(gen_raster)

    notif_all_de_names = (
        '106a 3.1.a.1 Bacteriologically confirmed, PTB (P-BC) [Cases] New',
        '106a 3.1.a.1 Bacteriologically confirmed, PTB (P-BC) [Cases] Relapse',
        '106a 3.1.a.1 Bacteriologically confirmed, PTB (P-BC) [Cases] Lost to Followup',
        '106a 3.1.a.1 Bacteriologically confirmed, PTB (P-BC) [Cases] Failure',
        '106a 3.1.a.1 Bacteriologically confirmed, PTB (P-BC) [Cases] Trt History Unknown',
        '106a 3.1.a.2 Clinically diagnosed PTB, (P-CD) [Cases] New',
        '106a 3.1.a.2 Clinically diagnosed PTB, (P-CD) [Cases] Relapse',
        '106a 3.1.a.2 Clinically diagnosed PTB, (P-CD) [Cases] Lost to Followup',
        '106a 3.1.a.2 Clinically diagnosed PTB, (P-CD) [Cases] Failure',
        '106a 3.1.a.2 Clinically diagnosed PTB, (P-CD) [Cases] Trt History Unknown',
        '106a 3.1.a.3 EPTB, (bacteriologically or clinically diagnosed) [Cases] New',
        '106a 3.1.a.3 EPTB, (bacteriologically or clinically diagnosed) [Cases] Relapse',
        '106a 3.1.a.3 EPTB, (bacteriologically or clinically diagnosed) [Cases] Lost to Followup',
        '106a 3.1.a.3 EPTB, (bacteriologically or clinically diagnosed) [Cases] Failure',
        '106a 3.1.a.3 EPTB, (bacteriologically or clinically diagnosed) [Cases] Trt History Unknown',
    )
    notif_all_short_names = (
        'Notification (All cases)',
    )
    de_notif_all_meta = list(product(notif_all_short_names, (None,)))

    qs_notif_all = DataValue.objects.what(*notif_all_de_names)
    if filter_district:
        qs_notif_all = qs_notif_all.where(filter_district)
    qs_notif_all = qs_notif_all.when(filter_period)
    qs_notif_all = qs_notif_all.annotate(de_name=Value(notif_all_short_names[0], output_field=CharField()))
    qs_notif_all = qs_notif_all.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_notif_all = qs_notif_all.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_notif_all = qs_notif_all.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_notif_all = qs_notif_all.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_notif_all = list(val_notif_all)

    gen_raster = grabbag.rasterize(ou_list, de_notif_all_meta, val_notif_all, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_notif_all2 = list(gen_raster)

    hiv_tested_de_names = (
        '106a 3.1.c.1 New HIV/TB Patients Registered, PTB (P-BC) Tested for HIV',
        '106a 3.1.c.2 New HIV/TB Patients Registered, Clinically diagnosed PTB (P-CD) Tested for HIV',
        '106a 3.1.c.3 New HIV/TB Patients Registered, EPTB (BC or CD) Tested for HIV',
        '106a 3.1.c.4 New HIV/TB Patients Registered, Other types of TB Tested for HIV',
    )
    hiv_tested_short_names = (
        'Tested for HIV',
    )
    de_hiv_tested_meta = list(product(hiv_tested_short_names, (None,)))

    qs_hiv_tested = DataValue.objects.what(*hiv_tested_de_names)
    if filter_district:
        qs_hiv_tested = qs_hiv_tested.where(filter_district)
    qs_hiv_tested = qs_hiv_tested.when(filter_period)
    qs_hiv_tested = qs_hiv_tested.annotate(de_name=Value(hiv_tested_short_names[0], output_field=CharField()))
    qs_hiv_tested = qs_hiv_tested.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_hiv_tested = qs_hiv_tested.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_hiv_tested = qs_hiv_tested.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_hiv_tested = qs_hiv_tested.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_hiv_tested = list(val_hiv_tested)

    gen_raster = grabbag.rasterize(ou_list, de_hiv_tested_meta, val_hiv_tested, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_hiv_tested2 = list(gen_raster)

    hiv_pos_de_names = (
        '106a 3.1.c.1 New HIV/TB Patients Registered, PTB (P-BC) HIV Positive',
        '106a 3.1.c.2 New HIV/TB Patients Registered, Clinically diagnosed PTB (P-CD) HIV Positive',
        '106a 3.1.c.3 New HIV/TB Patients Registered, EPTB (BC or CD) HIV Positive',
        '106a 3.1.c.4 New HIV/TB Patients Registered, Other types of TB HIV Positive',
    )
    hiv_pos_short_names = (
        'Tested HIV+',
    )
    de_hiv_pos_meta = list(product(hiv_pos_short_names, (None,)))

    qs_hiv_pos = DataValue.objects.what(*hiv_pos_de_names)
    if filter_district:
        qs_hiv_pos = qs_hiv_pos.where(filter_district)
    qs_hiv_pos = qs_hiv_pos.when(filter_period)
    qs_hiv_pos = qs_hiv_pos.annotate(de_name=Value(hiv_pos_short_names[0], output_field=CharField()))
    qs_hiv_pos = qs_hiv_pos.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_hiv_pos = qs_hiv_pos.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_hiv_pos = qs_hiv_pos.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_hiv_pos = qs_hiv_pos.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_hiv_pos = list(val_hiv_pos)

    gen_raster = grabbag.rasterize(ou_list, de_hiv_pos_meta, val_hiv_pos, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_hiv_pos2 = list(gen_raster)

    hiv_art_de_names = (
        '106a 3.1.c.1 New HIV/TB Patients Registered, PTB (P-BC) On ART',
        '106a 3.1.c.2 New HIV/TB Patients Registered, Clinically diagnosed PTB (P-CD) On ART',
        '106a 3.1.c.3 New HIV/TB Patients Registered, EPTB (BC or CD) On ART',
        '106a 3.1.c.4 New HIV/TB Patients Registered, Other types of TB On ART',
    )
    hiv_art_short_names = (
        'HIV+ on ART',
    )
    de_hiv_art_meta = list(product(hiv_art_short_names, (None,)))

    qs_hiv_art = DataValue.objects.what(*hiv_art_de_names)
    if filter_district:
        qs_hiv_art = qs_hiv_art.where(filter_district)
    qs_hiv_art = qs_hiv_art.when(filter_period)
    qs_hiv_art = qs_hiv_art.annotate(de_name=Value(hiv_art_short_names[0], output_field=CharField()))
    qs_hiv_art = qs_hiv_art.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_hiv_art = qs_hiv_art.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_hiv_art = qs_hiv_art.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_hiv_art = qs_hiv_art.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_hiv_art = list(val_hiv_art)

    gen_raster = grabbag.rasterize(ou_list, de_hiv_art_meta, val_hiv_art, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_hiv_art2 = list(gen_raster)

    registered_de_names = (
        '106a 3.1.h.1 TB Treat. Outcome (All): New Patients Category I (PTB-BC)',
    )
    registered_short_names = (
        'Number registered',
    )
    de_registered_meta = list(product(registered_short_names, (None,)))

    qs_registered = DataValue.objects.what(*registered_de_names)
    if filter_district:
        qs_registered = qs_registered.where(filter_district)
    qs_registered = qs_registered.when(filter_period)
    qs_registered = qs_registered.annotate(de_name=Value(registered_short_names[0], output_field=CharField()))
    qs_registered = qs_registered.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_registered = qs_registered.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_registered = qs_registered.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_registered = qs_registered.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_registered = list(val_registered)

    gen_raster = grabbag.rasterize(ou_list, de_registered_meta, val_registered, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_registered2 = list(gen_raster)

    evaluated_de_names = (
        '106a 3.1.h.1 TB Treat. Outcome (All): New Patients Category I (PTB-BC) Cured',
        '106a 3.1.h.1 TB Treat. Outcome (All): New Patients Category I (PTB-BC) Trt Completed',
        '106a 3.1.h.1 TB Treat. Outcome (All): New Patients Category I (PTB-BC) Died',
        '106a 3.1.h.1 TB Treat. Outcome (All): New Patients Category I (PTB-BC) Failure',
        '106a 3.1.h.1 TB Treat. Outcome (All): New Patients Category I (PTB-BC) Lost to Followup',
    )
    evaluated_short_names = (
        'Number evaluated',
    )
    de_evaluated_meta = list(product(evaluated_short_names, (None,)))

    qs_evaluated = DataValue.objects.what(*evaluated_de_names)
    if filter_district:
        qs_evaluated = qs_evaluated.where(filter_district)
    qs_evaluated = qs_evaluated.when(filter_period)
    qs_evaluated = qs_evaluated.annotate(de_name=Value(evaluated_short_names[0], output_field=CharField()))
    qs_evaluated = qs_evaluated.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_evaluated = qs_evaluated.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_evaluated = qs_evaluated.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_evaluated = qs_evaluated.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_evaluated = list(val_evaluated)

    gen_raster = grabbag.rasterize(ou_list, de_evaluated_meta, val_evaluated, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_evaluated2 = list(gen_raster)

    cured_completed_de_names = (
        '106a 3.1.h.1 TB Treat. Outcome (All): New Patients Category I (PTB-BC) Cured',
        '106a 3.1.h.1 TB Treat. Outcome (All): New Patients Category I (PTB-BC) Trt Completed',
    )
    cured_completed_short_names = (
        'Number cured or completed',
    )
    de_cured_completed_meta = list(product(cured_completed_short_names, (None,)))

    qs_cured_completed = DataValue.objects.what(*cured_completed_de_names)
    if filter_district:
        qs_cured_completed = qs_cured_completed.where(filter_district)
    qs_cured_completed = qs_cured_completed.when(filter_period)
    qs_cured_completed = qs_cured_completed.annotate(de_name=Value(cured_completed_short_names[0], output_field=CharField()))
    qs_cured_completed = qs_cured_completed.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_cured_completed = qs_cured_completed.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_cured_completed = qs_cured_completed.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_cured_completed = qs_cured_completed.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_cured_completed = list(val_cured_completed)

    gen_raster = grabbag.rasterize(ou_list, de_cured_completed_meta, val_cured_completed, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_cured_completed2 = list(gen_raster)

    cured_de_names = (
        '106a 3.1.h.1 TB Treat. Outcome (All): New Patients Category I (PTB-BC) Cured',
    )
    cured_short_names = (
        'Number Cured',
    )
    de_cured_meta = list(product(cured_short_names, (None,)))

    qs_cured = DataValue.objects.what(*cured_de_names)
    if filter_district:
        qs_cured = qs_cured.where(filter_district)
    qs_cured = qs_cured.when(filter_period)
    qs_cured = qs_cured.annotate(de_name=Value(cured_short_names[0], output_field=CharField()))
    qs_cured = qs_cured.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_cured = qs_cured.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_cured = qs_cured.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_cured = qs_cured.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_cured = list(val_cured)

    gen_raster = grabbag.rasterize(ou_list, de_cured_meta, val_cured, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_cured2 = list(gen_raster)

    ltfu_de_names = (
        '106a 3.1.h.1 TB Treat. Outcome (All): New Patients Category I (PTB-BC) Lost to Followup',
    )
    ltfu_short_names = (
        'LTFU',
    )
    de_ltfu_meta = list(product(ltfu_short_names, (None,)))

    qs_ltfu = DataValue.objects.what(*ltfu_de_names)
    if filter_district:
        qs_ltfu = qs_ltfu.where(filter_district)
    qs_ltfu = qs_ltfu.when(filter_period)
    qs_ltfu = qs_ltfu.annotate(de_name=Value(ltfu_short_names[0], output_field=CharField()))
    qs_ltfu = qs_ltfu.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_ltfu = qs_ltfu.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_ltfu = qs_ltfu.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_ltfu = qs_ltfu.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_ltfu = list(val_ltfu)

    gen_raster = grabbag.rasterize(ou_list, de_ltfu_meta, val_ltfu, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_ltfu2 = list(gen_raster)

    notif_under15_de_names = (
        '106a 3.1.b.1 Bacteriologically confirmed, PTB (P-BC) New and Relapse [Age Groups]',
        '106a 3.1.b.2 Clinically diagnosed PTB (P-CD) [Age Groups]',
        '106a 3.1.b.3 EPTB, (bacteriologically or clinically diagnosed) [Age Groups]',
    )
    notif_under15_short_names = (
        '<15 Years Notified',
    )
    de_notif_under15_meta = list(product(notif_under15_short_names, (None,)))

    qs_notif_under15 = DataValue.objects.what(*notif_under15_de_names)
    if filter_district:
        qs_notif_under15 = qs_notif_under15.where(filter_district)
    qs_notif_under15 = qs_notif_under15.when(filter_period)
    qs_notif_under15 = qs_notif_under15.annotate(de_name=Value(notif_under15_short_names[0], output_field=CharField()))
    qs_notif_under15 = qs_notif_under15.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_notif_under15 = qs_notif_under15.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_notif_under15 = qs_notif_under15.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_notif_under15 = qs_notif_under15.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_notif_under15 = list(val_notif_under15)

    gen_raster = grabbag.rasterize(ou_list, de_notif_under15_meta, val_notif_under15, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_notif_under152 = list(gen_raster)

    failed_de_names = (
        '106a 3.1.h.1 TB Treat. Outcome (All): New Patients Category I (PTB-BC) Failure',
    )
    failed_short_names = (
        'Number failed',
    )
    de_failed_meta = list(product(failed_short_names, (None,)))

    qs_failed = DataValue.objects.what(*failed_de_names)
    if filter_district:
        qs_failed = qs_failed.where(filter_district)
    qs_failed = qs_failed.when(filter_period)
    qs_failed = qs_failed.annotate(de_name=Value(failed_short_names[0], output_field=CharField()))
    qs_failed = qs_failed.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_failed = qs_failed.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_failed = qs_failed.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_failed = qs_failed.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_failed = list(val_failed)

    gen_raster = grabbag.rasterize(ou_list, de_failed_meta, val_failed, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_failed2 = list(gen_raster)

    died_de_names = (
        '106a 3.1.h.1 TB Treat. Outcome (All): New Patients Category I (PTB-BC) Died',
    )
    died_short_names = (
        'Number died',
    )
    de_died_meta = list(product(died_short_names, (None,)))

    qs_died = DataValue.objects.what(*died_de_names)
    if filter_district:
        qs_died = qs_died.where(filter_district)
    qs_died = qs_died.when(filter_period)
    qs_died = qs_died.annotate(de_name=Value(died_short_names[0], output_field=CharField()))
    qs_died = qs_died.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_died = qs_died.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_died = qs_died.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_died = qs_died.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_died = list(val_died)

    gen_raster = grabbag.rasterize(ou_list, de_died_meta, val_died, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_died2 = list(gen_raster)

    # combine the data and group by district, subcounty and facility
    grouped_vals = groupbylist(sorted(chain(val_targets2, val_notif_new2, val_notif_all2, val_hiv_tested2, val_hiv_pos2, val_hiv_art2, val_registered2, val_evaluated2, val_cured_completed2, val_cured2, val_ltfu2, val_notif_under152, val_failed2, val_died2), key=lambda x: (x['district'], x['subcounty'], x['facility'])), key=lambda x: (x['district'], x['subcounty'], x['facility']))
    if True:
        grouped_vals = list(filter_empty_rows(grouped_vals))

    # perform calculations
    for _group in grouped_vals:
        (district_subcounty_facility, (target_notif_new, notif_new, notif_all, hiv_tested, hiv_pos, hiv_art, registered, evaluated, cured_completed, cured, ltfu, notif_under15, failed, died, *other_vals)) = _group
        
        calculated_vals = list()

        if all_not_none(cured_completed['numeric_sum'], evaluated['numeric_sum']) and evaluated['numeric_sum']:
            tsr_percent = 100 * cured_completed['numeric_sum'] / evaluated['numeric_sum']
        else:
            tsr_percent = None
        tsr_percent_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': '% TSR',
            'cat_combo': None,
            'numeric_sum': tsr_percent,
        }
        calculated_vals.append(tsr_percent_val)

        if all_not_none(ltfu['numeric_sum'], evaluated['numeric_sum']) and evaluated['numeric_sum']:
            ltfu_percent = 100 * ltfu['numeric_sum'] / evaluated['numeric_sum']
        else:
            ltfu_percent = None
        ltfu_percent_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': '% LTFU',
            'cat_combo': None,
            'numeric_sum': ltfu_percent,
        }
        calculated_vals.append(ltfu_percent_val)

        if all_not_none(notif_new['numeric_sum'], target_notif_new['numeric_sum']) and target_notif_new['numeric_sum']:
            notif_new_percent = 100 * notif_new['numeric_sum'] / target_notif_new['numeric_sum']
        else:
            notif_new_percent = None
        notif_new_percent_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': '% of cases notified (NEW & Relapse)',
            'cat_combo': None,
            'numeric_sum': notif_new_percent,
        }
        calculated_vals.append(notif_new_percent_val)

        if all_not_none(hiv_tested['numeric_sum'], notif_all['numeric_sum']) and notif_all['numeric_sum']:
            hiv_tested_percent = 100 * hiv_tested['numeric_sum'] / notif_all['numeric_sum']
        else:
            hiv_tested_percent = None
        hiv_tested_percent_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': '% Tested for HIV',
            'cat_combo': None,
            'numeric_sum': hiv_tested_percent,
        }
        calculated_vals.append(hiv_tested_percent_val)

        if all_not_none(hiv_art['numeric_sum'], hiv_pos['numeric_sum']) and hiv_pos['numeric_sum']:
            hiv_art_percent = 100 * hiv_art['numeric_sum'] / hiv_pos['numeric_sum']
        else:
            hiv_art_percent = None
        hiv_art_percent_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': '% HIV+ on ART',
            'cat_combo': None,
            'numeric_sum': hiv_art_percent,
        }
        calculated_vals.append(hiv_art_percent_val)

        if all_not_none(cured['numeric_sum'], evaluated['numeric_sum']) and evaluated['numeric_sum']:
            cure_percent = 100 * cured['numeric_sum'] / evaluated['numeric_sum']
        else:
            cure_percent = None
        cure_percent_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': '% Cure Rate',
            'cat_combo': None,
            'numeric_sum': cure_percent,
        }
        calculated_vals.append(cure_percent_val)

        _group[1].extend(calculated_vals)

    data_element_names = list()
    data_element_names += list(product(targets_short_names, (None,)))
    data_element_names += list(product(notif_new_short_names, (None,)))
    data_element_names += list(product(notif_all_short_names, (None,)))
    data_element_names += list(product(hiv_tested_short_names, (None,)))
    data_element_names += list(product(hiv_pos_short_names, (None,)))
    data_element_names += list(product(hiv_art_short_names, (None,)))
    data_element_names += list(product(registered_short_names, (None,)))
    data_element_names += list(product(evaluated_short_names, (None,)))
    data_element_names += list(product(cured_completed_short_names, (None,)))
    data_element_names += list(product(cured_short_names, (None,)))
    data_element_names += list(product(ltfu_short_names, (None,)))
    data_element_names += list(product(notif_under15_short_names, (None,)))
    data_element_names += list(product(failed_short_names, (None,)))
    data_element_names += list(product(died_short_names, (None,)))

    data_element_names += list(product(['% TSR'], (None,)))
    data_element_names += list(product(['% LTFU'], (None,)))
    data_element_names += list(product(['% of cases notified (NEW & Relapse)'], (None,)))
    data_element_names += list(product(['% Tested for HIV'], (None,)))
    data_element_names += list(product(['% HIV+ on ART'], (None,)))
    data_element_names += list(product(['% Cure Rate'], (None,)))

    legend_sets = list()
    notif_ls = LegendSet()
    notif_ls.name = 'Notification, Testing and ART'
    notif_ls.add_interval('red', 0, 75)
    notif_ls.add_interval('yellow', 75, 95)
    notif_ls.add_interval('green', 95, None)
    notif_ls.mappings[19] = True
    notif_ls.mappings[20] = True
    notif_ls.mappings[21] = True
    legend_sets.append(notif_ls)
    cure_ls = LegendSet()
    cure_ls.name = 'Cure Rate'
    cure_ls.add_interval('red', 0, 50)
    cure_ls.add_interval('yellow', 50, 60)
    cure_ls.add_interval('green', 60, None)
    cure_ls.mappings[22] = True
    legend_sets.append(cure_ls)
    tsr_ls = LegendSet()
    tsr_ls.name = 'TSR'
    tsr_ls.add_interval('red', 0, 80)
    tsr_ls.add_interval('yellow', 80, 85)
    tsr_ls.add_interval('green', 85, None)
    tsr_ls.mappings[17] = True
    legend_sets.append(tsr_ls)
    cnr_ls = LegendSet()
    cnr_ls.name = 'CNR'
    cnr_ls.add_interval('red', 0, 85)
    cnr_ls.add_interval('yellow', 85, 115)
    cnr_ls.add_interval('green', 115, None)
    legend_sets.append(cnr_ls)

    if output_format == 'EXCEL':
        from django.http import HttpResponse
        import openpyxl

        wb = openpyxl.workbook.Workbook()
        ws = wb.active # workbooks are created with at least one worksheet
        ws.title = 'Sheet1' # unfortunately it is named "Sheet" not "Sheet1"
        ws.page_setup.orientation = ws.ORIENTATION_LANDSCAPE
        ws.page_setup.paperSize = ws.PAPERSIZE_A4

        headers = ou_headers + data_element_names
        for i, name in enumerate(headers, start=1):
            c = ws.cell(row=1, column=i)
            if not isinstance(name, tuple):
                c.value = str(name)
            else:
                de, cat_combo = name
                if cat_combo is None:
                    c.value = str(de)
                else:
                    c.value = str(de) + '\n' + str(cat_combo)
        for i, g in enumerate(grouped_vals, start=2):
            ou_path, g_val_list = g
            for col_idx, ou in enumerate(ou_path, start=1):
                ws.cell(row=i, column=col_idx, value=ou)
            for j, g_val in enumerate(g_val_list, start=len(ou_path)+1):
                ws.cell(row=i, column=j, value=g_val['numeric_sum'])

        for ls in legend_sets:
            # apply conditional formatting from LegendSets
            for rule in ls.openpyxl_rules():
                for cell_range in ls.excel_ranges():
                    print(cell_range, ls)
                    ws.conditional_formatting.add(cell_range, rule)


        response = HttpResponse(openpyxl.writer.excel.save_virtual_workbook(wb), content_type='application/vnd.ms-excel')
        response['Content-Disposition'] = 'attachment; filename="tb_sites_scorecard.xlsx"'

        return response

    context = {
        'grouped_data': grouped_vals,
        'ou_headers': ou_headers,
        'data_element_names': data_element_names,
        'legend_sets': legend_sets,
        'period_desc': period_desc,
        'period_list': PREV_5YR_QTRS,
        'district_list': DISTRICT_LIST,
        'excel_url': make_excel_url(request.path)
    }

    return render(request, 'cannula/tb_sites.html', context)

@login_required
def nutrition_by_hospital(request, output_format='HTML'):
    this_day = date.today()
    this_year = this_day.year
    PREV_5YR_QTRS = ['%d-Q%d' % (y, q) for y in range(this_year, this_year-6, -1) for q in range(4, 0, -1)]
    DISTRICT_LIST = list(OrgUnit.objects.filter(level=1).order_by('name').values_list('name', flat=True))

    if 'period' in request.GET and request.GET['period'] in PREV_5YR_QTRS:
        filter_period=request.GET['period']
    else:
        filter_period = '%d-Q%d' % (this_year, month2quarter(this_day.month))

    period_desc = dateutil.DateSpan.fromquarter(filter_period).format()

    if 'district' in request.GET and request.GET['district'] in DISTRICT_LIST:
        filter_district = OrgUnit.objects.get(name=request.GET['district'])
    else:
        filter_district = None

    # # all facilities (or equivalent)
    qs_ou = OrgUnit.objects.filter(level=3).annotate(district=F('parent__parent__name'), subcounty=F('parent__name'), facility=F('name'))
    if filter_district:
        qs_ou = qs_ou.filter(Q(lft__gte=filter_district.lft) & Q(rght__lte=filter_district.rght))
    ou_list = list(qs_ou.values_list('district', 'subcounty', 'facility'))
    ou_headers = ['District', 'Subcounty', 'Facility']

    def val_with_subcat_fun(row, col):
        district, subcounty, facility = row
        de_name, subcategory = col
        return { 'district': district, 'subcounty': subcounty, 'facility': facility, 'cat_combo': subcategory, 'de_name': de_name, 'numeric_sum': None }
   
    opd_attend_de_names = (
        '105-1.1 OPD New Attendance',
        '105-1.1 OPD Re-Attendance',
    )
    opd_attend_short_names = (
        'Total OPD attendence',
    )
    de_opd_attend_meta = list(product(opd_attend_short_names, (None,)))

    qs_opd_attend = DataValue.objects.what(*opd_attend_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_opd_attend = qs_opd_attend.where(filter_district)
    qs_opd_attend = qs_opd_attend.annotate(de_name=Value(opd_attend_short_names[0], output_field=CharField()))
    qs_opd_attend = qs_opd_attend.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_opd_attend = qs_opd_attend.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_opd_attend = qs_opd_attend.filter(facility__icontains='Hospital')
    qs_opd_attend = qs_opd_attend.annotate(period=F('quarter'))
    qs_opd_attend = qs_opd_attend.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_opd_attend = qs_opd_attend.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))

    gen_raster = grabbag.rasterize(ou_list, de_opd_attend_meta, val_opd_attend, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_opd_attend2 = list(gen_raster)
   
    muac_de_names = (
        '106a Nutri No. 1 of clients who received nutrition assessment in this quarter using color coded MUAC tapes/Z score chart',
    )
    muac_short_names = (
        'Clients assessed using MUAC/Z score in OPD',
    )
    de_muac_meta = list(product(muac_short_names, (None,)))

    qs_muac = DataValue.objects.what(*muac_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_muac = qs_muac.where(filter_district)
    qs_muac = qs_muac.annotate(de_name=Value(muac_short_names[0], output_field=CharField()))
    qs_muac = qs_muac.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_muac = qs_muac.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_muac = qs_muac.filter(facility__icontains='Hospital')
    qs_muac = qs_muac.annotate(period=F('quarter'))
    qs_muac = qs_muac.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_muac = qs_muac.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))

    gen_raster = grabbag.rasterize(ou_list, de_muac_meta, val_muac, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_muac2 = list(gen_raster)
   
    muac_mothers_de_names = (
        '106a Nutri No. 1 of clients who received nutrition assessment in this quarter using color coded MUAC tapes/Z score chart Pregnant/Lactating Women',
    )
    muac_mothers_short_names = (
        'Clients assessed using MUAC/Z score in OPD - Pregnant/Lactating Women',
    )
    de_muac_mothers_meta = list(product(muac_mothers_short_names, (None,)))

    qs_muac_mothers = DataValue.objects.what(*muac_mothers_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_muac_mothers = qs_muac_mothers.where(filter_district)
    qs_muac_mothers = qs_muac_mothers.annotate(de_name=Value(muac_mothers_short_names[0], output_field=CharField()))
    qs_muac_mothers = qs_muac_mothers.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_muac_mothers = qs_muac_mothers.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_muac_mothers = qs_muac_mothers.filter(facility__icontains='Hospital')
    qs_muac_mothers = qs_muac_mothers.annotate(period=F('quarter'))
    qs_muac_mothers = qs_muac_mothers.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_muac_mothers = qs_muac_mothers.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))

    gen_raster = grabbag.rasterize(ou_list, de_muac_mothers_meta, val_muac_mothers, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_muac_mothers2 = list(gen_raster)
   
    mothers_total_de_names = (
        '105-2.1 A3:Total ANC visits (New clients + Re-attendances)',
        '105-2.3 Postnatal Attendances',
    )
    mothers_total_short_names = (
        'Total number of pregnant and lactating mothers',
    )
    de_mothers_total_meta = list(product(mothers_total_short_names, (None,)))

    qs_mothers_total = DataValue.objects.what(*mothers_total_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_mothers_total = qs_mothers_total.where(filter_district)
    qs_mothers_total = qs_mothers_total.annotate(de_name=Value(mothers_total_short_names[0], output_field=CharField()))
    qs_mothers_total = qs_mothers_total.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_mothers_total = qs_mothers_total.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_mothers_total = qs_mothers_total.filter(facility__icontains='Hospital')
    qs_mothers_total = qs_mothers_total.annotate(period=F('quarter'))
    qs_mothers_total = qs_mothers_total.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_mothers_total = qs_mothers_total.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))

    gen_raster = grabbag.rasterize(ou_list, de_mothers_total_meta, val_mothers_total, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_mothers_total2 = list(gen_raster)
   
    i_f_counsel_de_names = (
        '106a Nutri N7-No. of pregnant and lactating women who received infant feeding counseling - Total',
    )
    i_f_counsel_short_names = (
        '106a Nutri N7-No. of pregnant and lactating women who received infant feeding counseling - Total',
    )
    de_i_f_counsel_meta = list(product(i_f_counsel_short_names, (None,)))

    qs_i_f_counsel = DataValue.objects.what(*i_f_counsel_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_i_f_counsel = qs_i_f_counsel.where(filter_district)
    qs_i_f_counsel = qs_i_f_counsel.annotate(de_name=Value(i_f_counsel_short_names[0], output_field=CharField()))
    qs_i_f_counsel = qs_i_f_counsel.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_i_f_counsel = qs_i_f_counsel.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_i_f_counsel = qs_i_f_counsel.filter(facility__icontains='Hospital')
    qs_i_f_counsel = qs_i_f_counsel.annotate(period=F('quarter'))
    qs_i_f_counsel = qs_i_f_counsel.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_i_f_counsel = qs_i_f_counsel.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))

    gen_raster = grabbag.rasterize(ou_list, de_i_f_counsel_meta, val_i_f_counsel, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_i_f_counsel2 = list(gen_raster)
   
    m_n_counsel_de_names = (
        '106a Nutri N6-No. of pregnant and lactating women who received maternal nutrition counseling - Total',
    )
    m_n_counsel_short_names = (
        '106a Nutri N6-No. of pregnant and lactating women who received maternal nutrition counseling - Total',
    )
    de_m_n_counsel_meta = list(product(m_n_counsel_short_names, (None,)))

    qs_m_n_counsel = DataValue.objects.what(*m_n_counsel_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_m_n_counsel = qs_m_n_counsel.where(filter_district)
    qs_m_n_counsel = qs_m_n_counsel.annotate(de_name=Value(m_n_counsel_short_names[0], output_field=CharField()))
    qs_m_n_counsel = qs_m_n_counsel.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_m_n_counsel = qs_m_n_counsel.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_m_n_counsel = qs_m_n_counsel.filter(facility__icontains='Hospital')
    qs_m_n_counsel = qs_m_n_counsel.annotate(period=F('quarter'))
    qs_m_n_counsel = qs_m_n_counsel.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_m_n_counsel = qs_m_n_counsel.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))

    gen_raster = grabbag.rasterize(ou_list, de_m_n_counsel_meta, val_m_n_counsel, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_m_n_counsel2 = list(gen_raster)
   
    active_art_de_names = (
        '106a ART No. active on ART on 1st line ARV regimen',
        '106a ART No. active on ART on 2nd line ARV regimen',
        '106a ART No. active on ART on 3rd line or higher ARV regimen',
    )
    active_art_short_names = (
        'Total No. active on ART in the quarter',
    )
    de_active_art_meta = list(product(active_art_short_names, (None,)))

    qs_active_art = DataValue.objects.what(*active_art_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_active_art = qs_active_art.where(filter_district)
    qs_active_art = qs_active_art.annotate(de_name=Value(active_art_short_names[0], output_field=CharField()))
    qs_active_art = qs_active_art.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_active_art = qs_active_art.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_active_art = qs_active_art.filter(facility__icontains='Hospital')
    qs_active_art = qs_active_art.annotate(period=F('quarter'))
    qs_active_art = qs_active_art.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_active_art = qs_active_art.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))

    gen_raster = grabbag.rasterize(ou_list, de_active_art_meta, val_active_art, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_active_art2 = list(gen_raster)
   
    active_art_malnourish_de_names = (
        '106a ART No. active on ART assessed for Malnutrition at their visit in quarter',
    )
    active_art_malnourish_short_names = (
        '106a ART No. active on ART assessed for Malnutrition at their visit in quarter',
    )
    de_active_art_malnourish_meta = list(product(active_art_malnourish_short_names, (None,)))

    qs_active_art_malnourish = DataValue.objects.what(*active_art_malnourish_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_active_art_malnourish = qs_active_art_malnourish.where(filter_district)
    qs_active_art_malnourish = qs_active_art_malnourish.annotate(de_name=Value(active_art_malnourish_short_names[0], output_field=CharField()))
    qs_active_art_malnourish = qs_active_art_malnourish.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_active_art_malnourish = qs_active_art_malnourish.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_active_art_malnourish = qs_active_art_malnourish.filter(facility__icontains='Hospital')
    qs_active_art_malnourish = qs_active_art_malnourish.annotate(period=F('quarter'))
    qs_active_art_malnourish = qs_active_art_malnourish.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_active_art_malnourish = qs_active_art_malnourish.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))

    gen_raster = grabbag.rasterize(ou_list, de_active_art_malnourish_meta, val_active_art_malnourish, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_active_art_malnourish2 = list(gen_raster)
   
    new_malnourish_de_names = (
        '106a Nutri N4-No. of newly identified malnourished cases in this quarter - Total',
    )
    new_malnourish_short_names = (
        'No of newly identified malnourished cases in this quarter',
    )
    de_new_malnourish_meta = list(product(new_malnourish_short_names, (None,)))

    qs_new_malnourish = DataValue.objects.what(*new_malnourish_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_new_malnourish = qs_new_malnourish.where(filter_district)
    qs_new_malnourish = qs_new_malnourish.annotate(de_name=Value(new_malnourish_short_names[0], output_field=CharField()))
    qs_new_malnourish = qs_new_malnourish.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_new_malnourish = qs_new_malnourish.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_new_malnourish = qs_new_malnourish.filter(facility__icontains='Hospital')
    qs_new_malnourish = qs_new_malnourish.annotate(period=F('quarter'))
    qs_new_malnourish = qs_new_malnourish.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_new_malnourish = qs_new_malnourish.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))

    gen_raster = grabbag.rasterize(ou_list, de_new_malnourish_meta, val_new_malnourish, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_new_malnourish2 = list(gen_raster)
   
    supp_feeding_de_names = (
        '106a Nutri N5-No. of clients who received nutrition supplementary / therapeutic feeds - Total',
    )
    supp_feeding_short_names = (
        'No. of clients who received nutrition suplementary/therapeutic feeds',
    )
    de_supp_feeding_meta = list(product(supp_feeding_short_names, (None,)))

    qs_supp_feeding = DataValue.objects.what(*supp_feeding_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_supp_feeding = qs_supp_feeding.where(filter_district)
    qs_supp_feeding = qs_supp_feeding.annotate(de_name=Value(supp_feeding_short_names[0], output_field=CharField()))
    qs_supp_feeding = qs_supp_feeding.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_supp_feeding = qs_supp_feeding.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_supp_feeding = qs_supp_feeding.filter(facility__icontains='Hospital')
    qs_supp_feeding = qs_supp_feeding.annotate(period=F('quarter'))
    qs_supp_feeding = qs_supp_feeding.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_supp_feeding = qs_supp_feeding.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))

    gen_raster = grabbag.rasterize(ou_list, de_supp_feeding_meta, val_supp_feeding, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_supp_feeding2 = list(gen_raster)

    # combine the data and group by district, subcounty and facility
    grouped_vals = groupbylist(sorted(chain(val_opd_attend2, val_muac2, val_muac_mothers2, val_mothers_total2, val_i_f_counsel2, val_m_n_counsel2, val_active_art2, val_active_art_malnourish2, val_new_malnourish2, val_supp_feeding2), key=lambda x: (x['district'], x['subcounty'], x['facility'])), key=lambda x: (x['district'], x['subcounty'], x['facility']))
    if True:
        grouped_vals = list(filter_empty_rows(grouped_vals))

    # perform calculations
    for _group in grouped_vals:
        (district_subcounty_facility, (opd_attend, muac, muac_mothers, mothers, infant_feeding, maternal_nutrition, active_art, active_art_malnourish, new_malnourish, supp_feeding, *other_vals)) = _group
        
        calculated_vals = list()

        if all_not_none(muac['numeric_sum'], opd_attend['numeric_sum']) and opd_attend['numeric_sum']:
            assessment_percent = (muac['numeric_sum'] * 100) / opd_attend['numeric_sum']
        else:
            assessment_percent = None
        assessment_percent_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': '% of clients who received nutrition asssessment  in OPD',
            'cat_combo': None,
            'numeric_sum': assessment_percent,
        }
        calculated_vals.append(assessment_percent_val)

        if all_not_none(mothers['numeric_sum'], muac_mothers['numeric_sum']) and mothers['numeric_sum']:
            assessment_mothers_percent = (muac_mothers['numeric_sum'] * 100) / mothers['numeric_sum']
        else:
            assessment_mothers_percent = None
        assessment_mothers_percent_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': '% of clients who received nutrition assessment   Pregnant/Lactating Women',
            'cat_combo': None,
            'numeric_sum': assessment_mothers_percent,
        }
        calculated_vals.append(assessment_mothers_percent_val)

        if all_not_none(active_art['numeric_sum'], active_art_malnourish['numeric_sum']) and active_art['numeric_sum']:
            active_art_malnourish_percent = (active_art_malnourish['numeric_sum'] * 100) / active_art['numeric_sum']
        else:
            active_art_malnourish_percent = None
        active_art_malnourish_percent_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': '% of active on ART assessed for Malnutrition at their visit in quarter',
            'cat_combo': None,
            'numeric_sum': active_art_malnourish_percent,
        }
        calculated_vals.append(active_art_malnourish_percent_val)

        if all_not_none(mothers['numeric_sum'], infant_feeding['numeric_sum']) and mothers['numeric_sum']:
            mothers_i_f_percent = (infant_feeding['numeric_sum'] * 100) / mothers['numeric_sum']
        else:
            mothers_i_f_percent = None
        mothers_i_f_percent_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': '% of pregnant and lactating women who received infant feeding counseling ',
            'cat_combo': None,
            'numeric_sum': mothers_i_f_percent,
        }
        calculated_vals.append(mothers_i_f_percent_val)

        if all_not_none(mothers['numeric_sum'], maternal_nutrition['numeric_sum']) and mothers['numeric_sum']:
            mothers_m_n_percent = (maternal_nutrition['numeric_sum'] * 100) / mothers['numeric_sum']
        else:
            mothers_m_n_percent = None
        mothers_m_n_percent_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': '% of pregnant and lactating women who received maternal nutrition counseling ',
            'cat_combo': None,
            'numeric_sum': mothers_m_n_percent,
        }
        calculated_vals.append(mothers_m_n_percent_val)

        if all_not_none(new_malnourish['numeric_sum'], supp_feeding['numeric_sum']) and new_malnourish['numeric_sum']:
            supp_feeding_percent = (supp_feeding['numeric_sum'] * 100) / new_malnourish['numeric_sum']
        else:
            supp_feeding_percent = None
        supp_feeding_percent_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': '% of newly identified malnorished cases who received nutrition suplementary/ therapeutic feeds',
            'cat_combo': None,
            'numeric_sum': supp_feeding_percent,
        }
        calculated_vals.append(supp_feeding_percent_val)

        _group[1].extend(calculated_vals)

    data_element_names = list()
    data_element_names += list(product(opd_attend_short_names, (None,)))
    data_element_names += list(product(muac_short_names, (None,)))
    data_element_names += list(product(muac_mothers_short_names, (None,)))
    data_element_names += list(product(mothers_total_short_names, (None,)))
    data_element_names += list(product(i_f_counsel_short_names, (None,)))
    data_element_names += list(product(m_n_counsel_short_names, (None,)))
    data_element_names += list(product(active_art_short_names, (None,)))
    data_element_names += list(product(active_art_malnourish_short_names, (None,)))
    data_element_names += list(product(new_malnourish_short_names, (None,)))
    data_element_names += list(product(supp_feeding_short_names, (None,)))

    data_element_names += list(product(['% of clients who received nutrition asssessment  in OPD'], (None,)))
    data_element_names += list(product(['% of clients who received nutrition assessment   Pregnant/Lactating Women'], (None,)))
    data_element_names += list(product(['% of active on ART assessed for Malnutrition at their visit in quarter'], (None,)))
    data_element_names += list(product(['% of pregnant and lactating women who received infant feeding counseling '], (None,)))
    data_element_names += list(product(['% of pregnant and lactating women who received maternal nutrition counseling '], (None,)))
    data_element_names += list(product(['% of newly identified malnorished cases who received nutrition suplementary/ therapeutic feeds'], (None,)))

    legend_sets = list()
    muac_ls = LegendSet()
    muac_ls.name = 'Nutrition Assessment'
    muac_ls.add_interval('red', 0, 25)
    muac_ls.add_interval('yellow', 25, 50)
    muac_ls.add_interval('green', 50, None)
    muac_ls.mappings[13] = True
    legend_sets.append(muac_ls)
    malnourished_ls = LegendSet()
    malnourished_ls.name = 'Assessed for Malnutrition'
    malnourished_ls.add_interval('red', 0, 50)
    malnourished_ls.add_interval('yellow', 50, 80)
    malnourished_ls.add_interval('green', 80, None)
    for i in range(13+1, 13+1+5):
        malnourished_ls.mappings[i] = True
    legend_sets.append(malnourished_ls)

    if output_format == 'EXCEL':
        from django.http import HttpResponse
        import openpyxl

        wb = openpyxl.workbook.Workbook()
        ws = wb.active # workbooks are created with at least one worksheet
        ws.title = 'Sheet1' # unfortunately it is named "Sheet" not "Sheet1"
        ws.page_setup.orientation = ws.ORIENTATION_LANDSCAPE
        ws.page_setup.paperSize = ws.PAPERSIZE_A4

        headers = ou_headers + data_element_names
        for i, name in enumerate(headers, start=1):
            c = ws.cell(row=1, column=i)
            if not isinstance(name, tuple):
                c.value = str(name)
            else:
                de, cat_combo = name
                if cat_combo is None:
                    c.value = str(de)
                else:
                    c.value = str(de) + '\n' + str(cat_combo)
        for i, g in enumerate(grouped_vals, start=2):
            ou_path, g_val_list = g
            for col_idx, ou in enumerate(ou_path, start=1):
                ws.cell(row=i, column=col_idx, value=ou)
            for j, g_val in enumerate(g_val_list, start=len(ou_path)+1):
                ws.cell(row=i, column=j, value=g_val['numeric_sum'])

        for ls in legend_sets:
            # apply conditional formatting from LegendSets
            for rule in ls.openpyxl_rules():
                for cell_range in ls.excel_ranges():
                    print(cell_range, ls)
                    ws.conditional_formatting.add(cell_range, rule)


        response = HttpResponse(openpyxl.writer.excel.save_virtual_workbook(wb), content_type='application/vnd.ms-excel')
        response['Content-Disposition'] = 'attachment; filename="nutrition_hospitals_scorecard.xlsx"'

        return response

    context = {
        'grouped_data': grouped_vals,
        'data_element_names': data_element_names,
        'legend_sets': legend_sets,
        'period_desc': period_desc,
        'period_list': PREV_5YR_QTRS,
        'district_list': DISTRICT_LIST,
    }

    return render(request, 'cannula/nutrition_hospitals.html', context)

@login_required
def vl_by_site(request, output_format='HTML'):
    this_day = date.today()
    this_year = this_day.year
    PREV_5YR_QTRS = ['%d-Q%d' % (y, q) for y in range(this_year, this_year-6, -1) for q in range(4, 0, -1)]
    DISTRICT_LIST = list(OrgUnit.objects.filter(level=1).order_by('name').values_list('name', flat=True))

    if 'period' in request.GET and request.GET['period'] in PREV_5YR_QTRS:
        filter_period=request.GET['period']
    else:
        filter_period = '%d-Q%d' % (this_year, month2quarter(this_day.month))

    period_desc = dateutil.DateSpan.fromquarter(filter_period).format()

    if 'district' in request.GET and request.GET['district'] in DISTRICT_LIST:
        filter_district = OrgUnit.objects.get(name=request.GET['district'])
    else:
        filter_district = None

    # # all facilities (or equivalent)
    qs_ou = OrgUnit.objects.filter(level=3).annotate(district=F('parent__parent__name'), subcounty=F('parent__name'), facility=F('name'))
    if filter_district:
        qs_ou = qs_ou.filter(Q(lft__gte=filter_district.lft) & Q(rght__lte=filter_district.rght))
    ou_list = list(qs_ou.values_list('district', 'subcounty', 'facility'))
    ou_headers = ['District', 'Subcounty', 'Facility']

    def val_with_subcat_fun(row, col):
        district, subcounty, facility = row
        de_name, subcategory = col
        return { 'district': district, 'subcounty': subcounty, 'facility': facility, 'cat_combo': subcategory, 'de_name': de_name, 'numeric_sum': None }

    viral_load_de_names = (
        'VL samples rejected',
        'VL samples sent',
    )
    viral_load_short_names = (
        'VL samples rejected',
        'VL samples sent',
    )
    de_viral_load_meta = list(product(viral_load_short_names, (None,)))

    qs_viral_load = DataValue.objects.what(*viral_load_de_names).filter(quarter=filter_period)
    if filter_district:
        qs_viral_load = qs_viral_load.where(filter_district)
    qs_viral_load = qs_viral_load.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_viral_load = qs_viral_load.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_viral_load = qs_viral_load.annotate(period=F('quarter'))
    qs_viral_load = qs_viral_load.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_viral_load = qs_viral_load.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_viral_load = list(val_viral_load)

    gen_raster = grabbag.rasterize(ou_list, de_viral_load_meta, val_viral_load, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_viral_load2 = list(gen_raster)

    viral_target_de_names = (
        'VL_TARGET',
    )
    viral_target_short_names = (
        'Annual target',
    )
    de_viral_target_meta = list(product(viral_target_short_names, (None,)))

    qs_viral_target = DataValue.objects.what(*viral_target_de_names).filter(year=filter_period[:4])
    if filter_district:
        qs_viral_target = qs_viral_target.where(filter_district)
    qs_viral_target = qs_viral_target.annotate(de_name=Value(viral_target_short_names[0], output_field=CharField()))
    qs_viral_target = qs_viral_target.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_viral_target = qs_viral_target.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_viral_target = qs_viral_target.annotate(period=F('year'))
    qs_viral_target = qs_viral_target.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_viral_target = qs_viral_target.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_viral_target = list(val_viral_target)

    gen_raster = grabbag.rasterize(ou_list, de_viral_target_meta, val_viral_target, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_viral_target2 = list(gen_raster)

    # combine the data and group by district, subcounty and facility
    grouped_vals = groupbylist(sorted(chain(val_viral_target2, val_viral_load2), key=lambda x: (x['district'], x['subcounty'], x['facility'])), key=lambda x: (x['district'], x['subcounty'], x['facility']))
    if True:
        grouped_vals = list(filter_empty_rows(grouped_vals))

    # perform calculations
    for _group in grouped_vals:
        (district_subcounty_facility, (vl_target, vl_rejected, vl_sent, *other_vals)) = _group
        
        calculated_vals = list()

        if all_not_none(vl_target['numeric_sum'], vl_sent['numeric_sum']) and vl_target['numeric_sum']:
            vl_sent_percent = (vl_sent['numeric_sum'] * 100) / (vl_target['numeric_sum']/Decimal(4))
        else:
            vl_sent_percent = None
        vl_sent_percent_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': '% Achievement (sent)',
            'cat_combo': None,
            'numeric_sum': vl_sent_percent,
        }
        calculated_vals.append(vl_sent_percent_val)

        if all_not_none(vl_sent['numeric_sum'], vl_rejected['numeric_sum']) and vl_sent['numeric_sum']:
            vl_rejected_percent = (vl_rejected['numeric_sum'] * 100) / vl_sent['numeric_sum']
        else:
            vl_rejected_percent = None
        vl_rejected_percent_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': '% Sample rejection',
            'cat_combo': None,
            'numeric_sum': vl_rejected_percent,
        }
        calculated_vals.append(vl_rejected_percent_val)

        vl_returned = default_zero(vl_sent['numeric_sum']) - default_zero(vl_rejected['numeric_sum'])
        vl_returned_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': 'Samples returned',
            'cat_combo': 'None',
            'numeric_sum': vl_returned,
        }
        calculated_vals.append(vl_returned_val)

        if all_not_none(vl_sent['numeric_sum'], vl_returned) and vl_sent['numeric_sum']:
            vl_returned_percent = (vl_returned * 100) / vl_sent['numeric_sum']
        else:
            vl_returned_percent = None
        vl_returned_percent_val = {
            'district': district_subcounty_facility[0],
            'subcounty': district_subcounty_facility[1],
            'facility': district_subcounty_facility[2],
            'de_name': '% Achievement (returned)',
            'cat_combo': None,
            'numeric_sum': vl_returned_percent,
        }
        calculated_vals.append(vl_returned_percent_val)

        _group[1].extend(calculated_vals)

    data_element_names = list()
    data_element_names += de_viral_target_meta
    data_element_names += de_viral_load_meta

    data_element_names += list(product(['% Achievement'], (None,)))
    data_element_names += list(product(['% Sample rejection'], (None,)))
    data_element_names += list(product(['Samples returned'], (None,)))
    data_element_names += list(product(['% Achievement'], (None,)))

    legend_sets = list()
    achievement_ls = LegendSet()
    achievement_ls.name = 'Achievement'
    achievement_ls.add_interval('orange', 0, 25)
    achievement_ls.add_interval('yellow', 25, 40)
    achievement_ls.add_interval('light-green', 40, 60)
    achievement_ls.add_interval('green', 60, None)
    achievement_ls.mappings[6] = True
    achievement_ls.mappings[9] = True
    legend_sets.append(achievement_ls)
    rejection_ls = LegendSet()
    rejection_ls.name = 'Sample Rejection'
    rejection_ls.add_interval('orange', 4, None)
    rejection_ls.mappings[7] = True
    legend_sets.append(rejection_ls)

    if output_format == 'EXCEL':
        from django.http import HttpResponse
        import openpyxl

        wb = openpyxl.workbook.Workbook()
        ws = wb.active # workbooks are created with at least one worksheet
        ws.title = 'Sheet1' # unfortunately it is named "Sheet" not "Sheet1"
        ws.page_setup.orientation = ws.ORIENTATION_LANDSCAPE
        ws.page_setup.paperSize = ws.PAPERSIZE_A4

        headers = ou_headers + data_element_names
        for i, name in enumerate(headers, start=1):
            c = ws.cell(row=1, column=i)
            if not isinstance(name, tuple):
                c.value = str(name)
            else:
                de, cat_combo = name
                if cat_combo is None:
                    c.value = str(de)
                else:
                    c.value = str(de) + '\n' + str(cat_combo)
        for i, g in enumerate(grouped_vals, start=2):
            ou_path, g_val_list = g
            for col_idx, ou in enumerate(ou_path, start=1):
                ws.cell(row=i, column=col_idx, value=ou)
            for j, g_val in enumerate(g_val_list, start=len(ou_path)+1):
                ws.cell(row=i, column=j, value=g_val['numeric_sum'])

        for ls in legend_sets:
            # apply conditional formatting from LegendSets
            for rule in ls.openpyxl_rules():
                for cell_range in ls.excel_ranges():
                    print(cell_range, ls)
                    ws.conditional_formatting.add(cell_range, rule)


        response = HttpResponse(openpyxl.writer.excel.save_virtual_workbook(wb), content_type='application/vnd.ms-excel')
        response['Content-Disposition'] = 'attachment; filename="viral_load_sites_scorecard.xlsx"'

        return response

    context = {
        'grouped_data': grouped_vals,
        'data_element_names': data_element_names,
        'legend_sets': legend_sets,
        'period_desc': period_desc,
        'period_list': PREV_5YR_QTRS,
        'district_list': DISTRICT_LIST,
    }

    return render(request, 'cannula/vl_sites.html', context)

@login_required
def sc_mos_by_site(request, output_format='HTML'):
    this_day = date.today()
    this_year = this_day.year
    PREV_5YR_QTRS = ['%d-Q%d' % (y, q) for y in range(this_year, this_year-6, -1) for q in range(4, 0, -1)]
    month_years = zip([((this_day.month-i-1)%12)+1 for i in range(5*12)], ([this_day.year] * this_day.month) + sorted([this_day.year-i for i in range(1, 5)]*12, reverse=True))
    PREV_5YR_MONTHS = ['{0}-{1:02}'.format(y, m) for m, y in month_years]
    DISTRICT_LIST = list(OrgUnit.objects.filter(level=1).order_by('name').values_list('name', flat=True))

    if 'period' in request.GET and request.GET['period'] in PREV_5YR_MONTHS:
        filter_period=request.GET['period']
    else:
        filter_period = '{0}-{1:02}'.format(this_year, this_day.month)

    period_desc = filter_period #dateutil.DateSpan.fromquarter(filter_period).format()

    if 'district' in request.GET and request.GET['district'] in DISTRICT_LIST:
        filter_district = OrgUnit.objects.get(name=request.GET['district'])
    else:
        filter_district = None

    # # all facilities (or equivalent)
    qs_ou = OrgUnit.objects.filter(level=3).annotate(district=F('parent__parent__name'), subcounty=F('parent__name'), facility=F('name'))
    if filter_district:
        qs_ou = qs_ou.filter(Q(lft__gte=filter_district.lft) & Q(rght__lte=filter_district.rght))
    ou_list = list(qs_ou.values_list('district', 'subcounty', 'facility'))
    ou_headers = ['District', 'Subcounty', 'Facility']

    def val_with_subcat_fun(row, col):
        district, subcounty, facility = row
        de_name, subcategory = col
        return { 'district': district, 'subcounty': subcounty, 'facility': facility, 'cat_combo': subcategory, 'de_name': de_name, 'numeric_sum': None }

    supply_names = [
        '105-6  Zidovudine /Lamivudine/Nevirapine (AZT/3TC/NVP)',
        '105-6 (RHZE) blister strip 150/75/400/275 mg',
        '105-6 Abacavir/Lamivudine (ABC/3TC) 60mg/30mg (Paediatric)',
        '105-6 Amoxicillin dispersible 125mg tablet (For children)',
        '105-6 Artemether/ Lumefantrine 100/20mg tablet',
        '105-6 Bendrofulazide (Aprinox) 5mg',
        '105-6 Blood 450 ml',
        '105-6 CD4 reagent Specify',
        # '105-6 Captopril 25mg tablet',
        # '105-6 Cardiac Aspirin 75/80 mg',
        # '105-6 Ceftriaxone 1g Injection',
        # '105-6 Chlorhexidine 20%',
        # '105-6 Co-tromoxazole 480mg tablet',
        # '105-6 Cotrimoxazole 960mg tablet',
        # '105-6 Determine HIV Screening test, tests',
        # '105-6 Efavirenz (EFV) 600mg',
        # '105-6 Glibenclamide 5mg tablet',
        # '105-6 Insulin short-acting',
        # '105-6 Mama Kit',
        # '105-6 Measles Vaccine',
        # '105-6 Metformin 500mg',
        # '105-6 Misoprostol 200mcg Tablet',
        # '105-6 Nevirapine (NVP) 200mg',
        # '105-6 Nevirapine (NVP) 50mg',
        # '105-6 Nifedipine tablets 20mg tablet',
        # '105-6 ORS Sachets with zinc tablet',
        # '105-6 Oxytocin Injection',
        # '105-6 Propranolol 40mg tablet',
        # '105-6 RH blister strip 150/75 mg',
        # '105-6 Ready to use Therapeutic feeds (RUTF)',
        # '105-6 Stat-pack HIV Confirmatory rapid tests, tests',
        # '105-6 Sulfadoxine / Pyrimethamine tablet',
        # '105-6 Tenofovir/Lamivudine (TDF/3TC) 300mg/300mg',
        # '105-6 Tenofovir/Lamivudine/Efavirenz (TDF/3TC/EFV) 300mg/300mg/',
        # '105-6 Therapeutic milk F100 (100Kcal/100ml)',
        # '105-6 Therapeutic milk F75 (75Kcal/100ml)',
        # '105-6 Unigold HIV RDT Tie-breaker test, tests',
        # '105-6 ZN reagent for AFB',
        # '105-6 Zidovudine/Lamivudine (AZT/3TC) 300mg/150m'
    ]
    stock_de_names = ((s+' Days out of stock', s+' Quantity Utilized', s+' Stock at Hand') for s in supply_names)
    stock_de_names = list(chain.from_iterable(stock_de_names)) # flatten the list of tuples of strings into a list of strings
    de_stock_meta = list(product(stock_de_names, (None,)))

    qs_stock = DataValue.objects.what(*stock_de_names).filter(month=filter_period)
    if filter_district:
        qs_stock = qs_stock.where(filter_district)
    qs_stock = qs_stock.annotate(cat_combo=Value(None, output_field=CharField()))

    qs_stock = qs_stock.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'), facility=F('org_unit__name'))
    qs_stock = qs_stock.annotate(period=F('quarter'))
    qs_stock = qs_stock.order_by('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period')
    val_stock = qs_stock.values('district', 'subcounty', 'facility', 'de_name', 'cat_combo', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    val_stock = list(val_stock)

    gen_raster = grabbag.rasterize(ou_list, de_stock_meta, val_stock, lambda x: (x['district'], x['subcounty'], x['facility']), lambda x: (x['de_name'], x['cat_combo']), val_with_subcat_fun)
    val_stock2 = list(gen_raster)

    # combine the data and group by district, subcounty and facility
    grouped_vals = groupbylist(sorted(chain(val_stock2), key=lambda x: (x['district'], x['subcounty'], x['facility'])), key=lambda x: (x['district'], x['subcounty'], x['facility']))
    if True:
        grouped_vals = list(filter_empty_rows(grouped_vals))

    calc_names = OrderedDict()
    # perform calculations
    for _group in grouped_vals:
        (district_subcounty_facility, (*stock_vals,)) = _group
        days_out, utilized, on_hand, *other_vals = stock_vals
        
        calculated_vals = list()
        AVG_MONTH_DAYS = 30 # days in month

        for days_out, utilized, on_hand in grouper(stock_vals, 3):
            if all_not_none(utilized['numeric_sum'], days_out['numeric_sum']) and days_out['numeric_sum'] < AVG_MONTH_DAYS:
                avg_consumption = utilized['numeric_sum'] * (AVG_MONTH_DAYS / (AVG_MONTH_DAYS - days_out['numeric_sum']))
            else:
                avg_consumption = None
            # calc_name = on_hand['de_name'].replace('Stock at Hand', 'Adjusted Consumption')
            # calc_names[calc_name] = True
            # avg_consumption_val = {
            #     'district': district_subcounty_facility[0],
            #     'subcounty': district_subcounty_facility[1],
            #     'facility': district_subcounty_facility[2],
            #     'de_name': calc_name,
            #     'cat_combo': None,
            #     'numeric_sum': avg_consumption,
            # }
            # calculated_vals.append(avg_consumption_val)

            if all_not_none(on_hand['numeric_sum']) and avg_consumption and on_hand['numeric_sum'] > 0:
                months_of_stock = on_hand['numeric_sum']/(avg_consumption)
            else:
                months_of_stock = None
                if on_hand['numeric_sum']:
                    months_of_stock = -on_hand['numeric_sum']
            calc_name = on_hand['de_name'].replace('Stock at Hand', 'Months of Stock')
            calc_names[calc_name] = True
            sc_mos_val = {
                'district': district_subcounty_facility[0],
                'subcounty': district_subcounty_facility[1],
                'facility': district_subcounty_facility[2],
                'de_name': calc_name,
                'cat_combo': None,
                'numeric_sum': months_of_stock,
            }
            calculated_vals.append(sc_mos_val)

        if True:
            _group[1] = calculated_vals
        else:
            _group[1].extend(calculated_vals)

    data_element_names = list()
    # data_element_names += de_stock_meta

    data_element_names.extend([(c, None) for c in calc_names])

    mos_base_index = 1+len(ou_headers)+len(stock_de_names)
    print(mos_base_index, len(ou_headers), len(stock_de_names))
    legend_sets = list()
    sc_mos_ls = LegendSet()
    sc_mos_ls.name = 'Months of Stock (MOS)'
    sc_mos_ls.add_interval('red', 0, 2)
    sc_mos_ls.add_interval('green', 2, 4)
    sc_mos_ls.add_interval('yellow', 4, None)
    for i in range(len(supply_names)):
        sc_mos_ls.mappings[mos_base_index+(i*2)] = True
    legend_sets.append(sc_mos_ls)
    sc_soh_ls = LegendSet()
    sc_soh_ls.name = 'Stock on Hand (SOH): invalid MOS'
    sc_soh_ls.add_interval('light-green', None, 0)
    for i in range(len(supply_names)):
        sc_soh_ls.mappings[mos_base_index+(i*2)] = True
    legend_sets.append(sc_soh_ls)

    if output_format == 'EXCEL':
        from django.http import HttpResponse
        import openpyxl

        wb = openpyxl.workbook.Workbook()
        ws = wb.active # workbooks are created with at least one worksheet
        ws.title = 'Sheet1' # unfortunately it is named "Sheet" not "Sheet1"
        ws.page_setup.orientation = ws.ORIENTATION_LANDSCAPE
        ws.page_setup.paperSize = ws.PAPERSIZE_A4

        headers = ou_headers + data_element_names
        for i, name in enumerate(headers, start=1):
            c = ws.cell(row=1, column=i)
            if not isinstance(name, tuple):
                c.value = str(name)
            else:
                de, cat_combo = name
                if cat_combo is None:
                    c.value = str(de)
                else:
                    c.value = str(de) + '\n' + str(cat_combo)
        for i, g in enumerate(grouped_vals, start=2):
            ou_path, g_val_list = g
            for col_idx, ou in enumerate(ou_path, start=1):
                ws.cell(row=i, column=col_idx, value=ou)
            for j, g_val in enumerate(g_val_list, start=len(ou_path)+1):
                ws.cell(row=i, column=j, value=g_val['numeric_sum'])

        for ls in legend_sets:
            # apply conditional formatting from LegendSets
            for rule in ls.openpyxl_rules():
                for cell_range in ls.excel_ranges():
                    print(cell_range, ls)
                    ws.conditional_formatting.add(cell_range, rule)


        response = HttpResponse(openpyxl.writer.excel.save_virtual_workbook(wb), content_type='application/vnd.ms-excel')
        response['Content-Disposition'] = 'attachment; filename="sc_mos_sites_scorecard.xlsx"'

        return response

    context = {
        'grouped_data': grouped_vals,
        'ou_headers': ou_headers,
        'data_element_names': data_element_names,
        'legend_sets': legend_sets,
        'period_desc': period_desc,
        'period_list': PREV_5YR_MONTHS,
        'district_list': DISTRICT_LIST,
        'excel_url': make_excel_url(request.path),
    }

    return render(request, 'cannula/sc_mos_sites.html', context)
