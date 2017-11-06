from django.shortcuts import render
from django.db.models import Avg, Case, Count, F, Max, Min, Prefetch, Q, Sum, When

from datetime import date
from itertools import groupby, tee, chain, product

from . import dateutil, grabbag

from .models import DataElement, OrgUnit, DataValue

def index(request):
    return render(request, 'cannula/index.html')

def data_elements(request):
    data_elements = DataElement.objects.order_by('name').all()
    return render(request, 'cannula/data_element_listing.html', {'data_elements': data_elements})

# avoid strange behaviour from itertools.groupby by evaluating all the group iterables as lists
def groupbylist(*args, **kwargs):
    return [(k, list(g)) for k, g in groupby(*args, **kwargs)]

def month2quarter(month_num):
    return ((month_num-1)//3+1)

def ipt_quarterly(request):
    ipt_de_names = (
        '105-2.1 A6:First dose IPT (IPT1)',
        '105-2.1 A7:Second dose IPT (IPT2)',
    )

    this_day = date.today()
    this_year = this_day.year
    PREV_5YR_QTRS = ['%d-Q%d' % (y, q) for y in range(this_year, this_year-6, -1) for q in range(4, 0, -1)]

    if 'period' in request.GET and request.GET['period'] in PREV_5YR_QTRS:
        filter_period=request.GET['period']
    else:
        filter_period = '%d-Q%d' % (this_year, month2quarter(this_day.month))

    period_desc = dateutil.DateSpan.fromquarter(filter_period).format()

    # get IPT1 and IPT2 without subcategory disaggregation
    qs = DataValue.objects.what(*ipt_de_names).filter(quarter=filter_period)
    # use clearer aliases for the unwieldy names
    qs = qs.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'))
    qs = qs.annotate(period=F('quarter')) # TODO: review if this can still work with different periods
    qs = qs.order_by('district', 'subcounty', 'de_name', 'period')
    val_dicts = qs.values('district', 'subcounty', 'de_name', 'period').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))
    
    # all subcounties (or equivalent)
    qs_ou = OrgUnit.objects.filter(level=2).annotate(district=F('parent__name'), subcounty=F('name'))
    ou_list = qs_ou.values_list('district', 'subcounty')

    def val_fun(row, col):
        return { 'district': row[0], 'subcounty': row[1], 'de_name': col, 'numeric_sum': None }
    gen_raster = grabbag.rasterize(ou_list, ipt_de_names, val_dicts, lambda x: (x['district'], x['subcounty']), lambda x: x['de_name'], val_fun)
    val_dicts = list(gen_raster)

    # get list of subcategories for IPT2
    qs_ipt_subcat = DataValue.objects.what('105-2.1 A7:Second dose IPT (IPT2)').order_by().values('de_name', 'category_str').distinct()
    subcategory_names = (*((x['de_name'], x['category_str']) for x in qs_ipt_subcat),)

    # get IPT2 with subcategory disaggregation
    qs2 = DataValue.objects.what('105-2.1 A7:Second dose IPT (IPT2)').filter(quarter=filter_period)
    # use clearer aliases for the unwieldy names
    qs2 = qs2.annotate(district=F('org_unit__parent__parent__name'), subcounty=F('org_unit__parent__name'))
    qs2 = qs2.annotate(period=F('quarter')) # TODO: review if this can still work with different periods
    qs2 = qs2.order_by('district', 'subcounty', 'de_name', 'period', 'category_str')
    val_dicts2 = qs2.values('district', 'subcounty', 'de_name', 'period', 'category_str').annotate(values_count=Count('numeric_value'), numeric_sum=Sum('numeric_value'))

    def val_with_subcat_fun(row, col):
        district, subcounty = row
        de_name, subcategory = col
        return { 'district': district, 'subcounty': subcounty, 'category_str': subcategory, 'de_name': de_name, 'numeric_sum': None }
    gen_raster = grabbag.rasterize(ou_list, subcategory_names, val_dicts2, lambda x: (x['district'], x['subcounty']), lambda x: (x['de_name'], x['category_str']), val_with_subcat_fun)
    val_dicts2 = list(gen_raster)

    # get expected pregnancies
    qs3 = DataValue.objects.what('Expected Pregnancies (*5/100)')
    # use clearer aliases for the unwieldy names
    qs3 = qs3.annotate(district=F('org_unit__parent__name'), subcounty=F('org_unit__name'))
    qs3 = qs3.annotate(period=F('year')) # TODO: review if this can still work with different periods
    qs3 = qs3.order_by('district', 'subcounty', 'de_name', 'period')
    val_dicts3 = qs3.values('district', 'subcounty', 'de_name', 'period').annotate(numeric_sum=Sum('numeric_value'))

    gen_raster = grabbag.rasterize(ou_list, ('Expected Pregnancies (*5/100)',), val_dicts3, lambda x: (x['district'], x['subcounty']), lambda x: x['de_name'], val_fun)
    val_dicts3 = list(gen_raster)

    # combine the data and group by district and subcounty
    grouped_vals = groupbylist(sorted(chain(val_dicts3, val_dicts, val_dicts2), key=lambda x: (x['district'], x['subcounty'])), key=lambda x: (x['district'], x['subcounty']))
    
    # calculate the IPT rate for the IPT1/IPT2 values (without subcategories)
    for _group in grouped_vals:
        (district_subcounty, (preg_val, *other_vals)) = _group
        if preg_val['de_name'] == 'Expected Pregnancies (*5/100)':
            for val in other_vals:
                if val['de_name'] in ipt_de_names and 'category_str' not in val:
                    pregnancies_per_annum = preg_val['numeric_sum']
                    if pregnancies_per_annum != 0 and val['numeric_sum']:
                        val['ipt_rate'] = val['numeric_sum']*100/(pregnancies_per_annum/4)
                    else:
                        val['ipt_rate'] = None

    context = {
        'grouped_data': grouped_vals,
        'data_element_names': ipt_de_names,
        'subcategory_names': subcategory_names,
        'period': filter_period,
        'period_desc': period_desc,
        'period_list': PREV_5YR_QTRS,
    }

    return render(request, 'cannula/ipt_quarterly.html', context)

def malaria_compliance(request):
    cases_de_names = (
        '105-1.3 OPD Malaria (Total)',
        '105-1.3 OPD Malaria Confirmed (Microscopic & RDT)',
    )

    this_day = date.today()
    this_year = this_day.year
    PREV_5YR_QTRS = ['%d-Q%d' % (y, q) for y in range(this_year, this_year-6, -1) for q in range(4, 0, -1)]

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
    
    # all facilities (or equivalent)
    qs_ou = OrgUnit.objects.filter(level=3).annotate(district=F('parent__parent__name'), subcounty=F('parent__name'), facility=F('name'))
    ou_list = qs_ou.values_list('district', 'subcounty', 'facility')

    # get data values without subcategory disaggregation
    qs = DataValue.objects.what(*cases_de_names)
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

    data_element_names = list()
    for de_n in cases_de_names:
        data_element_names.append((de_n, None))

    context = {
        'grouped_data': grouped_vals,
        'data_element_names': data_element_names,
        'start_period': start_quarter,
        'end_period': end_quarter,
        'periods': periods,
        'period_desc': dateutil.DateSpan.fromquarter(start_quarter).combine(dateutil.DateSpan.fromquarter(end_quarter)).format_long(),
        'period_list': PREV_5YR_QTRS,
    }

    return render(request, 'cannula/malaria_compliance.html', context)
