from collections import namedtuple
from decimal import Decimal

from openpyxl.styles import Color, PatternFill, Font, Border
from openpyxl.formatting.rule import ColorScaleRule, CellIsRule, Rule

_COLOR_MAP = {
    'RED': 'F44336',
    'YELLOW': 'FFEB3B',
    'GREEN': '4CAF50',
    'LIGHT-GREEN': '8BC34A',
    'ORANGE': 'FF9800',
}

_CONTRAST_MAP = {
    'RED': 'FFFFFF',
    'GREEN': 'FFFFFF',
}

NEG_INF, POS_INF = Decimal('-Inf'), Decimal('Inf') # a couple of infinities

LegendInterval = namedtuple('LegendInterval', ('color', 'start', 'end'))

def legend_sort_key(legend):
    if legend.start is None and legend.end is None:
        return NEG_INF, POS_INF
    elif legend.start is None:
        return NEG_INF, legend.end
    elif legend.end is None:
        return legend.start, POS_INF
    else:
        return legend.start, legend.end

def reconstitute_slice(x):
    if isinstance(x, tuple) and len(x) == 4:
        start, stop, step, cls = x
        if cls is slice:
            return slice(start, stop, step)
    
    return x

def num2alphaindices(i):
    if i < 26:
        return i, # as a tuple
    else:
        q, r = divmod(i, 26)
        return q-1, r

def indices2colname(t):
    import string
    return ''.join([string.ascii_uppercase[i] for i in t])

def excel_column_name(i, zero_indexed=True):
    """
    >>> excel_column_name(5)
    'F'
    >>> excel_column_name(26)
    'AA'
    >>> excel_column_name(5, zero_indexed=False)
    'E'
    >>> excel_column_name(26, zero_indexed=False)
    'Z'
    """
    if zero_indexed and i < 0:
        return indices2colname(num2alphaindices(0))
    elif not zero_indexed and i < 1:
        return indices2colname(num2alphaindices(0))

    if zero_indexed:
        return indices2colname(num2alphaindices(i))
    else:
        return indices2colname(num2alphaindices(i-1))

class LegendSetMappings(object):
    """
    >>> ls = LegendSet()
    >>> ls.mappings[9]=True
    >>> ls.mappings[11:16]=True
    >>> ls.mappings
    {9, slice(11, 16, None)}
    >>> ls.mappings[13]=False
    >>> ls.mappings[8]=True
    >>> ls.mappings
    {8, 9, slice(11, 16, None)}
    >>> [x for x in ls.mappings]
    [8, 9, slice(11, 16, None)]
    """
    def __init__(self):
        self.__mappings = set()

    def __setitem__(self, key, value):
        """
        >>> ls = LegendSet()
        >>> ls.mappings[9]=True
        >>> ls.mappings
        {9}
        >>> ls.mappings[9]=False
        >>> ls.mappings
        {}
        >>> ls.mappings[0] = 5
        Traceback (most recent call last):
            ...
        TypeError: Mappings can only be assigned True/False, to enable/disable them
        >>> ls.mappings[(0, 1, 2)] = True
        >>> ls.mappings['mobile'] = True
        >>> ls.mappings[(0, 1, 2)] = False
        >>> ls.mappings['mobile'] = False
        """
        if isinstance(value, bool):
            if value:
                if isinstance(key, slice):
                    # slice is unhashable, store tuple instead
                    self.__mappings.add((key.start, key.stop, key.step, slice))
                else:
                    self.__mappings.add(key)
            else:
                if key in self.__mappings:
                    self.__mappings.remove(key)
        else:
            raise TypeError('Mappings can only be assigned True/False, to enable/disable them')
        
    def __getitem__(self, key):
        """
        Check if a mapping has been enabled for our given index/key

        >>> ls = LegendSet()
        >>> ls.mappings[9]=True
        >>> ls.mappings[9]
        True
        >>> ls.mappings[4]
        False
        """
        return key in self.__mappings

    def __iter__(self):
        return iter(map(reconstitute_slice, self.__mappings))

    def __repr__(self):
        return '{%s}' % ', '.join(map(str, self))

class LegendSet():
    def __init__(self, ignore_blanks=True, skip_header=True):
        self.__legends = list()
        self.ignore_blanks = ignore_blanks
        self.skip_header = skip_header
        self.mappings = LegendSetMappings()

    def add_interval(self, color, min, max):
        self.__legends.append(LegendInterval(color, min, max))

    def legends(self):
        return [l_i._asdict() for l_i in sorted(self.__legends, key=legend_sort_key)]

    def openpyxl_rules(self, contrast_text=True):
        if self.ignore_blanks:
            # don't use the "containsBlanks" because (in Excel) it treats 0 as blank
            rule_ignore_blanks = CellIsRule(operator='equal', formula=['""'], stopIfTrue=True)
            yield rule_ignore_blanks

        for l_i in sorted(self.__legends, key=legend_sort_key):
            interval_color = _COLOR_MAP.get(l_i.color.upper(), l_i.color)
            color_fill = PatternFill(start_color=interval_color, end_color=interval_color, fill_type='solid')
            # use a contrasting text colour, like white, against dark coloured fills
            if contrast_text and l_i.color.upper() in _CONTRAST_MAP:
                interval_font = Font(color=_CONTRAST_MAP[l_i.color.upper()], bold=True)
            else:
                interval_font = Font(bold=True)

            if l_i.start is None and l_i.end is None:
                # make everything the same colour
                rule = ColorScaleRule(start_type='percentile', start_value=0, start_color=interval_color, end_type='percentile', end_value=100, end_color=interval_color, font=interval_font)
            elif l_i.start is None:
                rule = CellIsRule(operator='lessThan', formula=[str(l_i.end)], stopIfTrue=True, fill=color_fill, font=interval_font)
            elif l_i.end is None:
                rule = CellIsRule(operator='greaterThanOrEqual', formula=[str(l_i.start)], stopIfTrue=True, fill=color_fill, font=interval_font)
            else:
                rule = CellIsRule(operator='between', formula=[str(l_i.start),str(l_i.end)], stopIfTrue=True, fill=color_fill, font=interval_font)

            yield rule

    def excel_ranges(self):
        """
        >>> ls = LegendSet()
        >>> ls.mappings[5] = True
        >>> ls.mappings[7] = True
        >>> ls.excel_ranges()
        ['F1:F16384', 'H1:H16384']
        """
        # range = 'E:E' # entire-column-range syntax doesn't work in openpyxl 2.3.0
        # use old-school column/row limit as stand-in for entire row
        if self.skip_header:
            return ['{0}2:{0}16384'.format(excel_column_name(x)) for x in self.mappings]
        else:
            return ['{0}1:{0}16384'.format(excel_column_name(x)) for x in self.mappings]

    def apply_to_worksheet(self, ws):
        # apply conditional formatting from LegendSets
        for cell_range in self.excel_ranges():
            for rule in self.openpyxl_rules():
                ws.conditional_formatting.add(cell_range, rule)

    def canonical_name(self):
        intervals = [str(x).lower() for x in [self.legends()[0]['start']] + [l['end'] for l in self.legends()]]
        colors = [str(l['color'].lower()) for l in self.legends()]
        return '_'.join(intervals + colors)

    def __str__(self):
        return '<LegendSet %s>' % (', '.join([str(l) for l in sorted(self.__legends, key=legend_sort_key)]),)
