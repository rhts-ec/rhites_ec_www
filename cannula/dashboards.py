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

class LegendSet():
    def __init__(self, ignore_blanks=True):
        self.__legends = list()
        self.ignore_blanks = ignore_blanks

    def add_interval(self, color, min, max):
        self.__legends.append(LegendInterval(color, min, max))

    def legends(self):
        return [l_i._asdict() for l_i in sorted(self.__legends, key=legend_sort_key)]

    def openpyxl_rules(self):
        if self.ignore_blanks:
            rule_ignore_blanks = Rule(type="containsBlanks", stopIfTrue=True)
            yield rule_ignore_blanks

        for l_i in self.__legends:
            interval_color = _COLOR_MAP.get(l_i.color.upper(), l_i.color)
            color_fill = PatternFill(start_color=interval_color, end_color=interval_color, fill_type='solid')

            if l_i.start is None and l_i.end is None:
                # make everything the same colour
                rule = ColorScaleRule(start_type='percentile', start_value=0, start_color=interval_color, end_type='percentile', end_value=100, end_color=interval_color)
            elif l_i.start is None:
                rule = CellIsRule(operator='lessThan', formula=[str(l_i.end)], stopIfTrue=True, fill=color_fill)
            elif l_i.end is None:
                rule = CellIsRule(operator='greaterThanOrEqual', formula=[str(l_i.start)], stopIfTrue=True, fill=color_fill)
            else:
                rule = CellIsRule(operator='between', formula=[str(l_i.start),str(l_i.end)], stopIfTrue=True, fill=color_fill)

            yield rule

    def __str__(self):
        return '<LegendSet %s>' % (', '.join([str(l) for l in sorted(self.__legends, key=legend_sort_key)]),)
