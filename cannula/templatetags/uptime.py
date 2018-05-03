from django import template

import datetime
import subprocess
import functools

register = template.Library()

@functools.lru_cache(maxsize=1)
def uptime_delta(since_time):
    raw = subprocess.check_output(['uptime', '-s'])
    reboot_time = datetime.datetime.strptime(raw.decode('utf-8'), '%Y-%m-%d %H:%M:%S\n')
    return since_time - reboot_time

@register.simple_tag
def uptime():
    now = datetime.datetime.now()
    now_clamp_5mins = now.replace(minute=now.minute-now.minute%5, second=0, microsecond=0)
    delta_reboot = uptime_delta(now_clamp_5mins)
    return str(delta_reboot)
    # return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
