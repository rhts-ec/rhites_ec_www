from django.core.management.base import BaseCommand, CommandError
from cannula.models import OrgUnit, DataValue, orgunit_cleanup_name

from collections import defaultdict

def ou_value_count(ou):
    return DataValue.objects.filter(org_unit=ou).count()

def get_duplicate_orgunits():
    ou_uniques = defaultdict(list)
    for ou in OrgUnit.objects.all():
        ou_name = orgunit_cleanup_name(ou.name)
        ou_uniques[(ou.parent_id, ou_name)].append(ou)
    
    return [unique_list for k, unique_list in ou_uniques.items() if len(unique_list) > 1]

class Command(BaseCommand):
    help = 'Identifies duplicated OrgUnits'

    def handle(self, *args, **options):
        ou_duplicates = get_duplicate_orgunits()
        for dup_list in ou_duplicates:
            ou_retain, *ou_discards = dup_list
            clean_ou_name = orgunit_cleanup_name(ou_retain.name)
            
            self.stdout.write('%s: %s' % (clean_ou_name, dup_list))
