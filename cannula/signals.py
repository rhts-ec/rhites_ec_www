from django.db.models.signals import post_delete
from django.dispatch import receiver
from cannula.models import OrgUnit

# Clear the OrgUnit cache whenever an OrgUnit is deleted
@receiver(post_delete, sender=OrgUnit)
def orgunit_cache_clear_handler(sender, **kwargs):
	if OrgUnit.from_path_recurse.cache_clear and callable(OrgUnit.from_path_recurse.cache_clear):
		OrgUnit.from_path_recurse.cache_clear()
