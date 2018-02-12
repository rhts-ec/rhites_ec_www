from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^$', views.index, name='index'),
    url(r'dashboards/malaria/$', views.index, name='thematic_malaria'),
    url(r'dashboards/malaria/compliance\.php', views.malaria_compliance, name='malaria_compliance'),
    url(r'dashboards/malaria/quarterly\.php', views.ipt_quarterly, name='ipt_quarterly'),
    url(r'dashboards/malaria/quarterly\.xls', views.ipt_quarterly, {'output_format': 'EXCEL'}, name='ipt_quarterly_excel'),
    url(r'validation_rule\.php', views.validation_rule, name='validation_rule'),
    url(r'data_workflow_new.php', views.data_workflow_new, name='data_workflow_new'),
    url(r'data_workflow.php', views.data_workflow_detail, name='data_workflow_detail'),
    url(r'data_workflows.php', views.data_workflow_listing, name='data_workflow_listing'),
    url(r'data_element_alias.php', views.data_element_alias, name='data_element_alias'),
    url(r'dashboards/hts/$', views.index, name='thematic_hts'),
    url(r'dashboards/hts/sites.php', views.hts_by_site, name='hts_sites'),
    url(r'dashboards/hts/districts.php', views.hts_by_district, name='hts_districts'),
    url(r'dashboards/vmmc/$', views.index, name='thematic_vmmc'),
    url(r'dashboards/vmmc/sites.php', views.vmmc_by_site, name='vmmc_sites'),
    url(r'dashboards/lab/$', views.index, name='thematic_lab'),
    url(r'dashboards/lab/sites.php', views.lab_by_site, name='lab_sites'),
    url(r'dashboards/fp/$', views.index, name='thematic_fp'),
    url(r'dashboards/fp/sites.php', views.fp_by_site, name='fp_sites'),
]
