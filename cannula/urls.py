from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^$', views.index, name='index'),
    url(r'dash_malaria_compliance\.php', views.malaria_compliance, name='malaria_compliance'),
    url(r'dash_malaria_quarterly\.php', views.ipt_quarterly, name='ipt_quarterly'),
    url(r'validation_rule\.php', views.validation_rule, name='validation_rule'),
    url(r'data_workflow_new.php', views.data_workflow_new, name='data_workflow_new'),
    url(r'data_workflow.php', views.data_workflow_detail, name='data_workflow_detail'),
    url(r'data_workflows.php', views.data_workflow_listing, name='data_workflow_listing'),
]
