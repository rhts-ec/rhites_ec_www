from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^$', views.index, name='index'),
    url(r'dash_malaria_compliance\.php', views.malaria_compliance, name='malaria_compliance'),
    url(r'dash_malaria_quarterly\.php', views.ipt_quarterly, name='ipt_quarterly'),
]
