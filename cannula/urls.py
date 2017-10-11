from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^$', views.index, name='index'),
    url(r'summary.php\?de_id=(?P<data_element_id>[0-9]+)', views.data_element_summary, name='de_summary')
]
