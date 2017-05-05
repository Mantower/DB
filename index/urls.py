from django.conf.urls import url
from django.contrib import admin

from index import views

urlpatterns = [
    url(r'^$', views.index, name='index'),
    url(r'^sql/$', views.sql_view, name='sql_view'),
    url(r'^sql_insert/$', views.sql_insert, name='sql_insert'),
    url(r'^table/$', views.table_view, name='table_view'),
    url(r'^table/(?P<table_name>.+)$', views.table_view, name='table_view'),
    url(r'^init/$', views.init_db, name='init_db'),
]
