from django.conf.urls import url
from django.contrib import admin

from index import views

urlpatterns = [
    url(r'^$', views.index, name='index'),
    url(r'^sql/$', views.sql, name='sql'),
    url(r'^table/$', views.table, name='table'),
    url(r'^table/(?P<table_name>.+)$', views.table, name='table'),
]
