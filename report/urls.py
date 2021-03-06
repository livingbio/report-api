#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright © 2015 lizongzhe
#
# Distributed under terms of the MIT license.

from .views import ReportRootView, ReportGroupView, ReportReportView, ReportApiView, Upload_ReportView
from django.conf.urls import include, url
from django.contrib import admin
from django.views.decorators.csrf import csrf_exempt


admin.autodiscover()

urlpatterns = [
    url(r'quick_upload/?$', csrf_exempt(Upload_ReportView.as_view()), name='quick_upload'),
    url(r'(?P<group>\w+)/(?P<report>\w+)/(?P<api>\w+)/?$', ReportApiView.as_view(), name='api'),
    url(r'(?P<group>\w+)/(?P<report>\w+)/?$', ReportReportView.as_view(), name='report'),
    url(r'(?P<group>\w+)/?$', ReportGroupView.as_view(), name='group'),
    url(r'/?$', ReportRootView.as_view(), name='groups'),
]
