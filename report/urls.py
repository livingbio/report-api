#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright © 2015 lizongzhe
#
# Distributed under terms of the MIT license.

from report.routers import ReportRouter

# type routers
iot_router = ReportRouter()


# default router
router = ReportRouter()
router.register('iot', iot_router)


from django.conf import settings
for report_api in settings.REPORT_APIS:
    try:
        __import__('{}.urls'.format(report_api))
    except Exception as e:
        pass


urlpatterns = router.urls
