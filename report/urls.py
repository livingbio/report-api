#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright © 2015 lizongzhe
#
# Distributed under terms of the MIT license.

from report.routers import ReportRouter

# type routers
weather_router = ReportRouter()
opinion_router = ReportRouter()
iot_router = ReportRouter()


# default router
router = ReportRouter()
router.register('weather', weather_router)
router.register('opinion', opinion_router)
router.register('iot', iot_router)


from django.conf import settings
for report_api in settings.REPORT_APIS:
    __import__('{}.urls'.format(report_api))


urlpatterns = router.urls
