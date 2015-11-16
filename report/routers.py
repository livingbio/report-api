#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright © 2015 george
#
# Distributed under terms of the MIT license.


from django.conf.urls import url, include
#from django.contrib import admin
import re
from rest_framework.routers import BaseRouter
from rest_framework.compat import OrderedDict
from rest_framework.views import APIView
from rest_framework.response import Response
from django.core.urlresolvers import NoReverseMatch
from report.views import ReportApiView


class ReportRouter(BaseRouter):
    url = "^/?{name}/?$"
    include_url = "^/?{name}/?"
    name = "{name}"

    def get_urls(self, *args, **kwargs):
        urls = [url(self.url.format(name=''), self.get_api_root_view(),
                    name=self.name.format(name="index"))]

        for report_api_info in self.registry:
            if isinstance(report_api_info[1], BaseRouter):
                urls.append(
                    url(
                        self.include_url.format(name=report_api_info[0]),
                        include(report_api_info[1].get_urls(
                        ), namespace=report_api_info[0])
                    )
                )
            else:
                urls.append(
                    url(
                        self.url.format(name=report_api_info[0]),
                        report_api_info[1].as_view(),
                        name=self.name.format(name=report_api_info[0])
                    )
                )
        return urls

    def get_default_base_name(self, viewset):
        return "report"

    def get_api_root_view(self):
        """
        Return a view to use as the API root.
        """
        api_root_dict = OrderedDict()
        for name, viewset, basename in self.registry:

            api_root_dict[name] = viewset

        class APIRoot(APIView):
            _ignore_model_permissions = True

            def get(self, request, *args, **kwargs):
                info_list = []
                for key, viewset in api_root_dict.items():
                    try:
                        info = {}
                        curr_url = re.sub(
                            "/?$", "/", request.build_absolute_uri())
                        info['name'] = key
                        info['url'] = curr_url + key
                        if isinstance(viewset, ReportApiView):
                            info['type'] = 'api'
                            info['filter_schema'] = viewset.filter_schema
                            info['desc'] = viewset.desc
                        else:
                            info['type'] = 'apiset'

                        info_list.append(info)

                    except NoReverseMatch as error:
                        pass

                return Response(info_list)

        return APIRoot.as_view()
