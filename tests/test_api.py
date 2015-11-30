#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright © 2015 lizongzhe 
#
# Distributed under terms of the MIT license.

from datetime import datetime, timedelta
from django.test import TestCase
from faker import Faker
from mock import patch
from report.models import Report, ReportCol, Table, ReportApi
import re
from django.test import Client
from django.core.urlresolvers import reverse
import logging
from blub_report import settings
import json
from django.core.urlresolvers import reverse

logger = logging.getLogger('test')
faker = Faker()

class ReportRegistedTests(TestCase):
    def setUp(self):
        self.client = Client()

        from report.utils import register_api

        self.get_tables_patcher = patch("report.bigquery.get_tables")
        self.mock_get_tables = self.get_tables_patcher.start()
        faker_table_keys = [(datetime(2015, 10, 01) + timedelta(i)).strftime("%Y%m%d") for i in xrange(10)]
        fake_table_names = ["{}___{}".format(settings.TABLE_PREFIX, table_key) for table_key in faker_table_keys]
        self.mock_get_tables.return_value = fake_table_names

        self.bigquery_query_patcher = patch("report.bigquery.query")
        self.mock_bigquery_query = self.bigquery_query_patcher.start()
        with open('tests/bq_success', 'r+') as f:
            self.mock_bigquery_query.return_value = json.loads(f.read())

        self.storage_upload_patcher = patch("report.storage.upload")
        self.mock_upload = self.storage_upload_patcher.start()
        self.mock_upload.return_value = ""

        self.bigquery_write_table_patcher = patch("report.bigquery.write_table")
        self.mock_write_table = self.bigquery_write_table_patcher.start()

        from report.utils import register_api
        register_api(settings)

        self.report = Report.objects.get(prefix=settings.TABLE_PREFIX)

    def test_api(self):
        resp = self.client.get(reverse('report:groups'))
        self.assertEqual(resp.status_code, 200)
        self.assertEqual([{"name":"iot","description":""}], json.loads(resp.content))

        resp = self.client.get(reverse('report:group', kwargs={"group":"iot"}))
        self.assertEqual(resp.status_code, 200)
        self.assertEqual([{"description":None,"name":u"智慧燈泡"}], json.loads(resp.content))

        resp = self.client.get(reverse('report:report', kwargs={"group":"iot", "report": "blub"}))
        self.assertEqual(resp.status_code, 200)
        

    def test_export_api(self):
        api = ReportApi.objects.create(
                    name="test",
                )
        api.save()
        resp = self.client.get(reverse('report:api', kwargs={"group":"iot", "report": "blub", "api": "test"}))
        self.mock_bigquery_query.assert_called_once_with(pageSize=100, pageToken=None, query="\n        select time as time,STRFTIME_UTC_USEC(time, '%Y-%m-%d') as date,STRFTIME_UTC_USEC(time, '%Y-%m') as month,STRFTIME_UTC_USEC(time, '%Y') as year,DAYOFWEEK(time) as weekday,dimension21 as location_city,dimension22 as machine_type,dimension23 as machine_class,dimension24 as machine_id,dimension25 as machine_brand,dimension26 as network_user,dimension27 as network_machine_name,dimension28 as network_group,dimension29 as network_ip,dimension30 as system_language,dimension31 as color_temperature,dimension32 as scenes\n \n        from iot.blub___20151001,iot.blub___20151002,iot.blub___20151003,iot.blub___20151004,iot.blub___20151005,iot.blub___20151006,iot.blub___20151007,iot.blub___20151008,iot.blub___20151009,iot.blub___20151010\n \n        where  True \n        \n        \n        \n        ignore case\n    ")

    def test_export_api_2(self):
        api = ReportApi.objects.create(
                    name="test",
                )
        api.cols = self.report.cols.filter(type='dimension')[:5]
        api.save()
        resp = self.client.get(reverse('report:api', kwargs={"group":"iot", "report": "blub", "api": "test"}))
        self.mock_bigquery_query.assert_called_once_with(pageSize=100, pageToken=None, query="\n        select time as time,STRFTIME_UTC_USEC(time, '%Y-%m-%d') as date,STRFTIME_UTC_USEC(time, '%Y-%m') as month,STRFTIME_UTC_USEC(time, '%Y') as year,DAYOFWEEK(time) as weekday\n \n        from iot.blub___20151001,iot.blub___20151002,iot.blub___20151003,iot.blub___20151004,iot.blub___20151005,iot.blub___20151006,iot.blub___20151007,iot.blub___20151008,iot.blub___20151009,iot.blub___20151010\n \n        where  True \n        \n        \n        \n        ignore case\n    ")

    

