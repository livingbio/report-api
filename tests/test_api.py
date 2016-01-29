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
from report.models import Report, ReportCol, Table, ReportApi, ReportGroup
import re
from django.test import Client
from django.core.urlresolvers import reverse
import logging
from blub_report import settings
import json

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

        self.mock_create_job_patcher = patch("report.bigquery.create_job")
        self.mock_create_job = self.mock_create_job_patcher.start()
        self.mock_create_job.return_value = "create_job_genreator"

        self.mock_get_job_result_patcher = patch("report.bigquery.getJobResults")
        self.mock_get_job_result = self.mock_get_job_result_patcher.start()

        with open('tests/bq_success', 'r+') as f:
            self.mock_get_job_result.return_value = json.loads(f.read())


        from report.utils import register_app
        register_app("blub_report")

        self.report = Report.objects.get(prefix=settings.TABLE_PREFIX)

    def test_api(self):
        resp = self.client.get(reverse('report:groups'))
        self.assertEqual(resp.status_code, 200)
        self.assertEqual([{u'description': u'', u'name': u'iot', u'url': u'/report/iot'}, {u'description': u'\u5feb\u901f csv \u6a94\u6848\u4e0a\u50b3', u'name': u'quick_upload', u'url': u'/report/quick_upload'}], json.loads(resp.content))

        resp = self.client.get(reverse('report:group', kwargs={"group":"iot"}))
        self.assertEqual(resp.status_code, 200)
        self.assertEqual([{u'description': None, u'name': u'\u667a\u6167\u71c8\u6ce1', u'prefix': u'blub', u'url': u'/report/iot/blub'} ], json.loads(resp.content))

        resp = self.client.get(reverse('report:report', kwargs={"group":"iot", "report": "blub"}))
        self.assertEqual(resp.status_code, 200)
        data = '''
[{"url":"/report/iot/blub/export","description":"","name":"export","filters":[{"description":"page token for nex page","key":"__pageToken","name":"PageToken","example":"BG7AJWVXKAAQAAASAUIIBAEAAUNAICAKCAFCBMFOCU======"},{"description":"mapper js function","key":"__mapper","name":"mappre function","example":"function(v1, v2){return {\\"key\\":key, \\"value\\": value} }"},{"description":"reduce js function","key":"__reducer","name":"reduce function","example":"function(key, values){return {\\"result\\": result}}"},{"description":"\xe8\xb5\xb7\xe5\xa7\x8b\xe6\x99\x82\xe9\x96\x93","key":"st","name":"start_day","example":"2015-09-01"},{"description":"\xe7\xb5\x90\xe6\x9d\x9f\xe6\x99\x82\xe9\x96\x93","key":"ed","name":"end_day","example":"2015-10-01"}]},{"url":"/report/iot/blub/avg_power_frequency_by_time","description":"","name":"avg_power_frequency_by_time","filters":[{"description":"page token for nex page","key":"__pageToken","name":"PageToken","example":"BG7AJWVXKAAQAAASAUIIBAEAAUNAICAKCAFCBMFOCU======"},{"description":"mapper js function","key":"__mapper","name":"mappre function","example":"function(v1, v2){return {\\"key\\":key, \\"value\\": value} }"},{"description":"reduce js function","key":"__reducer","name":"reduce function","example":"function(key, values){return {\\"result\\": result}}"},{"description":"\xe6\x99\x82\xe9\x96\x93\xe5\x8d\x80\xe9\x96\x93(year|date|month|weekday)","key":"__time","name":"Time interval","example":"year"},{"description":"\xe8\xb5\xb7\xe5\xa7\x8b\xe6\x99\x82\xe9\x96\x93","key":"st","name":"start_day","example":"2015-09-01"},{"description":"\xe7\xb5\x90\xe6\x9d\x9f\xe6\x99\x82\xe9\x96\x93","key":"ed","name":"end_day","example":"2015-10-01"}]},{"url":"/report/iot/blub/machine_report","description":"","name":"machine_report","filters":[{"description":"page token for nex page","key":"__pageToken","name":"PageToken","example":"BG7AJWVXKAAQAAASAUIIBAEAAUNAICAKCAFCBMFOCU======"},{"description":"mapper js function","key":"__mapper","name":"mappre function","example":"function(v1, v2){return {\\"key\\":key, \\"value\\": value} }"},{"description":"reduce js function","key":"__reducer","name":"reduce function","example":"function(key, values){return {\\"result\\": result}}"},{"description":"\xe8\xb5\xb7\xe5\xa7\x8b\xe6\x99\x82\xe9\x96\x93","key":"st","name":"start_day","example":"2015-09-01"},{"description":"\xe7\xb5\x90\xe6\x9d\x9f\xe6\x99\x82\xe9\x96\x93","key":"ed","name":"end_day","example":"2015-10-01"}]},{"url":"/report/iot/blub/top10_color_temperature","description":"","name":"top10_color_temperature","filters":[{"description":"page token for nex page","key":"__pageToken","name":"PageToken","example":"BG7AJWVXKAAQAAASAUIIBAEAAUNAICAKCAFCBMFOCU======"},{"description":"mapper js function","key":"__mapper","name":"mappre function","example":"function(v1, v2){return {\\"key\\":key, \\"value\\": value} }"},{"description":"reduce js function","key":"__reducer","name":"reduce function","example":"function(key, values){return {\\"result\\": result}}"},{"description":"\xe8\xb5\xb7\xe5\xa7\x8b\xe6\x99\x82\xe9\x96\x93","key":"st","name":"start_day","example":"2015-09-01"},{"description":"\xe7\xb5\x90\xe6\x9d\x9f\xe6\x99\x82\xe9\x96\x93","key":"ed","name":"end_day","example":"2015-10-01"}]},{"url":"/report/iot/blub/anomaly_city_report","description":"","name":"anomaly_city_report","filters":[{"description":"page token for nex page","key":"__pageToken","name":"PageToken","example":"BG7AJWVXKAAQAAASAUIIBAEAAUNAICAKCAFCBMFOCU======"},{"description":"mapper js function","key":"__mapper","name":"mappre function","example":"function(v1, v2){return {\\"key\\":key, \\"value\\": value} }"},{"description":"reduce js function","key":"__reducer","name":"reduce function","example":"function(key, values){return {\\"result\\": result}}"},{"description":"\xe8\xb5\xb7\xe5\xa7\x8b\xe6\x99\x82\xe9\x96\x93","key":"st","name":"start_day","example":"2015-09-01"},{"description":"\xe7\xb5\x90\xe6\x9d\x9f\xe6\x99\x82\xe9\x96\x93","key":"ed","name":"end_day","example":"2015-10-01"}]}]
        '''
        self.assertEqual(json.loads(data), json.loads(resp.content))
        

    def test_export_api(self):
        api = ReportApi.objects.create(
                    name="test",
                )
        api.save()
        resp = self.client.get(reverse('report:api', kwargs={"group":"iot", "report": "blub", "api": "test"}))

        self.assertEqual({u'rows': [[u'1.447243682E9', u'2015-11-11'], [u'1.44725167E9', u'2015-11-11'], [u'1.447219137E9', u'2015-11-11'], [u'1.447238061E9', u'2015-11-11'], [u'1.447214164E9', u'2015-11-11'], [u'1.447222595E9', u'2015-11-11'], [u'1.447210338E9', u'2015-11-11'], [u'1.447263407E9', u'2015-11-11'], [u'1.44721687E9', u'2015-11-11'], [u'1.447255459E9', u'2015-11-11'], [u'1.447257306E9', u'2015-11-11'], [u'1.447255958E9', u'2015-11-11'], [u'1.447215459E9', u'2015-11-11'], [u'1.44725552E9', u'2015-11-11'], [u'1.447234063E9', u'2015-11-11'], [u'1.447218436E9', u'2015-11-11'], [u'1.447220416E9', u'2015-11-11'], [u'1.447273967E9', u'2015-11-11'], [u'1.447286016E9', u'2015-11-11'], [u'1.447228496E9', u'2015-11-11'], [u'1.447268432E9', u'2015-11-11'], [u'1.447288065E9', u'2015-11-12'], [u'1.447255551E9', u'2015-11-11'], [u'1.447224128E9', u'2015-11-11'], [u'1.447244883E9', u'2015-11-11'], [u'1.44720989E9', u'2015-11-11'], [u'1.44722551E9', u'2015-11-11'], [u'1.447256458E9', u'2015-11-11'], [u'1.447267325E9', u'2015-11-11'], [u'1.447238388E9', u'2015-11-11'], [u'1.44724825E9', u'2015-11-11'], [u'1.447237754E9', u'2015-11-11'], [u'1.447260593E9', u'2015-11-11'], [u'1.447256681E9', u'2015-11-11'], [u'1.447249276E9', u'2015-11-11'], [u'1.447283859E9', u'2015-11-11'], [u'1.447227478E9', u'2015-11-11'], [u'1.447248796E9', u'2015-11-11'], [u'1.447254108E9', u'2015-11-11'], [u'1.447217434E9', u'2015-11-11'], [u'1.447248664E9', u'2015-11-11'], [u'1.447216707E9', u'2015-11-11'], [u'1.447243556E9', u'2015-11-11'], [u'1.44728068E9', u'2015-11-11'], [u'1.447263608E9', u'2015-11-11'], [u'1.447239442E9', u'2015-11-11'], [u'1.447261449E9', u'2015-11-11'], [u'1.44728311E9', u'2015-11-11'], [u'1.447282135E9', u'2015-11-11'], [u'1.44724674E9', u'2015-11-11'], [u'1.447283991E9', u'2015-11-11'], [u'1.447245065E9', u'2015-11-11'], [u'1.447260975E9', u'2015-11-11'], [u'1.447251472E9', u'2015-11-11'], [u'1.44728828E9', u'2015-11-12'], [u'1.447211336E9', u'2015-11-11'], [u'1.447278501E9', u'2015-11-11'], [u'1.447288358E9', u'2015-11-12'], [u'1.447225933E9', u'2015-11-11'], [u'1.447237142E9', u'2015-11-11'], [u'1.447242408E9', u'2015-11-11'], [u'1.447209474E9', u'2015-11-11'], [u'1.447279962E9', u'2015-11-11'], [u'1.447246589E9', u'2015-11-11'], [u'1.447251217E9', u'2015-11-11'], [u'1.447218442E9', u'2015-11-11'], [u'1.447215E9', u'2015-11-11'], [u'1.447222473E9', u'2015-11-11'], [u'1.447245828E9', u'2015-11-11'], [u'1.447288094E9', u'2015-11-12'], [u'1.447255827E9', u'2015-11-11'], [u'1.447282325E9', u'2015-11-11'], [u'1.447219148E9', u'2015-11-11'], [u'1.447282498E9', u'2015-11-11'], [u'1.447275611E9', u'2015-11-11'], [u'1.447213267E9', u'2015-11-11'], [u'1.447257363E9', u'2015-11-11'], [u'1.447255544E9', u'2015-11-11'], [u'1.447230492E9', u'2015-11-11'], [u'1.447260729E9', u'2015-11-11'], [u'1.447284772E9', u'2015-11-11'], [u'1.447267057E9', u'2015-11-11'], [u'1.447264038E9', u'2015-11-11'], [u'1.447293602E9', u'2015-11-12'], [u'1.44723667E9', u'2015-11-11'], [u'1.447281593E9', u'2015-11-11'], [u'1.447261964E9', u'2015-11-11'], [u'1.44721097E9', u'2015-11-11'], [u'1.447271823E9', u'2015-11-11'], [u'1.447271972E9', u'2015-11-11'], [u'1.447277022E9', u'2015-11-11'], [u'1.447283854E9', u'2015-11-11'], [u'1.447223984E9', u'2015-11-11'], [u'1.447239028E9', u'2015-11-11'], [u'1.447285457E9', u'2015-11-11'], [u'1.447241622E9', u'2015-11-11'], [u'1.44727253E9', u'2015-11-11'], [u'1.44728642E9', u'2015-11-12'], [u'1.447234771E9', u'2015-11-11'], [u'1.44727932E9', u'2015-11-11']], u'fields': [u'time', u'date'], u'pageToken': u'BG65C5BSKEAQAAASAUIIBAEAAUNAICDECBSCBMFOCU======', u'total': u'10000'}, json.loads(resp.content))
        self.mock_bigquery_query.assert_called_once_with(pageSize=100, pageToken=None, query="\n        select time as time,dimension21 as location_city,dimension22 as machine_type,dimension23 as machine_class,dimension24 as machine_id,dimension25 as machine_brand,dimension26 as network_user,dimension27 as network_machine_name,dimension28 as network_group,dimension29 as network_ip,dimension30 as system_language,dimension31 as color_temperature,dimension32 as scenes,meteric21 as location_lat,meteric22 as location_lng,meteric23 as machine_boot_number,meteric24 as mcahine_total_run_time,metric22 as scenes_number\n \n        from iot.blub___20151001,iot.blub___20151002,iot.blub___20151003,iot.blub___20151004,iot.blub___20151005,iot.blub___20151006,iot.blub___20151007,iot.blub___20151008,iot.blub___20151009,iot.blub___20151010\n \n        where  True \n        \n        \n        \n        ignore case\n    ")

    def test_export_api_2(self):
        api = ReportApi.objects.create(
                    name="test",
                )
        api.cols = self.report.cols.filter(key__in=['time', 'year', 'weekday', 'month', 'date'])[:5]
        api.save()
        resp = self.client.get(reverse('report:api', kwargs={"group":"iot", "report": "blub", "api": "test"}))
        self.mock_bigquery_query.assert_called_once_with(pageSize=100, pageToken=None, query="\n        select time as time,STRFTIME_UTC_USEC(time, '%Y-%m-%d') as date,STRFTIME_UTC_USEC(time, '%Y-%m') as month,STRFTIME_UTC_USEC(time, '%Y') as year,DAYOFWEEK(time) as weekday\n \n        from iot.blub___20151001,iot.blub___20151002,iot.blub___20151003,iot.blub___20151004,iot.blub___20151005,iot.blub___20151006,iot.blub___20151007,iot.blub___20151008,iot.blub___20151009,iot.blub___20151010\n \n        where  True \n        \n        \n        \n        ignore case\n    ")


    def test_time_api(self):
        cols = self.report.cols.filter(key__in=['total'])
        api = ReportApi.objects.create(
                    name="test",
                    mode="TimeReportApi",
                )
        api.cols = cols
        api.save()

        resp = self.client.get(reverse('report:api', kwargs={"group":"iot", "report": "blub", "api": "test"}) + "?test=1")
        self.mock_bigquery_query.assert_called_once_with(pageSize=100, pageToken=None, query="\n        select count(time) as total,STRFTIME_UTC_USEC(time, '%Y-%m-%d') as date\n \n        from iot.blub___20151001,iot.blub___20151002,iot.blub___20151003,iot.blub___20151004,iot.blub___20151005,iot.blub___20151006,iot.blub___20151007,iot.blub___20151008,iot.blub___20151009,iot.blub___20151010\n \n        where  True \n        group by date\n \n        \n        \n        ignore case\n    ", udfs=[])

    def test_query(self):
        api = ReportApi.objects.create(
                    name="test",
                )
        api.cols = self.report.cols.filter(key__in=['time', 'year', 'weekday', 'month', 'date'])[:5]
        api.save()
        self.assertEqual(api.query(self.report).querystr, "\n        select time as time,STRFTIME_UTC_USEC(time, '%Y-%m-%d') as date,STRFTIME_UTC_USEC(time, '%Y-%m') as month,STRFTIME_UTC_USEC(time, '%Y') as year,DAYOFWEEK(time) as weekday\n \n        from iot.blub___20151001,iot.blub___20151002,iot.blub___20151003,iot.blub___20151004,iot.blub___20151005,iot.blub___20151006,iot.blub___20151007,iot.blub___20151008,iot.blub___20151009,iot.blub___20151010\n \n        where  True \n        \n        \n        \n        ignore case\n    ")



    def test_time_api(self):
        cols = self.report.cols.filter(key__in=['total'])
        api = ReportApi.objects.create(
                    name="test",
                    mode="TimeReportApi",
                )
        api.cols = cols
        api.save()
        

        resp = self.client.post(reverse('report:api', kwargs={"group":"iot", "report": "blub", "api": "test"}), {"__mapper": "function(total, date){return {'key': date, 'value': total}}", "__reducer": "function(key, values){return [key, sum(values)]}"})
        self.mock_create_job.assert_called_once_with(query="\n            select key, result  \n            from reducer(\n                select key, nest(value) as result\n                from mapper(\n                        select count(time) as total,STRFTIME_UTC_USEC(time, '%Y-%m-%d') as date\n \n                        from iot.blub___20151001,iot.blub___20151002,iot.blub___20151003,iot.blub___20151004,iot.blub___20151005,iot.blub___20151006,iot.blub___20151007,iot.blub___20151008,iot.blub___20151009,iot.blub___20151010\n \n                        where  True \n                        group by date\n \n                        \n                        ignore case\n                    )\n                group by key\n            )   \n            \n            ", udfs=[u'\n            bigquery.defineFunction(\n              \'mapper\', \n              ["total"],\n              [{name: \'key\', type: \'string\'}, {name: \'value\', type:\'string\'}],\n              function(total, date){return {\'key\': date, \'value\': total}}\n            );\n\n            ', u"\n            bigquery.defineFunction(\n              'reducer', \n              ['key', 'result'],\n              [{name: 'key', type: 'string'}, {name: 'result', type:'string'}],\n              function(key, values){return [key, sum(values)]}\n            );\n            "])
        self.mock_get_job_result.assert_called_once_with(jobId='create_job_genreator', maxResults=100, pageToken=None)

    def test_quick_upload_table(self):
        resp = self.client.get(reverse('report:quick_upload'))

        with open("tests/test_report.csv") as fp:
            group = ReportGroup.objects.get(name="iot").id
            url = reverse('report:quick_upload')
            resp = self.client.post(url, {"name": "test_report", "group": group, "file": fp})
        report = Report.objects.get(prefix="test_report")
        self.assertTrue(bool(report))
        self.assertEqual(self.mock_write_table.call_count, 1)


