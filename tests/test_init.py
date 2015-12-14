from datetime import datetime, timedelta
from django.test import TestCase
from faker import Faker
from mock import patch
from report.models import Report, ReportCol, Table
import re
from blub_report import settings
import json
from report.utils import register_app
faker = Faker()

class ReportRegistedTests(TestCase):
    def setUp(self):

        self.get_tables_patcher = patch("report.bigquery.get_tables")
        self.mock_get_tables = self.get_tables_patcher.start()
        faker_table_keys = [(datetime(2015, 10, 01) + timedelta(i)).strftime("%Y%m%d") for i in xrange(10)]
        fake_table_names = ["{}___{}".format(settings.TABLE_PREFIX, table_key) for table_key in faker_table_keys]
        self.mock_get_tables.return_value = fake_table_names

    def tearDown(self):
        self.get_tables_patcher.stop()


    def test_report_registed(self):
        register_app("blub_report")
        Report.objects.get(name=settings.REPORT_NAME)


    def test_col_registed(self):
        ##
        #  test col registed
        ##
        register_app("blub_report")

        report = Report.objects.get(name=settings.REPORT_NAME)

        report_dimensions = report.cols.filter(type='dimension')
        report_meterics = report.cols.filter(type='meteric')

        self.assertEqual(report_dimensions.count(), len(settings.REPORT_DIMENSIONS))
        self.assertEqual(report_meterics.count(), len(settings.REPORT_METERIC))

        for key, query, name in settings.REPORT_METERIC:
            col = ReportCol.objects.get(report=report, key=key, name=name)
            self.assertEqual(col.type, 'meteric')

        for key, query, name in settings.REPORT_DIMENSIONS:
            col = ReportCol.objects.get(report=report, key=key, name=name)
            self.assertEqual(col.type, 'dimension')

    def test_table_synced(self):
        register_app("blub_report")

        report = Report.objects.get(name=settings.REPORT_NAME)

        self.assertEqual(report.tables.count(), 10)

    def test_api_register(self):
        pass

class ReportQueryTest(TestCase):
    def setUp(self):

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

        register_app("blub_report")

        self.report = Report.objects.get(prefix=settings.TABLE_PREFIX)

    def tearDown(self):
        self.get_tables_patcher.stop()
        self.bigquery_query_patcher.stop()
        self.storage_upload_patcher.stop()


    def test_query(self):
        report_query = self.report.bigquery()
        with self.assertRaises(AssertionError) as error:
            report_query.execute()

        self.assertEqual(error.exception.message, 'need col')

        report_query.values(['location_lat', 'location_lng'])
        report_query.execute()
        self.assertEqual(re.sub("[\s\n]+", " ", report_query.querystr).strip(), "select meteric21 as location_lat,meteric22 as location_lng from iot.blub___20151001,iot.blub___20151002,iot.blub___20151003,iot.blub___20151004,iot.blub___20151005,iot.blub___20151006,iot.blub___20151007,iot.blub___20151008,iot.blub___20151009,iot.blub___20151010 where True ignore case")


    def test_table_upload(self):
        from cStringIO import StringIO

        input_file = StringIO()

        table0 = Table.objects.all()[0]
        table0.upload(input_file)

        self.assertEqual(self.mock_write_table.call_count, 1)
        self.assertEqual(self.mock_upload.call_count, 1)

    def test_quick_create_report(self):
        from datetime import datetime
        datas = [{"time": datetime(2015,11,26), "key": "test", "value": 123}]
        Report.quick_create("test_report", datas)
        report = Report.objects.get(name="test_report")
        self.assertEqual(report.cols.all().count(), 2)


        self.assertEqual(self.mock_write_table.call_count, 1)
        self.assertEqual(self.mock_upload.call_count, 1)


