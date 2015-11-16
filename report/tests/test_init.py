from datetime import datetime, timedelta
from django.test import TestCase
from faker import Faker
from mock import patch
from report.models import Report, ReportCol, Table
import re

faker = Faker()

class ReportRegistedTests(TestCase):
    def setUp(self):
        from report.tests import settings

        self.get_tables_patcher = patch("report.bigquery.get_tables")
        self.mock_get_tables = self.get_tables_patcher.start()
        faker_table_keys = [(datetime(2015, 10, 01) + timedelta(i)).strftime("%Y%m%d") for i in xrange(10)]
        fake_table_names = ["{}___{}".format(settings.TABLE_PREFIX, table_key) for table_key in faker_table_keys]
        self.mock_get_tables.return_value = fake_table_names

    def tearDown(self):
        self.get_tables_patcher.stop()


    def test_report_registed(self):
        from report.tests import settings
        from report.utils import register_api
        register_api(settings)
        Report.objects.get(name=settings.REPORT_NAME)


    def test_col_registed(self):
        ##
        #  test col registed
        ##
        from report.tests import settings
        from report.utils import register_api
        register_api(settings)

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
        from report.tests import settings
        from report.utils import register_api
        register_api(settings)

        report = Report.objects.get(name=settings.REPORT_NAME)

        self.assertEqual(report.tables.count(), 10)

class ReportQueryTest(TestCase):
    def setUp(self):
        from report.tests import settings
        from report.utils import register_api

        self.get_tables_patcher = patch("report.bigquery.get_tables")
        self.mock_get_tables = self.get_tables_patcher.start()
        faker_table_keys = [(datetime(2015, 10, 01) + timedelta(i)).strftime("%Y%m%d") for i in xrange(10)]
        fake_table_names = ["{}___{}".format(settings.TABLE_PREFIX, table_key) for table_key in faker_table_keys]
        self.mock_get_tables.return_value = fake_table_names

        self.bigquery_query_patcher = patch("report.bigquery.query")
        self.mock_bigquery_query = self.bigquery_query_patcher.start()
        self.mock_bigquery_query.return_value = ""

        self.storage_upload_patcher = patch("report.storage.upload")
        self.mock_upload = self.storage_upload_patcher.start()
        self.mock_upload.return_value = ""

        self.bigquery_write_table_patcher = patch("report.bigquery.write_table")
        self.mock_write_table = self.bigquery_write_table_patcher.start()

        from report.tests import settings
        from report.utils import register_api
        register_api(settings)

        self.report = Report.objects.get(prefix=settings.TABLE_PREFIX)

    def tearDown(self):
        self.get_tables_patcher.stop()
        self.bigquery_query_patcher.stop()
        self.storage_upload_patcher.stop()


    def test_query(self):
        self.mock_bigquery_query.return_value = ''
        report_query = self.report.bigquery()
        with self.assertRaises(AssertionError) as error:
            report_query.execute()

        self.assertEqual(error.exception.message, 'need col')

        report_query.values(['longitude', 'latitude'])
        report_query.execute()
        self.assertEqual(re.sub("[\s\n]+", " ", report_query.querystr).strip(), "select dimension1 as longitude,dimension2 as latitude from earthquake___20151001,earthquake___20151002,earthquake___20151003,earthquake___20151004,earthquake___20151005,earthquake___20151006,earthquake___20151007,earthquake___20151008,earthquake___20151009,earthquake___20151010 where True ignore case")


    def test_table_upload(self):
        from cStringIO import StringIO

        input_file = StringIO()

        table0 = Table.objects.all()[0]
        table0.upload(input_file)

        self.assertEqual(self.mock_write_table.call_count, 1)
        self.assertEqual(self.mock_upload.call_count, 1)
