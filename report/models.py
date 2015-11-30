# Create your models here.
# -*- coding: utf-8 -*-
from django.db import models
from report import settings as report_settings
from django.core.validators import RegexValidator


class ReportQuery(object):
    # query report
    _report = None
    _cols = []
    _tables = None
    _groups = []
    _limit = 0
    _filters = []
    _orders = []
    query_template = '''
        select {col_query}
        from {table_query}
        where {filter_query}
        {group_query}
        {order_query}
        {limit_query}
        ignore case
    '''

    def __init__(self, report, query_template=""):
        self._report = report
        # initial all table
        self._tables = list(report.tables.all())
        if query_template:
            self.query_template = query_template

    def set_query_template(self, query_template):
        self.query_template = query_template

    def filter(self, filters):
        self._filters += filters
        return self

    def values(self, cols):
        assert len(cols) == len(set(cols)), 'col duplicate'
        self._cols = map(lambda col: col if isinstance(col, ReportCol) else self._report.cols.get(key=col), cols)
        return self

    def orderby(self, cols):
        self._orders = cols
        return self

    def limit(self, limit):
        self._limit = limit
        return self

    # select from where group limit
    @property
    def querystr(self):
        import re
        cols = set([col.key for col in self._cols])
        meteric_cols = set([col.key for col in self._cols if col.type == 'meteric'])
        dimension_cols = set([col.key for col in self._cols if col.type == 'dimension'])
        # filter_cols = set([f[0] for f in self._filters])
        order_cols = set([re.sub("^-", "", col) for col in self._orders])
        orders = set(self._orders)
        # assert filter_cols.issubset(cols), 'filter error'
        assert cols, 'need col'
        assert orders.issubset(cols), 'orders error'

        groups = []
        if meteric_cols and dimension_cols:
            groups = dimension_cols

        # select query
        col_query = ",".join([col.get_query() for col in self._cols]) + "\n "
        # from query
        table_query = ",".join(["{}.{}".format(table.report.dataset, table.table_name) for table in self._tables]) + "\n "

        if self._filters:
            filter_query = " and ".join([f[1] for f in self._filters]) + "\n "
        else:
            filter_query = " True "

        if groups:
            group_query = "group by " + ",".join(groups) + "\n "
        else:
            group_query = ""

        if order_cols:
            orders = []
            for order in self._orders:
                if re.match("^-", order):
                    orders.append(re.sub(order, "^-", "") + " desc ")
                else:
                    orders.append(order)

            order_query = "order by " + ",".join(orders) + "\n "
        else:
            order_query = ""

        if self._limit:
            limit_query = "limit {} ".format(self._limit)
        else:
            limit_query = ""

        query = self.query_template.format(
            col_query=col_query,
            table_query=table_query,
            filter_query=filter_query,
            group_query=group_query,
            order_query=order_query,
            limit_query=limit_query
        )
        return query

    def execute(self, pageSize=100, pageToken=None):
        from report import bigquery
        from django.core.cache import cache
        import hashlib
        md5 = hashlib.md5()

        querystr = self.querystr

        md5.update(querystr)
        md5.update(str(pageSize))
        md5.update(str(pageToken))
        key = md5.hexdigest()

        value = cache.get(key, False)

        if not value:
            value = bigquery.query(
                query=querystr, pageSize=pageSize, pageToken=pageToken)
            if value.get('jobComplete', False):
                cache.set(key, value)
        return value


REPORT_KEY_TYPES = (
    ('meteric', 'meteric'),
    ('dimension', 'dimension')
)



class ReportCol(models.Model):
    name = models.CharField(max_length=1024)
    key = models.CharField(max_length=1024)
    report = models.ForeignKey("report.Report", related_name="cols", null=True, blank=True)

    type = models.CharField(max_length=100, choices=REPORT_KEY_TYPES)
    query = models.CharField(max_length=1024)

    def filter(self, operation, value):
        query = self.operation_mapping[operation].format(value)
        return (self.key, query)

    def get_query(self):
        return "{} as {}".format(self.query, self.key)

    class Meta:
        unique_together = ('report', 'key', 'type')



class Filter(models.Model):
    name = models.CharField(max_length=1024)
    key = models.CharField(max_length=1024)
    col = models.ForeignKey(ReportCol)
    query = models.TextField()
    description = models.TextField()


class ReportApi(models.Model):
    name = models.CharField(max_length=1024)
    cols = models.ManyToManyField(ReportCol)
    custom_filter = models.ManyToManyField(Filter, null=True, blank=True)
    mode = models.CharField(max_length=1024, choices=[('TimeReportApi', 'TimeReportApi'), ('ExportReportApi', 'ExportReportApi')], default='ExportReportApi')
    live = models.BooleanField(default=True)
    description = models.TextField()

    def view(self, report):
        from .views import ReportApiView, ExportReportApi
        api_view = locals()[self.mode]
        return api_view(
                    report=report,
                    cols=self.cols.all(),
                    custom_filter=self.custom_filter
                ).as_view()

class ReportGroup(models.Model):
    name = models.CharField(max_length=1024)
    description = models.TextField(max_length=1024)
    live = models.BooleanField(default=True)

    def urls(self):
        pass

class Report(models.Model):
    dataset = report_settings.DATASET
    name = models.CharField(max_length=1024)
    prefix = models.CharField(max_length=255, unique=True, validators=[RegexValidator(regex=".*___.*", inverse_match=True), RegexValidator(regex="[a-zA-Z-0-9]+")])
    group = models.ForeignKey(ReportGroup, null=True, blank=True)
    apis = models.ManyToManyField(ReportApi, null=True, blank=True)
    live = models.BooleanField(default=True)
    description = models.TextField(null=True, blank=True)

    def __repr__(self):
        return self.name.encode('utf-8')

    def __str__(self):
        return self.name.encode('utf-8')

    def bigquery(self):
        return ReportQuery(self)

    def create_table(self, key, input_file, replace=False):
        table_info = Table.objects.get_or_create(report=self, key=key)
        table_info[0].upload(input_file, replace=replace)

    def has_table(self, table):
        from report import bigquery
        return bigquery.has_table(self.dataset, table.key)

    def sync_tables(self):
        from report import bigquery
        table_names = bigquery.get_tables(self.dataset)
        for table_name in table_names:
            try:
                prefix, table_key = table_name.split("___")
                assert self.prefix == prefix
                Table.objects.get_or_create(report=self, key=table_key)
            except Exception as e:
                pass

    @property
    def key_mapping(self):
        return dict(self.cols.exclude(query__contains=r'(').values_list('key', 'query'))

    def data_transform(self, data):
        from django.utils import dateparse
        from report.gapis.bigquery import utils as bigquery_utils

        output = {}
        for key, value in data.items():
            if self.key_mapping.has_key(key):
                output[self.key_mapping[key]] = value

        ## trans time into bigquery time
        if output.has_key('time'):
            if isinstance(output['time'], basestring):
                output['time'] = dateparse.parse_datetime(output['time'])
            output['time'] = bigquery_utils.to_utctimestamp(output['time'])

        return output


class ReportTag(models.Model):
    name = models.CharField(max_length=1024)
    description = models.TextField(max_length=1024)
    apis = models.ManyToManyField(Report)



class Table(models.Model):
    report = models.ForeignKey(Report, related_name="tables")
    # table name in bigquery
    key = models.CharField(max_length=1024, unique=True, validators=[RegexValidator("[a-zA-Z-0-9]*")])
    #day = models.DateField(null=True, blank=True)

    @property
    def name(self):
        return self.report.dataset + "." + self.table_name

    @property
    def filename(self):
        return "{}.json".format(self.name)

    @property
    def table_name(self):
        return "{}___{}".format(self.report.prefix, self.key)

    def upload(self, input_file, replace=True):
        from report import storage, bigquery
        from tempfile import TemporaryFile
        import json
        if not replace and self.report.has_table(self):
            return

        tmpfile = TemporaryFile()
        for line in input_file:
            data = json.loads(line)
            data = self.report.data_transform(data)
            tmpfile.write(json.dumps(data) + "\n")

        gs_path = storage.upload(tmpfile, self.filename)
        result = bigquery.write_table(self.table_name, gs_path, async=False)
        return result



class MetericManager(models.Manager):

    def get_queryset(self):
        return super(MetericManager, self).get_queryset().filter(
            type='meteric')


class DimensionManager(models.Manager):

    def get_queryset(self):
        return super(DimensionManager, self).get_queryset().filter(
            type='dimension')


class ReportDimension(ReportCol):
    operation_mapping = {
        "startswith": "REGEXP_MATCH({key}, '^{value}')",
        "endswith": "REGEXP_MATCH({key}, '{value}$')",
        "contains": "{key} contains '{value}'",
        "regex": "REGEXP_MATCH({key}, '{value}')",
        "not_startswith": "not REGEXP_MATCH({key}, '^{value}')",
        "not_endswith": "not REGEXP_MATCH({key}, '{value}$')",
        "not_contains": "not {key} contains '{value}'",
        "not_regex": "not REGEXP_MATCH({key}, '{value}')",
    }
    objects = DimensionManager()

    class Meta:
        proxy = True
        verbose_name = u'dimension'


# numbic
class ReportMeteric(ReportCol):
    operation_mapping = {
        ">": "{key} > {value}",
        "<": "{key} < {value}",
        ">=": "{key} >= {value}",
        "<=": "{key} <= {value}",
        "=": "{key} = {value}",
        "eq": "{key} = '{value}'",
    }

    objects = MetericManager()

    class Meta:
        proxy = True
        verbose_name = u'meteric'
