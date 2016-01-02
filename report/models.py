# Create your models here.
# -*- coding: utf-8 -*-
from django.db import models
from report import settings as report_settings
from django.core.validators import RegexValidator


class ReportQuery(object):
    # query report

    query_template = '''
        select {col_query}
        from {table_query}
        where {filter_query}
        {order_query}
        {group_query}
        {limit_query}
        ignore case
    '''

    def __init__(self, report, custom_filters=[], query_template=""):

        self._report = None
        self._cols = []
        self._tables = None
        self._groups = []
        self._limit = 0
        self._filters = []
        self._orders = []
        self.udfs = []
        self.custom_filters = []
        self._report = report
        # initial all table
        self.custom_filters = custom_filters
        self._tables = list(report.tables.all())
        if query_template.strip():
            self.query_template = query_template

    def set_query_template(self, query_template):
        self.query_template = query_template

    def filter(self, filters):
        for key, value in filters:
            report_filter = filter( lambda f:f.key==key, self.custom_filters)
            if report_filter:
                self._filters += [(k, report_filter[0].query(value))]
            else:
                self._filters += [(key, value)]
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
        order_cols = set([re.sub("^-", "", col) for col in self._orders])
        orders = set(self._orders)
        assert cols, 'need col'
        assert orders.issubset(cols), 'orders error'

        groups = []
        aggfunctions = [
                    "AVG","BIT_AND","BIT_OR",
                    "BIT_XOR","CORR","COUNT",
                    "COVAR_POP","COVAR_SAMP","EXACT_COUNT_DISTINCT",
                    "FIRST","GROUP_CONCAT","GROUP_CONCAT_UNQUOTED",
                    "LAST","MAX","MIN","NEST","NTH",
                    "QUANTILES","STDDEV","STDDEV_POP","STDDEV_SAMP",
                    "SUM","TOP","UNIQUE","VARIANCE","VAR_POP","VAR_SAMP",
                ]
        aggfunction_pattern = "({})\s*\(".format("|".join(aggfunctions))

        has_agg = False
        groups = []
        for col in self._cols:
            if re.search(aggfunction_pattern, col.query, re.I):
                has_agg = True
            else:
                groups.append(col.key)
        if not has_agg:
            groups = []

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
        if self.udfs:
            return self.query_defer(pageSize=pageSize, pageToken=pageToken)
        else:
            return self.query(pageSize=pageSize, pageToken=pageToken)
        
    def query(self, pageSize=100, pageToken=None):
        from report import bigquery
        from django.core.cache import cache
        import hashlib
        import json
        md5 = hashlib.md5()

        querystr = self.querystr
        print querystr

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

    def query_defer(self, pageSize=100, pageToken=None):
        from report import bigquery
        from django.core.cache import cache
        import hashlib
        import json
        md5 = hashlib.md5()

        querystr = self.querystr
        udfs = self.udfs

        md5.update(querystr)
        md5.update(str(pageSize))
        md5.update(str(pageToken))
        md5.update(json.dumps(self.udfs))
        key = md5.hexdigest()

        job_id = cache.get(key, False)

        if not job_id:
            job_id = bigquery.create_job(query=querystr, udfs=udfs)
            cache.set(key, False)

        print querystr
        try:
            return bigquery.getJobResults(jobId=job_id, maxResults=pageSize, pageToken=pageToken)
        except Exception as e:
            cache.set(key, False)
            raise Exception('job error')


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


    def get_query(self):
        return "{} as {}".format(self.query, self.key)

    class Meta:
        unique_together = ('report', 'key', 'type')



class Filter(models.Model):
    name = models.CharField(max_length=1024)
    key = models.CharField(max_length=1024)
    col = models.ForeignKey(ReportCol, null=True, blank=True)
    query_template = models.TextField()
    description = models.TextField()
    example = models.TextField()


    def query(self, value):
        return self.query_template.format(value)



class ReportApi(models.Model):
    name = models.CharField(max_length=1024)
    cols = models.ManyToManyField(ReportCol)
    custom_filters = models.ManyToManyField(Filter, null=True, blank=True)
    mode = models.CharField(max_length=1024, choices=[('TimeReportApi', 'TimeReportApi'), ('ExportReportApi', 'ExportReportApi')], default='ExportReportApi')
    live = models.BooleanField(default=True)
    description = models.TextField()
    query_template = models.TextField()

    def view_class(self, report):
        from .views import ReportApiView, TimeReportApi, ExportReportApi
        api_view = locals()[self.mode]
        return api_view(
                    report=report,
                    cols=list(self.cols.all()),
                    custom_filters=list(self.custom_filters.all().values_list('name', 'key', 'description', 'example', 'query_template'))
                )

    def view(self, report):
        return self.view_class(report).as_view()

    def query(self, report):
        query = report.bigquery(custom_filters=list(self.custom_filters.all()), query_template=self.query_template)

        cols = self.cols.all() or self.report.cols().all()
        cols = cols.filter(name__regex="^dimension\d+$|^meteric\d+$")
        query.values(list(cols))

        query.values(list(self.cols.all()))
        return query



class ReportGroup(models.Model):
    name = models.CharField(max_length=1024)
    description = models.TextField(max_length=1024)
    live = models.BooleanField(default=True)
    key = models.CharField(max_length=1024)


class Report(models.Model):
    dataset = report_settings.DATASET
    name = models.CharField(max_length=1024)
    prefix = models.CharField(max_length=255, unique=True, validators=[RegexValidator(regex=".*___.*", inverse_match=True), RegexValidator(regex="[a-zA-Z-0-9]+")])
    group = models.ForeignKey(ReportGroup, null=True, blank=True)
    apis = models.ManyToManyField(ReportApi, null=True, blank=True)
    live = models.BooleanField(default=True)
    description = models.TextField(null=True, blank=True)

    def register_api(self, name, api_info):
        cols = api_info.report.cols.filter(key__in=api_info._cols)
        filters = []
        api = ReportApi.objects.get_or_create(
                    name=name,
                    mode=api_info.__class__.__name__
                )[0]
        api.custom_filters = []
        for name, key, description, example, query_template in api_info._custom_filters:
            f = Filter.objects.get_or_create(
                        name= name,
                        key= key,
                        description= description,
                        example= example,
                        query_template= query_template,
                    )
            api.custom_filters.add(f[0])
        api.cols = cols
        api.save()
        self.apis.add(api)


    def __repr__(self):
        return self.name.encode('utf-8')

    def __str__(self):
        return self.name.encode('utf-8')

    def bigquery(self, custom_filters=[], query_template=""):
        return ReportQuery(self, custom_filters=custom_filters, query_template=query_template)

    def create_table(self, key, datas, replace=False):
        table_info = Table.objects.get_or_create(report=self, key=key)
        table_info[0].upload(datas, replace=replace)

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
        if getattr(self, 'cache', None):
            return self.cache
        else:
            self.cache = dict(self.cols.exclude(query__contains=r'(').values_list('key', 'query'))
            return self.cache
        #return dict(self.cols.exclude(query__contains=r'(').values_list('key', 'query'))

    def data_transform(self, data):
        from django.utils import dateparse
        from report.gapis.bigquery import utils as bigquery_utils
        from datetime import datetime


        output = {}
        for key, value in data.items():
            if self.key_mapping.has_key(key):
                if self.key_mapping[key].startswith('meteric'):
                    try:
                        output[self.key_mapping[key]] = value and float(value) or 0
                    except:
                        output[self.key_mapping[key]] = 0
                elif self.key_mapping[key].startswith('dimension'):
                    try:
                        output[self.key_mapping[key]] = unicode(value)
                    except:
                        output[self.key_mapping[key]] = str(value)
                else:
                    output[self.key_mapping[key]] = value
                

        ## trans time into bigquery time
        if output.has_key('time'):
            if isinstance(output['time'], basestring):
                t = output['time']
                output['time'] = dateparse.parse_datetime(output['time']) or datetime.strptime(t, '%Y-%m-%d')
            output['time'] = bigquery_utils.to_utctimestamp(output['time'])

        return output

    @classmethod
    def quick_create(cls, report_name, datas):
        if isinstance(datas, list):
            datas = iter(datas)
        import itertools
        report = Report.objects.create(
                    name=report_name,
                    prefix=report_name,
                )

        data = next(datas, {})
        ## create cols
        dimension_idx = 1
        meteric_idx = 1
        for key in data.keys():
            if key in ['lat', 'lng', 'time', 'type']:
                continue
            try:
                float(data[key])
                ReportMeteric.objects.get_or_create(
                    type='meteric',
                    report=report,
                    key="meteric{}".format(meteric_idx)
                )
                meteric_idx+=1
            except:
                ReportDimension.objects.get_or_create(
                    type='dimension',
                    report=report,
                    key="dimension{}".format(meteric_idx)
                )
                meteric_idx+=1

        report.create_table("main", itertools.chain([data], datas))
        return report

        



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

    def upload(self, datas, replace=True):
        print 'upload start'
        from report import storage, bigquery
        from tempfile import TemporaryFile
        import json
        if not replace and self.report.has_table(self):
            return

        tmpfile = TemporaryFile()
        count = 0
        for line in datas:
            if count % 100 == 0:
                print count
            count += 1
            if isinstance(line, basestring):
                data = json.loads(line)
            else:
                data = line
            data = self.report.data_transform(data)

            tmpfile.write(json.dumps(data) + "\n")
        print 'write success'

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
