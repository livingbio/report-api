# -*- coding: utf-8 -*-
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.exceptions import Throttled


def projection(data):

    try:
        assert data['jobComplete']
    except:
        raise Throttled(5)

    result = {}
    result['rows'] = [[v['v'] for v in r['f']] for r in data['rows']]
    result['fields'] = [v['name'] for v in data['schema']['fields']]
    result['total'] = data['totalRows']
    result['pageToken'] = data.get('pageToken', None)

    return result


class BaseApiView(APIView):
    _report = []
    _base_filters = [("PageToken", "__pageToken", "page token for nex page",
                      "BG7AJWVXKAAQAAASAUIIBAEAAUNAICAKCAFCBMFOCU======", "")]
    _default_filters = []
    _custom_filters = []
    _http_method_names = ["get", "post"]
    _cols = []
    _bigquery = None

    def __init__(self, report, custom_filters=[], desc="", cols=[], **kwargs):
        super(BaseApiView, self).__init__(
            report=report, custom_filters=custom_filters, **kwargs)
        self._report = report
        self._custom_filters = custom_filters
        self._desc = desc or "{} data list".format(
            self._report.name.encode('utf-8'))
        self._cols = cols

    @property
    def filter_schema(self):
        return [{"name": name, "key": key, "desc": desc, "example": example} for name, key, desc, example, template in self._filters]

    @property
    def _filters(self):
        return self._base_filters + self._default_filters + self._custom_filters

    def as_view(self, *args, **kwargs):
        def view(request, *args, **kwargs):
            return self.dispatch(request, *args, **kwargs)
        return view

    @property
    def cols(self):
        return self._cols

    @property
    def desc(self):
        return self._desc

    @property
    def bigquery(self):
        return self._bigquery

    def before(self, request):
        self._bigquery = self._report.bigquery()
        for name, key, desc, example, template in self._filters:
            value = request.REQUEST.get(key, False)
            if value and not key.startswith('__'):
                self.bigquery.filter([(key, template.format(value))])
        self.bigquery.values(self.cols)

    def after(self, request, result):
        return projection(result)

    def get(self, request, *args, **kwargs):
        self.before(request)
        pageToken = request.REQUEST.get('__pageToken', None)
        result = self.bigquery.execute(pageToken=pageToken)
        result = self.after(request, result)
        return Response(result)


class ExportReportApi(BaseApiView):

    def __init__(self, *args, **kwargs):
        super(ExportReportApi, self).__init__(*args, **kwargs)
        self._cols = self.cols or list(self._report.cols.filter(type='dimension'))


class RawQueryApi(BaseApiView):

    def __init__(self, query_template, *args, **kwargs):
        super(RawQueryApi, self).__init__(*args, **kwargs)
        self.bigquery.set_query_template(query_template)


class TimeReportApi(BaseApiView):
    _dimensions = []
    _meterics = []
    _default_filters = [
        ("Time interval", "__time", "時間區間(year|date|month|weekday)", "year", "")]

    def __init__(self, meterics=[], default_interval='date', *args, **kwargs):
        super(TimeReportApi, self).__init__(*args, **kwargs)
        self._cols = list(self._report.cols.filter(key__in=meterics))

        self.default_interval = default_interval

    def before(self, request):
        time_iterval = request.REQUEST.get("__time", self.default_interval)
        dimension = self._report.cols.get(key=time_iterval)
        super(TimeReportApi, self).before(request)
        self.bigquery.values(self._cols + [dimension])

    @property
    def desc(self):
        col_names = [col.name.encode('utf-8') for col in self._cols]
        return "{} by 時間(date|month|year)".format(",".join(col_names))



from . import models as report_models

class ReportRootView(APIView):
    def get(self, *args, **kwargs):
        groups = report_models.ReportGroup.objects.all()
        result = groups.values('name', 'description')
        return Response(result)

class ReportGroupView(APIView):
    def get(self, *args, **kwargs):
        group = kwargs.get('group')

        group = report_models.ReportGroup.objects.get(name=group)
        reports = report_models.Report.objects.filter(group=group)
        result = reports.values("name", "description")
        return Response(result)

class ReportReportView(APIView):
    def get(self, *args, **kwargs):
        group = kwargs.get('group')
        report = kwargs.get('report')

        report = report_models.Report.objects.get(prefix=report)
        apis = report.apis.all()
        result = apis.values("name", "description")
        return Response(result)

class ReportApiView(APIView):
    def get(self, *args, **kwargs):
        group = kwargs.get('group')
        report = kwargs.get('report')
        api = kwargs.get('api')
        report = report_models.Report.objects.get(prefix=report)
        api = report_models.ReportApi.objects.get(name=api)
        view = api.view(report)
        return view(*args, **kwargs)
