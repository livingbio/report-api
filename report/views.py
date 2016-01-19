# -*- coding: utf-8 -*-
from rest_framework.views import APIView
from rest_framework.response import Response
from django.http import HttpResponse
from django.views.generic import View
from rest_framework.exceptions import Throttled
from django.core.urlresolvers import reverse
from django.utils.encoding import smart_unicode
import json


def projection(data):

    try:
        assert data['jobComplete']
    except:
        raise Throttled(5)

    result = {}
    result['rows'] = [[v['v'] for v in r['f']] for r in data.get('rows', [])]
    result['fields'] = [v['name'] for v in data['schema']['fields']]
    result['total'] = data['totalRows']
    result['pageToken'] = data.get('pageToken', None)

    return result


class BaseApiView(APIView):
    _report = []
    _base_filters = [
                        ("PageToken", "__pageToken", "page token for nex page","BG7AJWVXKAAQAAASAUIIBAEAAUNAICAKCAFCBMFOCU======", ""),
                        ("mappre function", "__mapper", "mapper js function", """function(v1, v2){return {"key":key, "value": value} }""", ""),
                        ("reduce function", "__reducer", "reduce js function", """function(key, values){return {"result": result}}""", ""), 
                      ]
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
        print id(self._bigquery)
        for name, key, desc, example, template in self._filters:
            value = request.query_params.get(key, False)
            if value and not key.startswith('__'):
                self.bigquery.filter([(key, template.format(value))])
        mapper = request.POST.get('__mapper', '').strip()
        reducer = request.POST.get('__reducer', '').strip()
        if mapper and reducer:
            query_template = '''
            select key, result  
            from reducer(
                select key, nest(value) as result
                from mapper(
                        select {col_query}
                        from {table_query}
                        where {filter_query}
                        {group_query}
                        {order_query}
                        ignore case
                    )
                group by key
            )   
            {limit_query}
            '''

            mapper = '''
            bigquery.defineFunction(
              'mapper', 
              %s,
              [{name: 'key', type: 'string'}, {name: 'value', type:'string'}],
              %s
            );

            ''' % (json.dumps([col.key for col in self.cols]), mapper)

            reducer = '''
            bigquery.defineFunction(
              'reducer', 
              ['key', 'result'],
              [{name: 'key', type: 'string'}, {name: 'result', type:'string'}],
              %s
            );
            ''' % (reducer)


            self.bigquery.query_template = query_template
            self.bigquery.udfs = [mapper, reducer]

        self.bigquery.values(self.cols)

    def after(self, request, result):
        return projection(result)

    def get(self, request, *args, **kwargs):
        self.before(request)
        pageToken = request.GET.get('__pageToken', None)
        result = self.bigquery.execute(pageToken=pageToken)
        result = self.after(request, result)
        return Response(result)

    def post(self, request, *args, **kwargs):
        return self.get(request, *args, **kwargs)


class ExportReportApi(BaseApiView):

    def __init__(self, *args, **kwargs):
        super(ExportReportApi, self).__init__(*args, **kwargs)
        self._cols = self.cols or list(self._report.cols.exclude(query__contains=r'('))


class RawQueryApi(BaseApiView):

    def __init__(self, query_template, *args, **kwargs):
        super(RawQueryApi, self).__init__(*args, **kwargs)
        self.bigquery.set_query_template(query_template)


class TimeReportApi(BaseApiView):
    _dimensions = []
    _meterics = []
    _default_filters = [
        ("Time interval", "__time", "時間區間(year|date|month|weekday)", "year", "")]

    def __init__(self, cols=[], default_interval='date', *args, **kwargs):
        super(TimeReportApi, self).__init__(*args, **kwargs)
        self._cols = cols

        self.default_interval = default_interval

    def before(self, request):
        time_iterval = request.GET.get("__time", self.default_interval)
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
        for data in result:
            data['url'] = reverse('report:group', kwargs={"group": data["name"]})
        return Response(result)

class ReportGroupView(APIView):
    def get(self, *args, **kwargs):
        group = kwargs.get('group')
        group = report_models.ReportGroup.objects.get(name=group)
        reports = report_models.Report.objects.filter(group=group)
        result = reports.values("name", "description", "prefix")
        for data in result:
            data['url'] = reverse('report:report', kwargs={"group": kwargs["group"], "report": data["prefix"]})
        return Response(result)

class ReportReportView(APIView):
    def get(self, *args, **kwargs):
        group = kwargs.get('group')
        report = kwargs.get('report')

        report = report_models.Report.objects.get(prefix=report)
        apis = report.apis.all()
        result = []
        for api in apis:
            api_info = {}
            api_info['name'] = api.name
            api_info['description'] = api.description
            view = api.view_class(report)
            api_info['filters'] = [{"name": smart_unicode(f[0]), "key":f[1], "description": smart_unicode(f[2]), "example": smart_unicode(f[3])} for f in view._filters]
            api_info['url'] = reverse('report:api', kwargs={"group": group, "report": report.prefix, "api": api.name})
            result.append(api_info)
        return Response(result)

    def post(self, request, *args, **kwargs):
        import csv
        group = kwargs.get('group')
        report = kwargs.get('report')
        datas = csv.DictReader(request.FILES.get("datas"))
        request.GET.get('report')
        report = report_models.Report.quick_create(report, datas)
        report.group = report_models.ReportGroup.objects.get(name=group)
        report.save()
        return Response({"status": "0"})

class ReportApiView(APIView):
    def __init__(self, *args, **kwargs):
        self.post = self.get
        super(ReportApiView, self).__init__(*args, **kwargs)


    def get(self, *args, **kwargs):
        group = kwargs.get('group')
        report = kwargs.get('report')
        api = kwargs.get('api')
        report = report_models.Report.objects.get(prefix=report)
        api = report_models.ReportApi.objects.get(name=api)
        view = api.view(report)
        return view(*args, **kwargs)


from forms import ReportForm

class Upload_ReportView(View):
    def get(self, request, *args, **kwargs):
        form = ReportForm()
        template = '''
            <form method="post" enctype="multipart/form-data">
            {}
            <input type="submit" value="Submit">
            </form>

        '''.format(form.as_p())
        return HttpResponse(template)
            

    def post(self, request, *args, **kwargs):
        form = ReportForm(request.POST, request.FILES)
        import csv
        group = form.data.get('group')
        report = form.data.get('name')
        description = form.data.get('description')
        datas = csv.DictReader(form.files.get("file"))
        request.GET.get('report')
        report = report_models.Report.quick_create(report, datas)
        report.group = report_models.ReportGroup.objects.get(name=group)
        report.description = description
        report.save()
        return HttpResponse("success")
