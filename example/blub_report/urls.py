# -*- encoding=utf8 -*-
from django.shortcuts import render
from report.views import ExportReportApi, TimeReportApi
from report.models import Report
from blub_report import settings
from report.utils import get_current_report

blub_report = get_current_report(__name__)

blub_report.register_api(
    'export',
    ExportReportApi(
        report=blub_report,
        custom_filters=[
            ("start_day", "st", "起始時間", "2015-09-01", "time > timestamp('{}')"),
            ("end_day", "ed", "結束時間", "2015-10-01", "time < timestamp('{}')"),
        ],
    )
)

blub_report.register_api(
    'avg_power_frequency_by_time',
    TimeReportApi(
        report=blub_report,
        cols=["avg_power_frequency"],
        custom_filters=[
            ("start_day", "st", "起始時間", "2015-09-01", "time > timestamp('{}')"),
            ("end_day", "ed", "結束時間", "2015-10-01", "time < timestamp('{}')"),
        ],
    )
)


blub_report.register_api(
    'machine_report',
    ExportReportApi(
        report=blub_report,
        cols=['machine_class', 'total'],
        custom_filters=[
            ("start_day", "st", "起始時間", "2015-09-01", "time > timestamp('{}')"),
            ("end_day", "ed", "結束時間", "2015-10-01", "time < timestamp('{}')"),
        ],
    )
)


blub_report.register_api(
    'top10_color_temperature',
    ExportReportApi(
        report=blub_report,
        cols=['color_temperature', 'total'],
        custom_filters=[
            ("start_day", "st", "起始時間", "2015-09-01", "time > timestamp('{}')"),
            ("end_day", "ed", "結束時間", "2015-10-01", "time < timestamp('{}')"),
        ],
        order=["-total"],
        limit=10,
    )
)


blub_report.register_api(
    'anomaly_city_report',
    ExportReportApi(
        report=blub_report,
        cols=['location_city', 'total'],
        custom_filters=[
            ("start_day", "st", "起始時間", "2015-09-01", "time > timestamp('{}')"),
            ("end_day", "ed", "結束時間", "2015-10-01", "time < timestamp('{}')"),
        ],
    )
)






