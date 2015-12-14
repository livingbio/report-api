#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright © 2015 george
#
# Distributed under terms of the MIT license.
from datetime import datetime
import random
from report import storage, bigquery
import time
from report.models import Report, Table
from django.conf import settings


def fake_bulb_data(day, pixel_type="random"):
    from faker import Factory
    FAKE = Factory.create()

    data = {
        "type": random.choice(["test", "test2"]),
        "dimension1": FAKE.sentence()
    }
    return data


def parse_date(date_str):
    from datetime import date
    if isinstance(date_str, date):
        return date_str
    else:
        return datetime.strptime(date_str, '%Y-%m-%d')


def flat_dict(data):
    keys = data.keys()
    result = {}
    for key in keys:
        if isinstance(data[key], dict):
            for key1, value in flat_dict(data[key]).items():
                result["{}.{}".format(key, key1)] = value
        else:
            result[key] = data[key]

    return result


def register_app(app_name=None):
    if app_name:
        app_names = [app_name]
    else:
        app_names = settings.REPORT_APIS

    for app_name in app_names:
        print 'register', app_name
        app = __import__('%s.settings' % app_name)

        register_api(app.settings)

        apis = __import__('%s.urls' % app_name)


def register_api(api_settings):
    from report.models import Report, ReportMeteric, ReportDimension, ReportGroup
    group = ReportGroup.objects.get_or_create(name=api_settings.GROUP)[0]
    report, _ = Report.objects.get_or_create(
        name=api_settings.REPORT_NAME,
        prefix=api_settings.TABLE_PREFIX,
        group=group,
    )

    # table, _ = Table.objects.get_or_create(
    #     report=report,
    #     key=api_settings.TABLE_PREFIX
    # )

    for key, query, name in api_settings.REPORT_DIMENSIONS:
        obj, _ = ReportDimension.objects.get_or_create(
            type="dimension",
            report=report,
            key=key
        )
        obj.name = name
        obj.query = query
        obj.save()

    for key, query, name in api_settings.REPORT_METERIC:
        obj, _ = ReportMeteric.objects.get_or_create(
            type='meteric',
            report=report,
            key=key
        )
        obj.name = name
        obj.query = query
        obj.save()

    report.sync_tables()


def upload(file_or_filename, name, day):
    ## quick upload file

    if isinstance(file_or_filename, str):
        file_or_filename = open(file_or_filename)

    assert isinstance(file_or_filename, file)
    day = parse_date(day)
    report = Report.objects.get(prefix=name)
    return report.create_table(day.strftime("%Y%m%d"), file_or_filename)


def get_current_report(name):
    from report.models import Report
    report_name = name.split('.')[-2]
    report_prefix = __import__('{}'.format(report_name)).settings.TABLE_PREFIX
    
    return Report.objects.get(prefix=report_prefix)
