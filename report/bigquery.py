# -*- coding: utf-8 -*-

from report import settings

import time

def raw_query(query, prefix=None, start_day=None, end_day=None):
    service = get_service()
    return service.query(query=query)


def write_table(table_name, gs_file, async=True, overwrite=True):
    service = get_service()
    fields = settings.BASIC_FIELDS
    job = service.write_table(table=table_name, dataset=settings.DATASET,
                              fields=fields, gspaths=[gs_file], overwrite=overwrite)
    if async:
        return job

    while True:
        time.sleep(5)
        status = service.job_status(job_id=job['jobReference']['jobId'])
        if status in ["RUNNING", "PENDING"]:
            print status
            continue
        else:
            print status
            break

def get_tables(dataset):
    service = get_service()
    for table in service.get_tables(dataset=dataset):
        yield table['tableReference']['tableId']


def has_table(dataset, table_name):
    tables = get_tables(dataset)
    return table_name in tables

def query(*args, **kwargs):
    service = get_service()
    return service.query(*args, **kwargs)

def create_job(query, udfs=[]):
    service = get_service()
    body = {}
    body['configuration'] = {}
    body['configuration']['query'] = {}
    body['configuration']['query']['query'] = query
    body['configuration']['query']['userDefinedFunctionResources'] = [{"inlineCode": code} for code in udfs]
    print query
    print '---'
    print body['configuration']['query']['userDefinedFunctionResources'][0]['inlineCode']
    print '---'
    print body['configuration']['query']['userDefinedFunctionResources'][1]['inlineCode']
    return service.jobs().insert(projectId=settings.PROJECT_ID, body=body).execute()['jobReference']['jobId']


def getJobResults(jobId, maxResults=None, pageToken=None):
    service = get_service()
    try:
        return service.jobs().getQueryResults(projectId=settings.PROJECT_ID, jobId=jobId, maxResults=maxResults, pageToken=pageToken).execute()
    except Exception as e:
        import pdb;pdb.set_trace()

def get_service():
    from report.gapis import bigquery
    from report.http import http

    service = bigquery.get_service(
        settings.REDIRECT_URI, settings.PROJECT_ID, http=http)
    return service


#write_table("test", "gs://dmp_track/bigquery/test_data", False)
