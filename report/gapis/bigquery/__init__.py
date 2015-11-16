import uuid
from report.gapis import core
from apiclient.discovery import build
import schema

SCOPES = ["https://www.googleapis.com/auth/bigquery"]


def write_table(table, dataset, fields, gspaths, service, project_id, overwrite=False):
    jobs = service.jobs()
    result = jobs.insert(
        projectId=project_id,
        body={
            'projectId': project_id,
            'configuration': {
                'load': {
                    'sourceUris': gspaths,
                    'schema': {
                        'fields': fields
                    },
                    'destinationTable': {
                        'projectId': project_id,
                        'datasetId': dataset,
                        'tableId': table
                    },
                    'createDisposition': 'CREATE_IF_NEEDED',
                    'writeDisposition': 'WRITE_TRUNCATE' if overwrite else 'WRITE_APPEND',
                    'encoding': 'UTF-8',
                    'sourceFormat': 'NEWLINE_DELIMITED_JSON'
                }
            }
        }
    ).execute()

    return result


def job_status(job_id, service, project_id):
    jobs = service.jobs()
    result = jobs.get(projectId=project_id, jobId=job_id).execute()


def get_tables(dataset, service, project_id):
    result = service.tables().list(projectId=project_id, datasetId=dataset).execute().get('tables', [])
    return result


def stream_row_to_bigquery(table, dataset_id, rows, service, project_id, num_retries=5):
    insert_all_data = {
        'insertId': str(uuid.uuid4()),
        'rows': [{'json': row} for row in rows]
    }
    return service.tabledata().insertAll(
        projectId=project_id,
        datasetId=dataset_id,
        tableId=table,
        body=insert_all_data).execute(num_retries=num_retries)


def query(query, pageSize=100, pageToken=None, timeout=10000, num_retries=5, service=None, project_id=None):
    query_data = {
        'query': query,
        'timeoutMs': timeout,
        'maxResults': pageSize,
        'pageToken': pageToken
    }
    result = service.jobs().query(
        projectId=project_id,
        body=query_data
    ).execute(num_retries=num_retries)

    return result


def get_service(redirect_uri, project_id, http=None, client_secrets=None):
    from functools import partial
    assert http or client_secrets, "need http or client_secrets"
    if not http:
        http = core.build_http(client_secrets, SCOPES, redirect_uri)
    service = build('bigquery', 'v2', http=http)
    setattr(service, "write_table", partial(
        write_table, service=service, project_id=project_id))
    setattr(service, "job_status", partial(
        job_status, service=service, project_id=project_id))
    setattr(service, "stream_row_to_bigquery", partial(
        stream_row_to_bigquery, service=service, project_id=project_id))
    setattr(service, "query", partial(
        query, service=service, project_id=project_id))
    setattr(service, "get_tables", partial(
        get_tables, service=service, project_id=project_id))
    return service
