#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright © 2015 george
#
# Distributed under terms of the MIT license.
# bigquery sample method


import uuid
from report.gapis import core
from apiclient.discovery import build
from apiclient.http import MediaIoBaseUpload
import schema
import magic
import json
from apiclient.errors import HttpError
import httplib2
import sys

SCOPES = ["https://www.googleapis.com/auth/devstorage.read_write",
          "https://www.googleapis.com/auth/devstorage.read_only"]
RETRYABLE_ERRORS = (httplib2.HttpLib2Error, IOError)


def print_with_carriage_return(s):
    sys.stdout.write('\r' + s)
    sys.stdout.flush()


def upload(upload_file, gspath, service, project_id):
    mime_type = magic.from_buffer(upload_file.read(1024), mime=True)
    upload_file.seek(0)
    bucket_name, object_name = gspath[5:].split('/', 1)
    assert bucket_name and object_name

    print('Building upload request...')

    media = MediaIoBaseUpload(upload_file, mime_type, resumable=True)

    request = service.objects().insert(bucket=bucket_name, name=object_name,
                                       media_body=media)

    print('Uploading file: %s to bucket: %s object: %s ' % ("file_name", bucket_name,
                                                            object_name))
    response = None
    progressless_iters = 0
    while response is None:
        error = None
        try:
            progress, response = request.next_chunk()
            if progress:
                print_with_carriage_return(
                    'Upload %d%%' % (100 * progress.progress()))
        except HttpError as err:
            error = err
            if err.resp.status < 500:
                raise
        except RETRYABLE_ERRORS as err:
            error = err

        if error:
            progressless_iters += 1
            #handle_progressless_iter(error, progressless_iters)
        else:
            progressless_iters = 0

    print('\nUpload complete!')


def download(path, service, project_id):
    pass


def get_service(client_secrets, redirect_uri, project_id, http=None):
    from functools import partial
    if not http:
        http = core.build_http(client_secrets, SCOPES, redirect_uri)
    service = build('storage', 'v1', http=http)
    setattr(service, "upload", partial(
        upload, service=service, project_id=project_id))
    setattr(service, "download", partial(
        download, service=service, project_id=project_id))
    return service
