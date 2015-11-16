from report import settings
import os


def upload(fp, name):
    gspath = os.path.join(settings.BQ_LOG_PATH, name)
    service = get_service()
    service.upload(fp, gspath)
    return gspath


def get_service():
    from report.gapis import storage
    from http import http

    service = storage.get_service(
        settings.REDIRECT_URI, settings.PROJECT_ID, http=http)
    return service


#write_table("test", "gs://dmp_track/bigquery/test_data", False)
