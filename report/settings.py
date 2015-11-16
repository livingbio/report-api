import os
BASE_DIR = os.path.dirname(os.path.abspath(__file__))



REST_FRAMEWORK = {
    'DEFAULT_RENDERER_CLASSES': (
        'rest_framework.renderers.JSONRenderer',
        'rest_framework.renderers.BrowsableAPIRenderer',
    )
}


BASIC_FIELDS_DEV = dict(
    [
        ("type", str),
        ("time", "timestamp"),
        ("lat", float),
        ("lng", float),
    ] +
    [("dimension{}".format(i), str) for i in range(1, 51)] +
    [("meteric{}".format(i), float) for i in range(1, 51)]
)


from django.conf import settings
from report.gapis.bigquery.schema import translate
BASIC_FIELDS = translate(BASIC_FIELDS_DEV)

#SERVICE_ACCOUNT_P12 = os.path.join(BASE_DIR, "tagtoo_lab.p12")
#SERVICE_ACCOUNT_EMAIL = '149333653538-jr7aqk33912blsbci4cvibo335ddcu2l@developer.gserviceaccount.com'
#PROJECT_ID = "gothic-province-823"
#DATASET = "iot"
#BQ_LOG_PATH = "gs://dmp_track/iot/"
#REDIRECT_URI = "http://localhost:8080/"
SERVICE_ACCOUNT_P12 = settings.REPORT_SERVICE_ACCOUNT_P12
SERVICE_ACCOUNT_EMAIL = settings.REPORT_SERVICE_ACCOUNT_EMAIL
PROJECT_ID = settings.REPORT_PROJECT_ID
DATASET = settings.REPORT_DATASET
BQ_LOG_PATH = settings.REPORT_BQ_LOG_PATH
REDIRECT_URI = getattr(settings, 'REPORT_REDIRECT_URI', "http://localhost:8080/" )
