from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
import hashlib
import httplib2
import json
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OAUTH2_SESSION_PATH = os.path.join(BASE_DIR, "tokens")


def build_http(client_secrets, scopes, redirect_uri):
    md5 = hashlib.md5()
    data = open(client_secrets).read() + json.dumps(scopes)
    md5.update(data.encode('utf-8'))
    cache_key = md5.hexdigest()
    storage = os.path.join(OAUTH2_SESSION_PATH, cache_key)
    session_path = storage
    storage = Storage(storage)
    credentials = storage.get()

    if credentials is None or credentials.invalid:
        # Perform OAuth 2.0 authorization.
        flow = flow_from_clientsecrets(
            client_secrets, scope=scopes, redirect_uri=redirect_uri)
        flow.params['access_type'] = 'offline'

        auth_url = flow.step1_get_authorize_url()
        auth_code = raw_input('{}\n\nEnter the auth code: '.format(auth_url))
        credentials = flow.step2_exchange(auth_code)
        with open(session_path, 'w+') as session_file:
            session_file.write(credentials.to_json())

    http_auth = credentials.authorize(httplib2.Http())

    return http_auth
