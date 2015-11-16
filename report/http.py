#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright © 2015 george
#
# Distributed under terms of the MIT license.

from oauth2client.client import SignedJwtAssertionCredentials
import os
from report.gapis import bigquery, storage
from report import settings

SCOPES = bigquery.SCOPES + storage.SCOPES

p12 = settings.SERVICE_ACCOUNT_P12

client_email = settings.SERVICE_ACCOUNT_EMAIL
with open(p12) as f:
    private_key = f.read()

credentials = SignedJwtAssertionCredentials(client_email, private_key, SCOPES)

from httplib2 import Http

http = credentials.authorize(Http())
