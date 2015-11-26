#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright © 2015 lizongzhe 
#
# Distributed under terms of the MIT license.

from datetime import datetime, timedelta
from django.test import TestCase
from faker import Faker
from mock import patch
from report.models import Report, ReportCol, Table
import re

faker = Faker()

class ReportRegistedTests(TestCase):
    pass
