#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright © 2016 lizongzhe 
#
# Distributed under terms of the MIT license.

from django import forms
from models import Report

class ReportForm(forms.ModelForm):
    file = forms.FileField()

    class Meta:
        model = Report
        fields = ['name', 'group', 'description']
