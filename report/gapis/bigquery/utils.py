#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright © 2015 george
#
# Distributed under terms of the MIT license.


def to_utctimestamp(dt, multi=1):
    import calendar
    return int((calendar.timegm(dt.utctimetuple()) + dt.microsecond / 1e6) * multi)
