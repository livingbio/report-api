#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright © 2015 george
#
# Distributed under terms of the MIT license.


def translate(schema):
    rs = []
    for key, value in schema.items():
        if value == "timestamp":
            rs.append({"name": key, "type": "timestamp"})
        elif value == str:
            rs.append({"name": key, "type": "string"})
        elif value == float:
            rs.append({"name": key, "type": "float"})
        elif value == int:
            rs.append({"name": key, "type": "integer"})
        elif isinstance(value, list):
            if isinstance(value[0], dict):
                rs.append({"name": key, "type": "record",
                           "mode": "repeated", "fields": translate(value[0])})
            elif value[0] == str:
                rs.append({"name": key, "type": "string", "mode": "repeated"})
            elif value[0] == float:
                rs.append({"name": key, "type": "float", "mode": "repeated"})
            else:
                raise Exception("Translate Failed")
        elif isinstance(value, dict):
            rs.append({"name": key, "type": "record",
                       "fields": translate(value)})
        else:
            raise Exception("Translate Failed")
    return rs


def validate(obj):
    """ bigquery not accept None"""
    if isinstance(obj, dict):
        new_obj = {}
        for k, v in obj.items():
            v = validate(v)

            if v:
                new_obj[k] = v
        return new_obj

    elif isinstance(obj, list):
        new_obj = []
        for v in obj:
            v = validate(v)
            if v:
                new_obj.append(v)
        return new_obj

    if obj:
        return obj
