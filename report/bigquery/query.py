# -*- coding: utf-8 -*-

from report import settings
from report import utils
flat_fields = utils.flat_dict(settings.BASIC_FIELDS_DEV)

basic_template = '''
    select {select_query}
    from {table_query}
    where {where_query}
    {groupby_query}
    having {having_query}
    {order_query}
    {limit_query}
    ignore case
'''


filter_map = {
    ">": "{key} > {value}",
    "<": "{key} < {value}",
    ">=": "{key} >= {value}",
    "<=": "{key} <= {value}",
    "=": "{key} = {value}",
    "eq": "{key} = '{value}'",
    "startswith": "REGEXP_MATCH({key}, '^{value}')",
    "endswith": "REGEXP_MATCH({key}, '{value}$')",
    "contains": "{key} contains '{value}'",
    "regex": "REGEXP_MATCH({key}, '{value}')",
    "not_startswith": "not REGEXP_MATCH({key}, '^{value}')",
    "not_endswith": "not REGEXP_MATCH({key}, '{value}$')",
    "not_contains": "not {key} contains '{value}'",
    "not_regex": "not REGEXP_MATCH({key}, '{value}')",
}


# 維度
extra_dimensions = {
    "day": {"query": "STRFTIME_UTC_USEC(time, '%Y-%m-%d')", "name": "日期"},
    "week": {"query": "STRFTIME_UTC_USEC(time, '%Y-%W')", "name": "週"},
    "month": {"query": "STRFTIME_UTC_USEC(time, '%Y-%m')", "name": "月"},
    "weekday": {"query": "DAYOFWEEK(time)", "name": "weekday"},
}

# 基礎指標
extra_metrics = {
    "total": {"query": "sum(1)", "name": "總數"},
}


def parse_filter(key, opt, value):
    query_str = filter_map[opt].format(key=key, value=value)
    return query_str


def parse_metric(metric):
    if metric in extra_metrics:
        query = "{} as {}".format(extra_metrics[metric]['query'], metric)
        return {"query": query, "key": metric}


def parse_dimension(dimension):
    if dimension in flat_fields:
        return {"query": dimension, "key": dimension}
    if dimension in extra_dimensions:
        query = "{} as {}".format(
            extra_dimensions[dimension]['query'], dimension)
        return {"query": query, "key": dimension}


def parse_order(orders):
    import re
    if orders:
        tmp = []
        for order in orders:
            flag, name = re.match("^(-)?([^-]*)", order).groups()

            if name not in metric_keys + dimension_keys:
                continue

            if flag.strip() == '-':
                tmp.append("{} desc".format(name))
        if tmp:
            order_query = "order by {}".format(" ".join(tmp))
    return order_query


def parse_query(table_query, dimension_keys=[], metric_keys=[], filters=[], orders=[], limit=None):
    dimensions = [parse_dimension(dimension)
                  for dimension in set(dimension_keys)]
    metrics = [parse_metric(metric) for metric in set(metric_keys)]
    dimension_keys = [dimension['key']
                      for dimension in dimensions if dimension]
    metric_keys = [metric['key'] for metric in metrics if metric]
    select_query = ",".join([dimension['query'] for dimension in dimensions if dimension] + [
                            metric['query'] for metric in metrics if metric])

    where_query = " and ".join([parse_filter(key, opt, value) for key, opt,
                                value in filters if key in flat_fields.keys() or key in dimension_keys]) or "True"
    having_query = " and ".join([parse_filter(
        key, opt, value) for key, opt, value in filters if key in metric_keys]) or "True"

    if metrics and dimension_keys:
        groupby_query = "group by " + \
            ",".join([dimension['key']
                      for dimension in dimensions if dimension])
    else:
        groupby_query = ""

    order_query = parse_order(orders)

    if limit:
        limit_query = "limit {}".format(limit)
    else:
        limit_query = ""

    query = basic_template.format(select_query=select_query, table_query=table_query, where_query=where_query,
                                  groupby_query=groupby_query, having_query=having_query, order_query=order_query, limit_query=limit_query)
    return query
