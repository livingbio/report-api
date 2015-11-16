# -*- encoding=utf8 -*-
REPORT_NAME = "地震資料"
TABLE_PREFIX = "earthquake"
REPORT_DIMENSIONS = (
    ("longitude", "dimension1", u"經度"),
    ("latitude", "dimension2", u"緯度"),
    ("title", "dimension3", u"標題"),
    ("date", "STRFTIME_UTC_USEC(time, '%Y-%m-%d')", u"日期"),
    ("month", "STRFTIME_UTC_USEC(time, '%Y-%m')", u"月份"),
    ("year", "STRFTIME_UTC_USEC(time, '%Y')", u"年份"),
    ("updated", "time", u"時間"),
)

REPORT_METERIC = (
    ("total", "sum(1)", u"總數"),
    ("magnitude", "meteric2", u"震度"),
    ("avg_magnitude", "avg(meteric2)", "平均震度"),
    ("elev", "meteric3", "深度"),
    ("avg_elev", "avg(meteric3)", "平均深度")
)

