# -*- encoding=utf8 -*-
GROUP = "iot"
REPORT_NAME = "智慧燈泡"
TABLE_PREFIX = "blub2"

## 基礎 dimension
# 保留 dimension1-20 供客製化
# 保留 meteric1-20 供客製化
BASIC_DIMENSIONS = [
        ("time", "time", "系統時間"),
        ("date", "STRFTIME_UTC_USEC(time, '%Y-%m-%d')", u"日期"),
        ("month", "STRFTIME_UTC_USEC(time, '%Y-%m')", u"月份"),
        ("year", "STRFTIME_UTC_USEC(time, '%Y')", u"年份"),
        ("weekday", "DAYOFWEEK(time)", u"weekday"),

        ("location_city", "dimension21", "所在城市"),

        ("machine_type", "dimension22", "機器類別"),
        ("machine_class", "dimension23", "機器型號"),
        ("machine_id", "dimension24", "機器id"),
        ("machine_brand", "dimension25", "機器品牌"),

        ("network_user", "dimension26", "使用者id"),
        ("network_machine_name", "dimension27", "使用者定義機器名稱"),
        ("network_group", "dimension28", "使用者分群"),
        ("network_ip", "dimension29", "ip address"),

        ("system_language", "dimension30", "系統語言"),
]

BASIC_METERICS = [
        ("location_lat", "meteric21", "所在經度"),
        ("location_lng", "meteric22", "所在緯度"),

        ("machine_boot_number", "meteric23", "開機次數"),
        ("mcahine_total_run_time", "meteric24", "開機時間(s)"),
        ("total", "count(time)", "總數"),
]


REPORT_DIMENSIONS = BASIC_DIMENSIONS + [
        ("color_temperature", "dimension31", "色溫"),
        ("scenes", "dimension32", "當前場景"),

]

REPORT_METERIC = BASIC_METERICS + [
        ("scenes_number", "metric22", "場景數量"),
        ("total_user", "count(distinct network_user)", "使用者數"),
        ("avg_power_frequency", "avg(meteric21)", "平均開機次數"),
        ("total_group", "count(network_group)", "總群組數"),
]

