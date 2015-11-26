#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright © 2015 lizongzhe 
#
# Distributed under terms of the MIT license.

from report.utils import get_current_report
import random
from faker import Faker
from datetime import datetime
fake = Faker()
sn_pool = [fake.ssn() for i in xrange(100)]
user_pool = [fake.ssn() for i in xrange(10000)]

def fake_blub_base(date=None):
    if not date:
        date = datetime.now()
    else:
        date = datetime.strptime("%Y-%m-%d", date)

    fake_data = {}
    fake_data['time'] = fake.date_time_between(start_date="-1d", end_date=date) .strftime('%Y-%m-%d %H:%M:%S')
    fake_data['location_city'] = fake.city()
    fake_data['machine_type'] = 'blub'
    fake_data['machine_class'] = random.choice(['hue start pack', 'hue smart light' ])
    fake_data['machine_id'] = random.choice(sn_pool)
    fake_data['machine_brand'] = random.choice(['philips', 'mi'])
    fake_data['network_user'] = random.choice(user_pool)
    fake_data['network_machine_name'] = fake.word()
    fake_data['network_group'] = fake.word()
    fake_data['network_ip'] = fake.ipv4()
    fake_data['system_language'] = fake.language_code()
    return fake_data


def fake_blub_data(date=None):

    fake_data = fake_blub_base(date)
    fake_data['location_lng'] = float(fake.longitude())
    fake_data['location_lat'] = float(fake.latitude())
    fake_data['color_temperature'] = random.choice(["5,500K", "5,000K", "5,800K", "6,000K", "6,500K", "3,000K", "2,000K", "4,000K", "4,000K", "1,000K", "10,000K"])
    fake_data['scenes'] = random.choice(["General", "Reflector", "Mini Reflector", "Others", "Candle"])
    fake_data['machine_boot_number'] = int(random.random() * 50)
    fake_data['mcahine_total_run_time'] = int(random.random() * 5000000)

    return fake_data

