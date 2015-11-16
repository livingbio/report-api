#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright © 2014 vagrant
#
# Distributed under terms of the MIT license.

from django.core.management.base import NoArgsCommand
import json
import random
from django.core.management.base import BaseCommand

from faker import Faker

fake = Faker()
fake.seed(4321)
random.seed(4321)


class Command(BaseCommand):

    def generate_sales_data(self):
        sales_list = []

        items = {}
        items['Candle 6.5W'] = 10.99
        items['Candle 7.4W'] = 12.99
        items['Candle 10W'] = 19.99

        items['Mini Reflector 6W'] = 8.99
        items['Mini Reflector 7W'] = 9.99
        items['Mini Reflector 8W'] = 12.99

        items['Reflector 7.5W'] = 11.99
        items['Reflector 8.4W'] = 12.99
        items['Reflector 12W'] = 18.99

        for i in range(1, 1000):
            sales_data = {}
            sales_data['date'] = str(fake.date_time_this_year().date())
            sales_data['city'] = fake.city()
            sales_data['item'] = random.choice(items.keys())
            sales_data['cost'] = str(items.get(sales_data['item']))
            if fake.boolean():
                sales_data['units'] = ''
                sales_data['hide'] = str(random.randint(1, 20)*500)
            else:
                sales_data['units'] = str(random.randint(1, 20)*500)
                sales_data['hide'] = ''

            sales_list.append(sales_data)

        with open('dashboard/data/sales.json', 'w') as f:
            f.write(
                json.dumps(sales_list, indent=4)
            )

    def generate_mapreduce_data(self):
        user_list = []

        for i in range(1, 1000):
            profile = fake.profile()
            del profile['website']
            del profile['username']
            del profile['ssn']
            del profile['residence']
            del profile['current_location']

            profile['ip'] = fake.ipv4()
            profile['city'] = fake.city()
            profile['language_code'] = fake.language_code()
            profile['smart_phone'] = random.choice(['iOS', 'Android', ])
            profile['last_login'] = str(fake.date_time_this_year().date())
            profile['scenes'] = random.randint(1, 5)
            profile['light'] = random.randint(1, 20)

            user_list.append(profile)

        with open('dashboard/data/users.json', 'w') as f:
            f.write(
                json.dumps(user_list, indent=4)
            )

    def handle(self, *args, **kwargs):
        self.generate_sales_data()
        self.generate_mapreduce_data()
