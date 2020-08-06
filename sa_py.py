import requests
import json
from datetime import datetime, timedelta, date
import dateutil.parser
from dateutil.relativedelta import relativedelta

from collections import OrderedDict
import operator

from peewee import *

import click

import sqlite3


# #########################################
def get_data_from_api():
    url = 'https://randomuser.me/api/'
    result = requests.get(url)
    api_content = json.loads(result.content)

    # print(api_content)


# get_data_from_api()


# #########################################
def get_data_from_file():
    path = open("persons.json", 'r', encoding='utf-8')
    result = json.load(path)
    return result


data_from_file = get_data_from_file()


# #########################################
def prepare_date(json_data):
    def birthday_days_left(file_data):
        for person_data in file_data['results']:
            for spec in person_data:
                if spec == 'dob':
                    parse_to_datetime = dateutil.parser.parse(person_data['dob']['date'])
                    current_date = datetime.now()

                    if parse_to_datetime.month != 2 and parse_to_datetime.day != 29:
                        next_birth_date = parse_to_datetime.replace(year=current_date.year)
                        if next_birth_date.date() > current_date.date():
                            days_left = next_birth_date.date() - current_date.date()
                            person_data['dob']['days_to_birthday'] = days_left.days
                        else:
                            next_birth_date = parse_to_datetime.replace(year=current_date.year + 1)
                            days_left = next_birth_date.date() - current_date.date()
                            person_data['dob']['days_to_birthday'] = days_left.days
                    else:
                        next_birth_date = parse_to_datetime.replace(year=current_date.year)
                        if next_birth_date.date() > current_date.date():
                            days_left = next_birth_date.date() - current_date.date()
                            person_data['dob']['days_to_birthday'] = days_left.days
                        else:
                            y = 1
                            success = False
                            while not success:
                                try:
                                    next_birth_date = parse_to_datetime.replace(year=current_date.year + y)
                                    days_left = next_birth_date.date() - current_date.date()
                                    person_data['dob']['days_to_birthday'] = days_left.days
                                    success = True
                                except:
                                    y += 1
        return file_data

    def phone_number_clean(file_data):
        for person_data in file_data['results']:
            for sign in person_data['phone']:
                person_data['phone'] = ''.join([i for i in person_data['phone'] if i.isdigit()])

        return file_data

    def remove_picture(file_data):
        for person_data in file_data['results']:
            person_data.pop('picture')

        return file_data

    clean_birthday = birthday_days_left(json_data)
    clean_phone_number = phone_number_clean(clean_birthday)
    clean_picuture = remove_picture(clean_phone_number)

    return clean_picuture


data_clean = prepare_date(data_from_file)