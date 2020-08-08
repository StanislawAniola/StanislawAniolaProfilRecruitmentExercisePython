import requests
import json
from datetime import datetime, timedelta, date
import dateutil.parser

from peewee import *

import click

import sqlite3


# #########################################
def get_data_from_api():
    """
    getting data from randomuser.me
    :return: dict object with data from url
    """
    url = 'https://randomuser.me/api/'
    result = requests.get(url)
    api_content = json.loads(result.content)
    return api_content
    # print(api_content)

get_data_from_api()


# #########################################
def get_data_from_file():
    """
    getting data from persons.json file
    :return: list of dicts with data from path
    """
    path = open("persons.json", 'r', encoding='utf-8')

    result = json.load(path)
    return result


data_from_file = get_data_from_file()

#print(data_from_file['results'][0])

l = []
for i in range(10):
    l.append(get_data_from_api()['results'])

print()