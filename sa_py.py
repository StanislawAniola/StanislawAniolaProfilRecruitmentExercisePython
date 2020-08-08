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
    """
    getting data from randomuser.me
    :return: dict object with data from url
    """
    url = 'https://randomuser.me/api/'
    result = requests.get(url)
    api_content = json.loads(result.content)

    # print(api_content)


# get_data_from_api()


# #########################################
def get_data_from_file():
    """
    getting data from persons.json file
    :return: list of dicts with data from path
    """
    path = open("persons.json", 'r',
                encoding='utf-8')

    result = json.load(path)
    return result


data_from_file = get_data_from_file()


# #########################################
def prepare_date(json_data):
    """
    preparing data for further work
    :param json_data: data from get_data_from_file func
    :return: processed data in list of dict
    """
    def birthday_days_left(file_data):
        """
        counting how many days are left for nex birthday
        :param file_data: data in list of dict
        :return: list of dict with days left for next birth day
        """
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
        """
        cleaning phone numbers from special characters
        :param file_data: data in list of dict
        :return: list of dict with cleaned phone numbers
        """
        for person_data in file_data['results']:
            for sign in person_data['phone']:
                person_data['phone'] = ''.join([i for i in person_data['phone'] if i.isdigit()])

        return file_data

    def remove_picture(file_data):
        """
        removing picture key from list of dicts
        :param file_data: data in list of dict
        :return: list of dict without picture key
        """
        for person_data in file_data['results']:
            person_data.pop('picture')

        return file_data

    clean_birthday = birthday_days_left(json_data)
    clean_phone_number = phone_number_clean(clean_birthday)
    clean_picuture = remove_picture(clean_phone_number)

    return clean_picuture


data_clean = prepare_date(data_from_file)


# #########################################
def prepare_data_to_database(clean_data):
    """
    prepare data to be simple to put to the database
    :param clean_data: list of dicts after being processed
    :return: list of dicts in simple format
    """
    main_list = []

    for dic in clean_data['results']:
        s_dict = {}
        for k, v in dic.items():
            if not isinstance(v, dict):
                if k in list(s_dict.keys()):
                    s_dict['{0}{1}'.format(k, 1)] = v
                else:
                    s_dict[k] = v
            elif isinstance(v, dict):
                for kk, vv in v.items():
                    if not isinstance(vv, dict):
                        if kk in list(s_dict.keys()):
                            s_dict['{0}{1}'.format(kk, 1)] = vv
                        else:
                            s_dict[kk] = vv
                    elif isinstance(vv, dict):
                        for kkk, vvv in vv.items():
                            if not isinstance(vvv, dict):
                                if k in list(s_dict.keys()):
                                    s_dict['{0}{1}'.format(kkk, 1)] = vvv
                                else:
                                    s_dict[kkk] = vvv
        main_list.append(s_dict)

    return main_list


ready_data = prepare_data_to_database(data_clean)


# #########################################
def database_table_define(file_data):
    """
    defining database columns structure
    :param file_data: list of dicts in simple format
    :return: dict with database column structure
    """
    file_data_work = file_data[0]

    def prepare_table():
        """
        defining database columns types
        :return: dict with columns structures
        """
        table_structure = {}
        table_data_type = {'str': 'CharField', 'int': 'IntegerField'}

        for k, typ in file_data_work.items():
            if 'date' in k:
                table_structure[k] = 'DateTimeField'
            elif isinstance(typ, str):
                table_structure[k] = table_data_type['str']
            elif isinstance(typ, int):
                table_structure[k] = table_data_type['int']

        return table_structure

    return prepare_table()


table_define = database_table_define(ready_data)

# #########################################
db_test = SqliteDatabase("PersonData.sqlite")


class BaseModel(Model):
    class Meta:
        database = db_test


class Person(BaseModel):
    gender = CharField(null=True)
    title = CharField(null=True)
    first = CharField(null=True)
    last = CharField(null=True)
    number = IntegerField(null=True)
    name = CharField(null=True)
    city = CharField(null=True)
    state = CharField(null=True)
    country = CharField(null=True)
    postcode = IntegerField(null=True)
    latitude = CharField(null=True)
    longitude = CharField(null=True)
    offset = CharField(null=True)
    description = CharField(null=True)
    email = CharField(null=True)
    uuid = CharField(null=True)
    username = CharField(null=True)
    password = CharField(null=True)
    salt = CharField(null=True)
    md5 = CharField(null=True)
    sha1 = CharField(null=True)
    sha256 = CharField(null=True)
    date = DateTimeField(null=True)
    age = IntegerField(null=True)
    days_to_birthday = IntegerField(null=True)
    date1 = DateTimeField(null=True)
    age1 = IntegerField(null=True)
    phone = CharField(null=True)
    cell = CharField(null=True)
    name1 = CharField(null=True)
    value = CharField(null=True)
    nat = CharField(null=True)


@click.group()
@click.option('--inn', is_flag=True)
def cli(inn):
    click.echo()


@cli.command(name='init')
def initialize_db():
    """
    initializing database
    """
    click.echo('initializing database')
    db_test.connect()
    # Person = Table('person', ('id', 'first', 'last'))
    # Person = Person.bind(db_test)
    db_test.create_tables([Person], safe=True)
    db_test.close()


# initialize_db()


@cli.command(name='populate')
def insert_data():
    """
    inserting data from prepare_data_to_database function to database
    """
    # print(ready_data[0])
    click.echo('inserting data to Person table')
    for i in ready_data:
        Person.insert_many(i).execute()


# insert_data()
if __name__ == '__main__':
    cli()