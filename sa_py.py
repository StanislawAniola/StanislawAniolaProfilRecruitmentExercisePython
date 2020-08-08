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
    path = open("persons.json", 'r', encoding='utf-8')

    result = json.load(path)
    return result


data_from_file = get_data_from_file()


# #########################################
class PrepareDate:
    """
    preparing data for further work
    :param json_data: data from get_data_from_file func
    :return: processed data in list of dict
    """

    def __init__(self, data_after_process):
        self.data_after_process = data_from_file


    def birthday_days_left(self, file_data):
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

    def phone_number_clean(self, file_data):
        """
        cleaning phone numbers from special characters
        :param file_data: data in list of dict
        :return: list of dict with cleaned phone numbers
        """
        for person_data in file_data['results']:
            for sign in person_data['phone']:
                person_data['phone'] = ''.join([i for i in person_data['phone'] if i.isdigit()])

        return file_data

    def remove_picture(self, file_data):
        """
        removing picture key from list of dicts
        :param file_data: data in list of dict
        :return: list of dict without picture key
        """
        for person_data in file_data['results']:
            person_data.pop('picture')

        return file_data

    def get_prepared_data(self):
        clean_birthday = PrepareDate(data_from_file).birthday_days_left(self.data_after_process)
        clean_phone_number = PrepareDate(data_from_file).phone_number_clean(clean_birthday)
        clean_picture = PrepareDate(data_from_file).remove_picture(clean_phone_number)

        return clean_picture


data_clean = PrepareDate(data_from_file).get_prepared_data()

# #########################################
class SimplifyDataStructure:

    def __init__(self, processed_data):
        self.processed_data = data_clean

    def prepare_data_to_database(self, data_after_process):
        """
        prepare data to be simple to put to the database
        :param clean_data: list of dicts after being processed
        :return: list of dicts in simple format
        """
        main_list = []

        for dic in data_after_process['results']:
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

    def get_simp_data_str(self):
        simp_ready_data = SimplifyDataStructure(data_clean).prepare_data_to_database(self.processed_data)
        return simp_ready_data

simp_data_struct = SimplifyDataStructure(data_clean).get_simp_data_str()


# #########################################
# Function has been not used, I could not find a way to connect peewee with sqlite3. My idea was to initialize
# basic table with peewee and dynamically create columns with sqlite3; then populate them with peewee.
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

#table_define = database_table_define(ready_data)


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


class DatabaseOperations:

    def __init__(self):
        self.data_str = simp_data_struct


    def initialize_db(self):
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


    def insert_data(self):
        """
        inserting data from prepare_data_to_database function to database
        """
        # print(ready_data[0])
        click.echo('inserting data to Person table')
        for i in self.data_str:
            Person.insert_many(i).execute()

    # insert_data()


    # ######################################### ZAD1
    def gender_perc(self):
        """
        showing and counting percentage of males and females in data
        :return:
        """
        cnt = 0
        for gen in Person.select():
            cnt += 1

        male_cnt = 0
        female_cnt = 0
        for sex in Person.select():
            if sex.gender == "male":
                male_cnt += 1
            elif sex.gender == "female":
                female_cnt += 1

        male_perc = (male_cnt * 100) / cnt
        female_perc = (female_cnt * 100) / cnt

        result = 'male: {0}% \nfemale: {1}%'.format(male_perc, female_perc)
        click.echo(result)


    # print(gender_perc())

    # print('\n')


    # ######################################### ZAD 2

    def average_age(self, genderr):
        """
        showing and counting average age of males/ females/ total
        :param gender: choosing a gender (can be None)
        """
        result_message = 'total average age -> '
        query_average_age = None
        average_total_age = None

        def gen_query(gen):
            query_avg_male_female = Person.select(fn.COUNT(Person.age).alias('total'),
                                              fn.SUM(Person.age).alias('total_age')).where(Person.gender == gen)
            return query_avg_male_female

        if  genderr == None:
            query_average_age = Person.select(fn.COUNT(Person.age).alias('total'), fn.SUM(Person.age).alias('total_age'))
        elif genderr == 'male':
            query_average_age = gen_query(genderr)
        elif genderr == 'female':
            query_average_age = gen_query(genderr)

        #result_final_message = '{0}{1}'.format(result_message, average_total_age)

        for res in query_average_age:
            average_total_age = res.total_age/res.total

        result_final_message = '{0}{1}'.format(result_message, average_total_age)

        click.echo(result_final_message)


    # average_age()

    # print('\n')


    # ######################################### ZAD 3

    def city_frequency(self, count):
        """
        counting most common cities in data
        :param count: how many cities to display
        """
        query = Person.select(fn.COUNT(Person.city).alias('city_count'), Person.city).group_by(Person.city).order_by(
            fn.COUNT(Person.city).desc()).limit(count)

        for c in query:
            result_messgae = '{0} -> {1}'.format(c.city, c.city_count)
            click.echo(result_messgae)


    # city_frequency(5)

    # print('\n')


    def password_frequency(self, count):
        """
        counting most common passwords in data
        :param count: how many passwords to display
        """
        query = Person.select(fn.COUNT(Person.password).alias('pass_count'), Person.password).group_by(
            Person.password).order_by(
            fn.COUNT(Person.password).desc()).limit(count)

        for p in query:
            click.echo('{0} -> {1}'.format(p.password, p.pass_count))


    # password_frequency(5)

    # print('\n')


    # ######################################### ZAD 4

    def birth_between(self, start):
        """
        showing how many people were born in specified date range
        :param start: start date in format (%Y-%m-%d)
        :param end: end date in format (%Y-%m-%d)
        """
        date_l = list(start)
        start_date = date_l[0]
        end_date = date_l[1]

        start_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
        query = Person.select(Person.first, Person.last).where(
            (fn.DATE(Person.date) >= start_date) & (fn.DATE(Person.date) <= end_date))

        for i in query:
            print(i.first, ' ', i.last)


    # birth_between('1990-06-01', '1993-06-01')

    # print('\n')


    # ######################################### ZAD 5

    def password_rewarder(self, count):
        """
        showing the strongest password that meets the condictions
        :param count:
        """
        query = Person.select(fn.COUNT(Person.password).alias('pass_count'), Person.password).group_by(Person.password)

        d_test = {}

        for i in query:
            d_test[i.password] = {'flag': None}

        for k, v in d_test.items():
            lista = []
            for l in k:
                if l.islower():
                    lista.append('low')
                elif l.isupper():
                    lista.append('upp')
                elif l.isdigit():
                    lista.append('dig')
            if len(k) >= 8:
                lista.append('l8')
            if not k.isalnum():
                lista.append('sp')
            v['flag'] = (set(lista))
            v['flag'] = list(v['flag'])

        for k in d_test:
            score = 0
            for l in d_test[k]:
                if 'low' in d_test[k][l]:
                    score += 1
                if 'upp' in d_test[k][l]:
                    score += 2
                if 'dig' in d_test[k][l]:
                    score += 1
                if 'l8' in d_test[k][l]:
                    score += 5
                if 'sp' in d_test[k][l]:
                    score += 3
            d_test[k] = score

        v = list(d_test.values())
        k = list(d_test.keys())
        key_max = k[v.index(max(v))]
        val_max = d_test[key_max]

        l_max_dict = {}

        # for i in range(0, count+1):
        for i in d_test:
            if d_test[i] == val_max:
                l_max_dict[i] = d_test[i]
            # val_max -= 1

        for i in l_max_dict:
            print(i, ' -> ', '{0} pkt.'.format(l_max_dict[i]))

    # password_rewarder()


exe_db_operations = DatabaseOperations()

# COMMAND LINE USER INTERFACE
@click.group()
@click.option('--inn', is_flag=True)
def cli(inn):
    click.echo()


@cli.command(name='init')
def init():
    return exe_db_operations.initialize_db()

@cli.command(name='insert')
def insert():
    return exe_db_operations.insert_data()

@cli.command(name='perc')
def gender_perc():
    return exe_db_operations.gender_perc()

@cli.command(name='genderr')
@click.option('--genderr', default=None, type=str, help='average age of specified gender')
def avg_gen_age(genderr):
    return exe_db_operations.average_age(genderr)


@cli.command(name='city')
@click.option('--count', default=1, type=int, help='most common city')
def most_common_city(count):
    return exe_db_operations.city_frequency(count)

@cli.command('pass')
@click.option('--count', default=1, type=int, help='number of most frequently appearing passwords in password column')
def most_common_password(count):
    return exe_db_operations.password_frequency(count)

@cli.command('birth')
@click.option('--start', '--end', multiple=True, type=str, help='strpngest password')
def birth_between(start):
    return exe_db_operations.birth_between(start)

@cli.command('passr')
@click.option('--count', is_flag=True, help='strpngest password')
def strongest_password(count):
    return exe_db_operations.password_rewarder(count)


if __name__ == '__main__':
    cli()

