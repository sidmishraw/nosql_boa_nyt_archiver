# mongo_basic_demo1.py
# -*- coding: utf-8 -*-
# @Author: Sidharth Mishra
# @Date:   2017-03-15 12:36:16
# @Last Modified by:   Sidharth Mishra
# @Last Modified time: 2017-03-16 10:47:22




'''
This script contains the demo for using PyMongo and connecting to mongodb database and queries
for the usecases.
'''
# python standard library imports
from datetime import datetime
from json import dumps
from json import loads
from urllib.request import urlopen
from logging import basicConfig
from logging import warning




# pymongo imports
from pymongo import MongoClient




# Constants
__HOSTNAME__ = 'localhost'
__PORT__ = '27017'
__NYT_API_BASE_PATH__ = 'http://api.nytimes.com/svc/'
__API_KEY__ = 'c9e169f173f34249a02bc0aff8d50fb9'
__ARCHIVES_DOC_KEY__ = 'docs'
__RESPONSE__ = 'response'




# Connection to MongoDB
def get_client():
  '''
  Connects to the MongoDB instance and returns the MongoDBclient.

  :return: client `MongoClient`
  '''

  warning('Connecting to mongodb://{hostname}:{port}'.format(hostname = __HOSTNAME__, \
    port = __PORT__))

  client = MongoClient('mongodb://{hostname}:{port}'.format(hostname = __HOSTNAME__, \
    port = __PORT__))

  return client




# Creation of archives dataset
# NYT Arcives API for fetching data from NYT
def __archives_api__(year = 2000, month = 4):
  '''
  Calls the NYT archives API and generates the sample dataset inside `nyt_archives.json`.
  Takes year and month as the arguments.

  :param: year `int` - Default value = 2000 - The year of the archives
  :param: month `int` - Default value = 4 - The month of the archives

  :return: archives `list`
  '''

  url_string = '{base_url}archive/v1/{year}/{month}.json?api-key={api_key}'.format(\
    base_url = __NYT_API_BASE_PATH__, year = year, month = month, api_key = __API_KEY__)

  json_string = None

  with urlopen(url_string) as archive_res:
    json_string = archive_res.read()

  return loads(json_string)[__RESPONSE__][__ARCHIVES_DOC_KEY__]




def create_archives_dataset():
  '''
  Creates the archives dataset by 
  '''

  warning('Getting connection to MongoDB')
  client = get_client()
  warning('Got connection to MongoDB')

  # try building 2 collections for 2 months
  months = [4, 5]

  for month in months:
    warning('Fetching archives from NYT for month {}'.format(month))
    archives = __archives_api__(month = month)
    warning('Fetched archives from NYT for month {}'.format(month))
    warning('Fetching database archives_2000 from mongodb')
    db = client.get_database('archives_2000')
    warning('Fetched database archives_2000 from mongodb')
    warning('Inserting documents into the collection month_{}'.format(month))
    db['month_{}'.format(month)].insert_many(archives)
    warning('Inserted documents into the collection month_{}'.format(month))
    print('Created and inserted archives into month_{} collection..'.format(month))




if __name__ == '__main__':
  basicConfig(format = '%(asctime)s %(message)s')
  print('Testing pymongo and mongo connections and queries...')
  # db = client.get_database('usdata')
  # print('All the collections of `usdata` db : {}'.format(db.collection_names()))

