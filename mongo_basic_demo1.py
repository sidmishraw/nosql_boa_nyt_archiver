# mongo_basic_demo1.py
# -*- coding: utf-8 -*-
# @Author: Sidharth Mishra
# @Date:   2017-03-15 12:36:16
# @Last Modified by:   Sidharth Mishra
# @Last Modified time: 2017-05-03 13:10:05


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


# Time frame contants
PHASE_1_START_YEAR = 2005
PHASE_1_END_YEAR = 2007
PHASE_2_START_YEAR = 2015
PHASE_2_END_YEAR = 2017


# Constants
__HOSTNAME__ = 'localhost'
__PORT__ = '27017'
__NYT_API_BASE_PATH__ = 'http://api.nytimes.com/svc/'
__API_KEY__ = 'c9e169f173f34249a02bc0aff8d50fb9'
__ARCHIVES_DOC_KEY__ = 'docs'
__ARCHIVES_META_KEY__ = 'meta'
__ARCHIVES_META_HITS_KEY__ = 'hits'
__RESPONSE__ = 'response'


# Database(mongo) name
__DATABASE_NAME__ = 'nyt_archives'
__COLLECTION_NAME__ = 'archives'


# Mongo Operator constants
__OR__ = '$or'
__AND__ = '$and'
__REGEX__ = '$regex'
__LT__ = '$lt'
__GT__ = '$gt'
__EQ__ = '$eq'
__GROUP__ = '$group'
__SUM__ = '$sum'
__ID_OP__ = '_id'
__MATCH__ = '$match'
__UNWIND__ = '$unwind'
__SORT__ = '$sort'


# Article fields
__WEB_URL__ = 'web_url'
__SNIPPET__ = 'snippet'
__LEAD_PARAGRAPH__ = 'lead_paragraph'
__ABSTRACT__ = 'abstract'
__PRINT_PAGE__ = 'print_page'
__BLOG__ = 'blog'
__SOURCE__ = 'source'
__MULTIMEDIA__ = 'multimedia'
__HEADLINE__ = 'headline'
__HEADLINE_MAIN__ = 'main'
__KEYWORDS__ = 'keywords'
__KEYWORDS_NAME__ = 'name'
__KEYWORDS_VALUE__ = 'value'
__PUB_DATE__ = 'pub_date'
__DOCUMENT_TYPE__ = 'document_type'
__NEWS_DESK__ = 'news_desk'
__SECTION_NAME__ = 'section_name'
__SUBSECTION_NAME__ = 'subsection_name'
__BYLINE__ = 'byline'
__PERSON__ = 'person'
__FIRSTNAME__ = 'firstname'
__MIDDLENAME__ = 'middlename'
__LASTNAME__ = 'lastname'
__RANK__ = 'rank'
__ROLE__ = 'role'
__ORGANIZATION__ = 'organization'
__ORIGINAL__ = 'original'
__TYPE_OF_MATERIAL__ = 'type_of_material'
__ID__ = '_id'
__WORD_COUNT__ = 'word_count'
__SLIDESHOW_CREDITS__ = 'slideshow_credits'


# Connection to MongoDB
def get_client():
  '''
  get_client() -> MongoClient

  Connects to the MongoDB instance and returns the `MongoClient`.

  Output(s):

    :return: client `MongoClient` -- The mongo DB client
  '''

  warning('Connecting to mongodb://{hostname}:{port}'.format(hostname=__HOSTNAME__,
                                                             port=__PORT__))

  client = MongoClient('mongodb://{hostname}:{port}'.format(hostname=__HOSTNAME__,
                                                            port=__PORT__))

  warning('Connection established...')

  return client


# Creation of archives dataset
# NYT Arcives API for fetching data from NYT
def invoke_archives_api(year=2000, month=4):
  '''
  invoke_archives_api(year = 2000, month = 4) -> (list, int)

  Invokes the NYT archives API and generates the dataset like the one in `nyt_archives.json`.

  Takes year and month as the arguments.

  Input(s):

    :param: year `int` -- Default value = 2000 - The year of the archives

    :param: month `int` -- Default value = 4 - The month of the archives

  Output(s):

    :return: archives `list` -- A list of documents under the `docs` key of the JSON.
    This is the first phase of data wrangling. `docs` is the key for the actual documents we need
    for the application.

    :return: archives_count `int` -- The count of hits for the particular year-month combination.
    This is obtained from the `hits` field of the `meta` field of the response JSON.
  '''

  url_string = '{base_url}archive/v1/{year}/{month}.json?api-key={api_key}'.format(
      base_url=__NYT_API_BASE_PATH__,
      year=year,
      month=month,
      api_key=__API_KEY__)

  json_string = None

  with urlopen(url_string) as archive_response:
    json_string = archive_response.read()

  json_string = loads(json_string)

  return (json_string[__RESPONSE__][__ARCHIVES_DOC_KEY__],
          json_string[__ARCHIVES_META_KEY__][__ARCHIVES_META_HITS_KEY__])


# modularizing insertion of data into the mongo cluster
def __insert_documents__(client, year, month):
  '''
  __insert_documents__(client, year, month)

  Inserts the documents for the year, month into the mongo cluster.
  Uses the `insert_many` query style for bulk insert of documents into mongo.

  Input(s):

    :param: client `MongoClient` -- The client instance that will establish connection
    to the Mongo cluster.

    :param: year `int` -- The year for fetching data from NYT service.

    :param: month `int` -- The month for fetching data from NYT service.

  Internally invokes the ``invoke_archives_api(year=year, month=month)``.

  Uses the global __DATABASE_NAME__ and __COLLECTION_NAME__ defined on the script level.

  Output(s):
    None
  '''

  archives, archives_count = invoke_archives_api(year=year, month=month)

  db = client.get_database(__DATABASE_NAME__)
  db[__COLLECTION_NAME__].insert_many(archives)

  print('''Created and inserted {archvies_count} archives for year: {year} month:{month} \
        into {collection_name} collection.'''.format(archvies_count=archives_count,
                                                     year=year,
                                                     month=month,
                                                     collection_name=__COLLECTION_NAME__))
  return


# Create archives dataset
def create_archives_dataset():
  '''
  create_archives_dataset()

  Creates the archives dataset on the mongo cluster.

  Input(s):

    None

  Output(s):

    None
  '''

  client = get_client()

  months = [x for x in range(1, 13)]

  # insert data for FIRST PHASE - 2005 - 2007 (Includes data from years
  # 2005, 2006 and 2007)
  for year in range(PHASE_1_START_YEAR, PHASE_1_END_YEAR + 1):
    for month in months:
      try:
        __insert_documents__(client, year, month)
      except Error:
        print(
            'Failed inserting data for year:{year}, month:{month}'.format(
                year=year, month=month))

  print('Completed data insertion for Phase 1: 2005 - 2007')

  # insert data for SECOND PHASE - 2015 - 2017 ( Includes data from years
  # 2015, 2016 and few years of 2017)
  for year in range(PHASE_2_START_YEAR, PHASE_2_END_YEAR + 1):
    for month in months:
      try:
        __insert_documents__(client, year, month)
      except Error:
        print(
            'Failed inserting data for year:{year}, month:{month}'.format(
                year=year, month=month))

  print('Completed data insertion for Phase 1: 2015 - 2017')

  return


# 3. Search for articles based on user entry.
# Querying mongodb
def search_in_articles(user_entry):
  '''
  search_in_articles(user_entry) -> pymongo.cursor.Cursor

  Query#3 - Search for articles based on user entry.

  Searches the articles on the `snippet`, `lead_paragraph` and `abstract` fields for the patterns
  sought by the user. This function returns the cursor object that can then be iterated to get all
  the matching articles.

  Input(s):

    :param: user_entry `str` -- The string or pattern the user is looking for.

  Output(s):

    :return: cursor `pymongo.cursor.Cursor` -- The cursor object that can be used to iterate through
    the results on demand.
  '''

  client = get_client()

  db = client.get_database(__DATABASE_NAME__)

  '''
  Sample mongo shell query:

  db.nyt_archives.find({
        $or: [{
                "lead_paragraph": {
                    $regex: /.*of the need.*/
                  }
              },
              {
                "snippet": {
                    $regex: /.*of the need.*/
                }
              },
              {
                "abstract": {
                    $regex: /.*of the need.*/
                }
              }
        ]
  })
  '''

  query = {
      __OR__: [
          {
              __LEAD_PARAGRAPH__: {
                  __REGEX__: '.*{}.*'.format('.*'.join(user_entry.split(' ')))
              }
          },
          {
              __SNIPPET__: {
                  __REGEX__: '.*{}.*'.format('.*'.join(user_entry.split(' ')))
              }
          },
          {
              __ABSTRACT__: {
                  __REGEX__: '.*{}.*'.format('.*'.join(user_entry.split(' ')))
              }
          }
      ]
  }

  cursor = db[__COLLECTION_NAME__].find(query)

  return cursor


# 4. Find articles by reporter name.
def search_articles_reporter_name(first_name, middle_name, last_name):
  '''
  search_articles_reporter_name(first_name, middle_name, last_name) -> pymongo.cursor.Cursor

  Query#4. Find articles by reporter name.

  Searches the articles for the given name of reporter and then returns the cursor object which can
  be iterated upon to get the matching list of articles.

  The status being looked for is ```reported```. Other statuses are ignored.

  Input(s):

    :param: first_name `str` -- The first name of the reporter.

    :param: middle_name `str` -- The middle name of the reporter.

    :param last_name `str` -- The last name of the reporter.

  Output(s):

    :return: cursor `pymongo.cursor.Cursor` -- The cursor instance that can be iterated upon when
    needed.
  '''

  client = get_client()

  db = client.get_database(__DATABASE_NAME__)

  '''
  Sample mongo shell query:

  db.nyt_archives.find({
        $and: [
          {
            'byline.person.firstname': 'Constance'
          },
          {
            'byline.person.middlename': 'L.'
          },
          {
            'byline.person.lastname': 'HAYS'
          },
          {
            'byline.person.role': 'reported'
          }
        ]
  })
  '''

  query = {
      __AND__: [
          {
              '{byline}.{person}.{first_name}'.format(
                  byline=__BYLINE__,
                  person=__PERSON__,
                  first_name=__FIRSTNAME__): first_name
          },
          {
              '{byline}.{person}.{middle_name}'.format(
                  byline=__BYLINE__,
                  person=__PERSON__,
                  middle_name=__MIDDLENAME__): middle_name
          },
          {
              '{byline}.{person}.{last_name}'.format(
                  byline=__BYLINE__,
                  person=__PERSON__,
                  last_name=__LASTNAME__): last_name
          },
          {
              '{byline}.{person}.{role}'.format(
                  byline=__BYLINE__,
                  person=__PERSON__,
                  role=__ROLE__): 'reported'
          }
      ]
  }

  cursor = db[__COLLECTION_NAME__].find(query)

  return cursor


# 13. List all the types of material with article count
def list_articles_type_of_materials():
  '''

  list_articles_type_of_materials() -> pymongo.cursor.Cursor

  Query#13. List all the types of material with article count.

  This function will list all the articles according to the `type_of_material` field and their
  associated counts.

  Input(s):

    None

  Output(s):

    :return: cursor `pymongo.cursor.Cursor` -- The cursor instance that can be iterated upon when
    needed.

  Note: Internally uses the mongo's pipeline aggregation query instead of map-reduce query.
  '''

  client = get_client()

  db = client.get_database(__DATABASE_NAME__)

  # a grouping query, followed by pipeline aggregation
  # unlike the `find` queries, pymongo expects a `list` as argument
  # for `aggregate` queries.

  '''
  Sample Mongo shell query:

  db.nyt_archives.aggregate({
    $group: {
      '_id': '$type_of_material',
      'count': {
        $sum: 1
      }
    }
  })
  '''

  pipeline_query = [
      {
          __GROUP__: {
              __ID_OP__: '${}'.format(__TYPE_OF_MATERIAL__),
              'count': {
                  __SUM__: 1
              }
          }
      }
  ]

  cursor = db[__COLLECTION_NAME__].aggregate(pipeline_query)

  return cursor


# 7. Find the most productive reporter (reporter)
def most_productive_reporter():
  '''

  most_productive_reporter() -> str

  Query#7. Find the most productive reporter (reporter)

  This function will return the json string for the most productive reporter. The productivity
  is defined by the number of articles the person appears in `byline.person` field with a
  `reported` role.

  Input(s):

    None

  Output(s):

    :return: most_productive_reporter `str` -- The most productive reporter's JSON string
  '''

  client = get_client()

  db = client.get_database(__DATABASE_NAME__)

  '''
  db.nyt_archives.aggregate([
    {
      $match: {
        $and: [
          {
            'byline.person.role': "reported"
          },
          {
            'byline.person.firstname': {
              $regex: /.+/
            }
          }
        ]
      }
    },
    {
      $unwind: '$byline.person'
    },
    {
      $group: {
        '_id': '$byline.person',
        'article_count': {
          $sum: 1
        }
      }
    },
    {
      $sort: {
        'article_count': -1
      }
    }
  ])
  '''

  pipeline_query = [
      {
          __MATCH__: {
              __AND__: [
                  {
                      '{byline}.{person}.{role}'.format(
                          byline=__BYLINE__,
                          person=__PERSON__,
                          role=__ROLE__): 'reported'
                  },
                  {
                      '{byline}.{person}.{first_name}'.format(
                          byline=__BYLINE__,
                          person=__PERSON__,
                          first_name=__FIRSTNAME__): {
                          __REGEX__: '.+'
                      }
                  }
              ]
          }
      },
      {
          __UNWIND__: '${byline}.{person}'.format(
              byline=__BYLINE__,
              person=__PERSON__)
      },
      {
          __GROUP__: {
              __ID_OP__: '${byline}.{person}'.format(
                  byline=__BYLINE__,
                  person=__PERSON__),
              'article_count': {
                  __SUM__: 1
              }
          }
      },
      {
          __SORT__: {
              'article_count': -1
          }
      }
  ]

  cursor = db[__COLLECTION_NAME__].aggregate(pipeline_query)

  most_productive_reporter = None

  if cursor is not None:
    most_productive_reporter = next(cursor)

  return most_productive_reporter


if __name__ == '__main__':
  basicConfig(format='%(asctime)s %(message)s')
  print('Testing pymongo and mongo connections and queries...')
  # db = client.get_database('usdata')
  # print('All the collections of `usdata` db : {}'.format(db.collection_names()))
