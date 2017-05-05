# nyt_queries.py
# -*- coding: utf-8 -*-
# @Author: Sidharth Mishra
# @Date:   2017-03-15 12:36:16
# @Last Modified by:   Sidharth Mishra
# @Last Modified time: 2017-05-05 12:41:07


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
import re


# pymongo imports
from pymongo import MongoClient
from pymongo import ASCENDING
from pymongo import DESCENDING


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
# __DATABASE_NAME__ = 'nyt_archives'
# __COLLECTION_NAME__ = 'archives'
__DATABASE_NAME__ = 'archives_2000'
__COLLECTION_NAME__ = 'month_4'


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
__ELEM_MATCH__ = '$elemMatch'
__LTEQ__ = '$lte'
__GTEQ__ = '$gte'
__COUNT_OP__ = '$count'
__COUNT_FIELD__ = 'count'
__LIMIT__ = '$limit'


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


# set database and collection names for the script from run-time
# just for offerring more flexibility
def set_db_collection_names(database_name=None, collection_name=None):
  '''
  Sets the db and collection names for the script
  '''

  global __DATABASE_NAME__, __COLLECTION_NAME__

  if database_name is None or collection_name is None:
    warning(
        'Working with default database name: {db} and collection name: {collec}'.format(
            db=__DATABASE_NAME__,
            collec=__COLLECTION_NAME__))
  else:
    __DATABASE_NAME__ = database_name
    __COLLECTION_NAME__ = collection_name
    warning('Updated database name:{db} and collection name: {collec}'.format(
        db=__DATABASE_NAME__, collec=__COLLECTION_NAME__))

  return


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

  warning('''Created and inserted {archvies_count} archives for year: {year} month:{month} \
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
        warning(
            'Failed inserting data for year:{year}, month:{month}'.format(
                year=year, month=month))

  warning('Completed data insertion for Phase 1: 2005 - 2007')

  # insert data for SECOND PHASE - 2015 - 2017 ( Includes data from years
  # 2015, 2016 and few years of 2017)
  for year in range(PHASE_2_START_YEAR, PHASE_2_END_YEAR + 1):
    for month in months:
      try:
        __insert_documents__(client, year, month)
      except Error:
        warning(
            'Failed inserting data for year:{year}, month:{month}'.format(
                year=year, month=month))

  warning('Completed data insertion for Phase 1: 2015 - 2017')

  return


# 3. Search for articles based on user entry.
# Querying mongodb
def search_in_articles(user_entry):
  '''
  search_in_articles(user_entry) -> list[dict]

  Query#3 - Search for articles based on user entry.

  Searches the articles on the `snippet`, `lead_paragraph` and `abstract` fields for the patterns
  sought by the user. This function returns the cursor object that can then be iterated to get all
  the matching articles.

  Input(s):

    :param: user_entry `str` -- The string or pattern the user is looking for.

  Output(s):

    :return: articles `list[dict]` -- The list of article documents
  '''

  client = get_client()

  db = client.get_database(__DATABASE_NAME__)

  '''
  Sample mongo shell query:

  db.month_4.find({
      $and: [{
          "document_type": "article"
      }, {

          $or: [{
              "lead_paragraph": {
                  $regex: /.*of the need.*/i
              }
          }, {
              "snippet": {
                  $regex: /.*of the need.*/i
              }
          }, {
              "abstract": {
                  $regex: /.*of the need.*/i
              }
          }]
      }]
  })
  '''

  query = {
      __AND__: [
          {
              __DOCUMENT_TYPE__: "article"
          },
          {
              __OR__: [
                  {
                      __LEAD_PARAGRAPH__: {
                          __REGEX__: re.compile('.*{pattern}.*'.format(
                              pattern='.*'.join(user_entry.split(' '))),
                              re.IGNORECASE)
                      }
                  },
                  {
                      __SNIPPET__: {
                          __REGEX__: re.compile('.*{pattern}.*'.format(
                              pattern='.*'.join(user_entry.split(' '))),
                              re.IGNORECASE)
                      }
                  },
                  {
                      __ABSTRACT__: {
                          __REGEX__: re.compile('.*{pattern}.*'.format(
                              pattern='.*'.join(user_entry.split(' '))),
                              re.IGNORECASE)
                      }
                  }
              ]
          }
      ]
  }

  cursor = db[__COLLECTION_NAME__].find(query)

  articles = list(cursor) if cursor is not None else None

  return articles


# 4. Find articles by reporter name.
def search_articles_reporter_name(first_name='', middle_name='', last_name=''):
  '''
  search_articles_reporter_name(first_name, middle_name, last_name) -> list[dict]

  Query#4. Find articles by reporter name.

  Searches the articles for the given name of reporter and then returns the cursor object which can
  be iterated upon to get the matching list of articles.

  The status being looked for is ```reported```. Other statuses are ignored.

  Input(s):

    :param: first_name `str` -- The first name of the reporter. -- defaults to ''

    :param: middle_name `str` -- The middle name of the reporter.  -- defaults to ''

    :param last_name `str` -- The last name of the reporter.  -- defaults to ''

  Output(s):

    :return: articles `list[dict]` -- The list of article documents
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
          },
          {
            'document_type': 'article'
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
                  first_name=__FIRSTNAME__): {
                  __REGEX__: re.compile('.*{firstname}.*'.format(firstname=first_name),
                                        re.IGNORECASE)
              }
          },
          {
              '{byline}.{person}.{middle_name}'.format(
                  byline=__BYLINE__,
                  person=__PERSON__,
                  middle_name=__MIDDLENAME__): {
                  __REGEX__: re.compile('.*{middlename}.*'.format(middlename=middle_name),
                                        re.IGNORECASE)
              }
          },
          {
              '{byline}.{person}.{last_name}'.format(
                  byline=__BYLINE__,
                  person=__PERSON__,
                  last_name=__LASTNAME__): {
                  __REGEX__: re.compile('.*{lastname}.*'.format(lastname=last_name),
                                        re.IGNORECASE)
              }
          },
          {
              '{byline}.{person}.{role}'.format(
                  byline=__BYLINE__,
                  person=__PERSON__,
                  role=__ROLE__): 'reported'
          },
          {
              __DOCUMENT_TYPE__: 'article'
          }
      ]
  }

  cursor = db[__COLLECTION_NAME__].find(query)

  articles = list(cursor) if cursor is not None else None

  return articles


# 13. List all the types of material with article count
def list_articles_type_of_materials():
  '''

  list_articles_type_of_materials() -> list[dict]

  Query#13. List all the types of material with article count.

  This function will list all the articles according to the `type_of_material` field and their
  associated counts.

  Input(s):

    None

  Output(s):

    :return: articles `list[dict]` -- The list of articles

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
              __ID_OP__: '${type}'.format(type=__TYPE_OF_MATERIAL__),
              'count': {
                  __SUM__: 1
              }
          }
      }
  ]

  cursor = db[__COLLECTION_NAME__].aggregate(pipeline_query)

  articles = list(cursor) if cursor is not None else None

  return articles


# 7. Find the most productive reporter (reporter)
def most_productive_reporter():
  '''

  most_productive_reporter() -> dict

  Query#7. Find the most productive reporter (reporter)

  This function will return the json string for the most productive reporter. The productivity
  is defined by the number of articles the person appears in `byline.person` field with a
  `reported` role.

  Input(s):

    None

  Output(s):

    :return: most_productive_reporter `dict` -- The most productive reporter's JSON string
  '''

  client = get_client()

  db = client.get_database(__DATABASE_NAME__)

  '''
  Sample mongo shell query:

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

  most_productive_reporter = list(cursor) if cursor is not None else list()

  most_productive_reporter = most_productive_reporter[
      0] if len(most_productive_reporter) > 0 else None

  return most_productive_reporter


# query#1. Compare the top news keywords for the years 2015-2017 and 2005-2007 to
# see what the news has been about. (Basically try and find the difference tha
# thas come about in last 10 years.)
def compare_news_keywords():
  '''
  compare_news_keywords() -> (list[dict], list[dict])

  Query#1: Compare the top news keywords for the years 2015-2017 and 2005-2007 to
  see what the news has been about. (Basically try and find the difference that
  has come about in last 10 years.)

  Fetches the top 10 keywords for years 2005-2007 and 2015-2017 and return the lists of these
  words with the most popular words on the top of each list.

  Input(s):

    None

  Output(s):

    :return: phase1_words `list[dict]` -- The list of keyword values, length = 10 and list index 0
    is the most frequent keyword for year range 2005-2007.

    :return: phase2_words `list[dict]` -- The list of keyword values, length = 10 and list index 0
    is the most frequent keyword for year range 2015-2017.
  '''

  client = get_client()

  db = client.get_database(__DATABASE_NAME__)

  phase1_words = None
  phase2_words = None

  '''
  Sample mongo shell query:

  // for year 2005-2007 -- top 10 keywords a.k.a tags
  db.archives.aggregate([{
      $match: {
          "pub_date": {
              $regex: /200[5-7].*/i
          }
      }
  }, {
      $unwind: "$keywords"
  }, {
      $group: {
          _id: "$keywords",
          count: {
              $sum: 1
          }
      }
  }, {
      $sort: {
          count: -1
      }
  }, {
      $limit: 10
  }])

  // for year 2015 - 2017 -- top 10 keywords a.k.a tags
  db.archives.aggregate([{
      $match: {
          "pub_date": {
              $regex: /201[5-7].*/i
          }
      }
  }, {
      $unwind: "$keywords"
  }, {
      $group: {
          _id: "$keywords",
          count: {
              $sum: 1
          }
      }
  }, {
      $sort: {
          count: -1
      }
  }, {
      $limit: 10
  }])
  '''

  # query is for aggregation pipeline of pymongo
  # query for year range 2005-2007
  phase1_query = [
      {
          __MATCH__: {
              __PUB_DATE__: {
                  __REGEX__: re.compile('200[5-7].*', re.IGNORECASE)
              }
          }
      },
      {
          __UNWIND__: '${pattern}'.format(pattern=__KEYWORDS__)
      },
      {
          __GROUP__: {
              __ID_OP__: '${pattern}'.format(pattern=__KEYWORDS__),
              __COUNT_FIELD__: {
                  __SUM__: 1
              }
          }
      },
      {
          __SORT__: {
              __COUNT_FIELD__: -1
          }
      },
      {
          __LIMIT__: 10
      }
  ]

  cursor = db[__COLLECTION_NAME__].aggregate(phase1_query)

  phase1_words = list(cursor) if cursor is not None else None

  phase2_query = [
      {
          __MATCH__: {
              __PUB_DATE__: {
                  __REGEX__: re.compile('201[5-7].*', re.IGNORECASE)
              }
          }
      },
      {
          __UNWIND__: '${pattern}'.format(pattern=__KEYWORDS__)
      },
      {
          __GROUP__: {
              __ID_OP__: '${pattern}'.format(pattern=__KEYWORDS__),
              __COUNT_FIELD__: {
                  __SUM__: 1
              }
          }
      },
      {
          __SORT__: {
              __COUNT_FIELD__: -1
          }
      },
      {
          __LIMIT__: 10
      }
  ]

  cursor = db[__COLLECTION_NAME__].aggregate(phase2_query)

  phase2_words = list(cursor) if cursor is not None else None

  return (phase1_words, phase2_words)


# Query#2: Find the most popular news keywords from the entire archives
# collection.
def most_popular_news_keywords():
  '''
  most_popular_news_keywords() -> list[dict]

  Query#2: Find the most popular `news` keywords from the entire archives.

  Fetches the list of 5 most popular keywords for news type of material.

  Uses mongo's aggregation pipeline under the hood.

  Input(s):

    None

  Output(s):

    :return: most_popular_keywords `list[dict]` -- The list of most popular news keywords in the
    entire archives collection(dataset) with the most popular of the bunch on the top of the list
    indexed at 0 and so on..
  '''

  client = get_client()

  db = client.get_database(__DATABASE_NAME__)

  most_popular_keywords = None

  '''
  Sample mongo shell query:

  //#2Find the most popular news keywords from the entire archives collection.
  db.archives.aggregate([{
      $match: {
          "type_of_material": "News"
      }
  }, {
      $unwind: "$keywords"
  }, {
      $group: {
          _id: "$keywords",
          count: {
              $sum: 1
          }
      }
  }, {
      $sort: {
          count: -1
      }
  }, {
      $limit: 5
  }])
  '''

  query = [
      {
          __MATCH__: {
              __TYPE_OF_MATERIAL__: "News"
          }
      },
      {
          __UNWIND__: "${pattern}".format(pattern=__KEYWORDS__)
      },
      {
          __GROUP__: {
              __ID_OP__: "${pattern}".format(pattern=__KEYWORDS__),
              __COUNT_FIELD__: {
                  __SUM__: 1
              }
          }
      },
      {
          __SORT__: {
              __COUNT_FIELD__: -1
          }
      },
      {
          __LIMIT__: 5
      }
  ]

  cursor = db[__COLLECTION_NAME__].aggregate(query)

  most_popular_keywords = list(cursor) if cursor is not None else None

  return most_popular_keywords


# Query#6: Find the articles that have occurred on page# x over these years.
def xpage_articles(page_number=1):
  '''
  xpage_articles(page_number=1) -> list[dict]

  Query#6: Find the articles that have occured on page# x over these years.

  Fetches the list of documents for the articles that have occurred on page number `page_number`
  over these years in the dataset (archives).

  Input(s):

    :param: page_number `int` -- The printed page number you are looking for.

  Output(s):

    :return: articles `list[dict]` -- The list of `article` documents occurring
    on the printed page provided.
  '''

  client = get_client()

  db = client.get_database(__DATABASE_NAME__)

  articles = None

  '''
  Sample mongo shell query:

  // Query#6. Find the articles that have occured on page# x over these years.
  db.archives.find({
      $and: [{
          "print_page": "90"
      }, {
          "document_type": "article"
      }]
  })
  '''

  query = {
      __AND__: [
          {
              __PRINT_PAGE__: str(page_number)
          },
          {
              __DOCUMENT_TYPE__: "article"
          }
      ]
  }

  cursor = db[__COLLECTION_NAME__].find(query)

  articles = list(cursor) if cursor is not None else None

  return articles


# Query#8. Find the longest article (page or word count)
def longest_article():
  '''
  longest_article() -> dict

  Query#8. Find the longest article (page or word count)

  Finds and returns the longest article depending on the word count
  (document) obtained from the archives dataset.

  Input(s):

    None

  Output(s):

    :return: article `dict` -- The longest article mongo document
  '''

  client = get_client()

  db = client.get_database(__DATABASE_NAME__)

  article = None

  '''
  Sample mongo shell query:

  //Query#8. Find the longest article (page or word count)
  db.archives.find({
      "document_type": "article"
  }).sort({
      "word_count": -1
  }).limit(1)
  '''

  query = {
      __DOCUMENT_TYPE__: 'article'
  }

  cursor = db[__COLLECTION_NAME__].find(
      query).sort(__WORD_COUNT__, DESCENDING).limit(1)

  article = list(cursor)[0] if cursor is not None else None

  return article


# Query#5. Find articles about specific people or organizations
def search_people_or_organization(search_string, flag_person=False):
  '''
  search_people_or_organization(flag_person=False, search_string) -> list[dict]

  Query#5. Find articles about specific people or organizations

  Find the articles given name of person or an organization and then returns the cursor object which can be iterated upton to get the matching list of articles.

  The flag_person will determine if the query is about people(false) or organization(true)

  Input(s):

    :param: flag_person `bool` -- The flag to determine whether the query is about people or not. -- defaults to false

    :param: search_string `str` -- The name of the person or organization. -- no default

  Output(s):

    :return: articles `list[dict]` -- The list of articles
  '''

  client = get_client()

  db = client.get_database(__DATABASE_NAME__)

  '''
  Mongo shell sample query:

  //#5 -- Organization
  db.archives.find({
    “keywords”: {
      $elemMatch: {
        “name”: “organizations”,
        “value”: {
          $regex: /.*MATTEL.*/i
        }
      }
    }
  })

  //#5 -- People
  db.archives.find({
    “keywords”: {
      $elemMatch: {
        “name”: “persons”,
        “value”: {
          $regex: /.*CONDOLEEZZA.*/i
        }
      }
    }
  })
  '''

  query = {
      __KEYWORDS__: {
          __ELEM_MATCH__: {
              __KEYWORDS_NAME__: "organizations" if not flag_person else "persons",
              __KEYWORDS_VALUE__: {
                  __REGEX__: re.compile('.*{pattern}.*'.format(
                      pattern=search_string), re.IGNORECASE)
              }
          }
      }
  }

  cursor = db[__COLLECTION_NAME__].find(query)

  articles = list(cursor) if cursor is not None else None

  return articles


# Query#10: Find the articles published in certain time range (date)
def articles_between(begin_time, end_time):
  '''
  articles_between(begin_time, end_time) -> list[dict]

  Finds the articles published between the `begin_time` and `end_time`.

  Input(s):

    :param: begin_time `str` -- The beginning of the time/date range -- yyyy-mm-dd format

    :param: end_time `str` -- The end of the time/date range -- yyyy-mm-dd format

  Output(s):

    :return: articles `list[dict]` -- The list of all the article/documents that were published in
    the given date range.
  '''

  client = get_client()

  db = client.get_database(__DATABASE_NAME__)

  '''
  Sample Mongo shell query
  db.archives.find({
    $and: [{
        "pub_date": { $gt: "2000-04-01" }
    }, {
        "pub_date": { $lt: "2000-05-01" }
    }]
  })
  '''
  query = {
      __AND__: [
          {
              __PUB_DATE__: {
                  __GT__: begin_time
              }
          },
          {
              __PUB_DATE__: {
                  __LT__: end_time
              }
          }
      ]
  }

  cursor = db[__COLLECTION_NAME__].find(query)

  articles = list(cursor) if cursor is not None else None

  return articles


# Query#11: Find the organization that appears the most in NYT
def most_organization():
  '''
  most_organization() -> dict

  Query#11: Find the organization that appears the most in NYT

  Input(s):
    None

  Output(s):
    :return: organization `dict` -- The organization that appears the most in NYT
  '''

  client = get_client()

  db = client.get_database(__DATABASE_NAME__)

  organization = None

  '''
  Sample mongo shell query:
  db.month_4.aggregate([
    {
      $match: {
        $and: [
          {
            'keywords.name': "organizations"
          },
          {
            'keywords.value': {
              $regex: /.+/
            }
          }
        ]
      }
    },
    {
      $unwind: '$keywords'
    },
    {
      $group: {
        '_id': '$keywords.value',
        'organization_count': {
          $sum: 1
        }
      }
    },
    {
      $sort: {
        'organization_count': -1
      }
    }
  ])
  '''

  query = [
      {
          __UNWIND__: '${keywords}'.format(
              keywords=__KEYWORDS__)
      },
      {
          __MATCH__: {
              __AND__: [
                  {
                      '{keywords}.{name}'.format(
                          keywords=__KEYWORDS__,
                          name=__KEYWORDS_NAME__): 'organizations'
                  },
                  {
                      '{keywords}.{value}'.format(
                          keywords=__KEYWORDS__,
                          value=__KEYWORDS_VALUE__): {
                          __REGEX__: re.compile('.+', re.IGNORECASE)
                      }
                  }
              ]
          }
      },
      {
          __GROUP__: {
              __ID_OP__: '${keywords}.{value}'.format(
                  keywords=__KEYWORDS__,
                  value=__KEYWORDS_VALUE__),
              'organization_count': {
                  __SUM__: 1
              }
          }
      },
      {
          __SORT__: {
              'organization_count': -1
          }
      }
  ]

  cursor = db[__COLLECTION_NAME__].aggregate(query)

  organization = list(cursor) if cursor is not None else list()

  organization = organization[0] if len(organization) > 0 else None

  return organization


if __name__ == '__main__':
  basicConfig(format='%(asctime)s %(message)s')
  warning('Testing pymongo and mongo connections and queries...')
  # db = client.get_database('usdata')
  # print('All the collections of `usdata` db : {}'.format(db.collection_names()))
