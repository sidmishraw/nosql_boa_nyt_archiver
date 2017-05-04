# midterm_data_populator.py
# -*- coding: utf-8 -*-
# @Author: Sidharth Mishra
# @Date:   2017-03-31 22:48:18
# @Last Modified by:   Sidharth Mishra
# @Last Modified time: 2017-05-04 10:23:45


'''
This script is used to populate data into the sharding cluster made for midterm take home exam.
The data populated will be for 1 month from NYT archives API.
'''


# Python Standard Library imports
from urllib.request import urlopen
from json import loads
from json import dumps
from datetime import datetime


# pymongo imports
from pymongo import MongoClient


# Constants
__HOSTNAME__ = 'ec2-54-245-184-166.us-west-2.compute.amazonaws.com'
__PORT__ = '27021'
__NYT_API_BASE_PATH__ = 'http://api.nytimes.com/svc/'
__API_KEY__ = 'c9e169f173f34249a02bc0aff8d50fb9'
__ARCHIVES_DOC_KEY__ = 'docs'
__RESPONSE__ = 'response'
__DB__ = 'archivesdb'
__COLLECTION__ = 'month4'


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
  Connects to the MongoDB instance and returns the MongoDBclient.

  :return: client `MongoClient`
  '''

  client = MongoClient('mongodb://{hostname}:{port}'.format(hostname=__HOSTNAME__,
                                                            port=__PORT__))

  return client


# Creation of archives dataset
# NYT Arcives API for fetching data from NYT
def __archives_api__(year=2000, month=4):
  '''
  Calls the NYT archives API and generates the sample dataset inside `nyt_archives.json`.
  Takes year and month as the arguments.

  :param: year `int` - Default value = 2000 - The year of the archives
  :param: month `int` - Default value = 4 - The month of the archives

  :return: archives `list`
  '''

  url_string = '{base_url}archive/v1/{year}/{month}.json?api-key={api_key}'.format(
      base_url=__NYT_API_BASE_PATH__, year=year, month=month, api_key=__API_KEY__)

  json_string = None

  with urlopen(url_string) as archive_res:
    json_string = archive_res.read()

  return loads(json_string)[__RESPONSE__][__ARCHIVES_DOC_KEY__]


# dumping function
def dump_data_scluster():
  '''
  Calls the NYT archives API and dumps all the archives into the sharded cluster.

  :return: `None`
  '''

  client = get_client()
  archives = __archives_api__()
  db = client.get_database(__DB__)
  db[__COLLECTION__].insert_many(archives)
  print('Created and inserted archives into {} collection..'.format(__COLLECTION__))
  return


# query 5
def query_5(flag_person=False, search_string):
  '''
  docs -- ignore for now
  ~ sid
  '''

  client = get_client()

  db = client.get_database(__DATABASE_NAME__)

  '''
  Mongo shell sample query:

  //#5 -- Organization
  db.month_4.find({
      "keywords": {
          $elemMatch: {
              "name": "organizations",
              "value": {
                  $regex: /.*MATTEL.*/i
              }
          }
      }
  })

  //#5 -- People
  db.month_4.find({
      "keywords": {
          $elemMatch: {
              "name": "persons",
              "value": {
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

  return cursor

if __name__ == '__main__':
  print('Dumping data into Sharding cluster running on {hostname}:{port}'.format(
      hostname=__HOSTNAME__, port=__PORT__))
  dump_data_scluster()
  print('Done dumping data. Please check the cluster...')
