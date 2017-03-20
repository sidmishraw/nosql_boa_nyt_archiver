# mongo_basic_demo1.py
# -*- coding: utf-8 -*-
# @Author: Sidharth Mishra
# @Date:   2017-03-15 12:36:16
# @Last Modified by:   Sidharth Mishra
# @Last Modified time: 2017-03-20 16:16:48




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




# Create
def create_archives_dataset():
  '''
  Creates the archives dataset by 
  '''

  client = get_client()

  # try building 2 collections for 2 months
  months = [4, 5]

  for month in months:
    archives = __archives_api__(month = month)
    db = client.get_database('archives_2000')
    db['month_{}'.format(month)].insert_many(archives)
    print('Created and inserted archives into month_{} collection..'.format(month))




# 3. Search for articles based on user entry.
# Querying mongodb
# for the demo, just going with month_4 collection
def search_in_articles(user_entry):
  '''
  Searches the articles on the `snippet`, `lead_paragraph` and `abstract` fields for the patterns
  sought by the user. This function returns the cursor object that can then be iterated to get all
  the matching articles.

  :param user_entry: The string or pattern the user is looking for. :class: `str`

  :return: cursor :class: `pymongo.cursor.Cursor`
  '''

  client = get_client()

  db = client.get_database('archives_2000')

  # db.month_4.find({$or: [{"lead_paragraph" :{$regex: /.*of the need.*/}},\
  # {"snippet" :{$regex: /.*of the need.*/}}]})
  query = {__OR__: [{__LEAD_PARAGRAPH__: {__REGEX__: '.*{}.*'.format('.*'.join(user_entry.split(' ')))}},\
  {__SNIPPET__: {__REGEX__: '.*{}.*'.format('.*'.join(user_entry.split(' ')))}},\
  {__ABSTRACT__: {__REGEX__: '.*{}.*'.format('.*'.join(user_entry.split(' ')))}}]}

  cursor = db['month_4'].find(query)

  return cursor




# 4. Find articles by reporter name.
def search_articles_reporter_name(first_name, middle_name, last_name):
  '''
  Searches the articles for the given name of reporter and then returns the cursor object which can
  then be iterated upon to get the matching list of articles.

  :param first_name: The first name of the reporter. :class: `str`
  :param middle_name: The middle name of the reporter. :class: `str`
  :param last_name: The last name of the reporter. :class: `str`

  :return: cursor :class:`pymongo.cursor.Cursor`
  '''

  client = get_client()

  db = client.get_database('archives_2000')

  # db.month_4.find({$and: [{'byline.person.firstname': 'Constance'}, \
  # {'byline.person.middlename': 'L.'}, {'byline.person.lastname': 'HAYS'},\
  # {'byline.person.role': 'reported'}]})
  query = {__AND__: [{'{}.{}.{}'.format(__BYLINE__, __PERSON__, __FIRSTNAME__): first_name},\
  {'{}.{}.{}'.format(__BYLINE__, __PERSON__, __MIDDLENAME__): middle_name},\
  {'{}.{}.{}'.format(__BYLINE__, __PERSON__, __LASTNAME__): last_name},\
  {'{}.{}.{}'.format(__BYLINE__, __PERSON__, __ROLE__): 'reported'}]}

  cursor = db['month_4'].find(query)

  return cursor




# 13. List all the types of material with article count
def list_articles_type_of_materials():
  '''
  This function will list all the articles according to the `type_of_material` field and their
  associated counts.

  :return: cursor :class: `pymongo.cursor.Cursor`
  '''

  client = get_client()

  db = client.get_database('archives_2000')

  # db.month_4.aggregate({$group: {'_id': '$type_of_material', 'count': {$sum: 1}}})
  pipeline_query = [{__GROUP__: {__ID_OP__: '${}'.format(__TYPE_OF_MATERIAL__),\
  'count': {__SUM__: 1}}}]

  cursor = db['month_4'].aggregate(pipeline_query)

  return cursor




# 7. Find the most productive reporter (reporter)
def most_productive_reporter():
  '''
  This function will return the json string for the most productive reporter. The productivity
  is defined by the number of articles the person appears in `byline.person` field with a 
  `reported` role.

  :return: most_productive_reporter :class: `str`
  '''

  client = get_client()

  db = client.get_database('archives_2000')

  # db.month_4.aggregate([{$match: {$and: [{'byline.person.role': "reported"}, \
  # {'byline.person.firstname': {$regex: /.+/}}]}}, {$unwind: '$byline.person'},\ 
  # {$group: {'_id': '$byline.person', 'article_count': {$sum: 1}}}, {$sort: {'article_count': -1}}])
  pipeline_query = [{__MATCH__: {__AND__: [\
  {'{}.{}.{}'.format(__BYLINE__, __PERSON__, __ROLE__): 'reported'},\
  {'{}.{}.{}'.format(__BYLINE__, __PERSON__, __FIRSTNAME__): {__REGEX__: '.+'}}]}},\
  {__UNWIND__: '${}.{}'.format(__BYLINE__, __PERSON__)},\
  {__GROUP__: {__ID_OP__: '${}.{}'.format(__BYLINE__, __PERSON__), 'article_count': {__SUM__: 1}}},\
  {__SORT__: {'article_count': -1}}]

  cursor = db['month_4'].aggregate(pipeline_query)

  most_productive_reporter = None

  if cursor != None:
    most_productive_reporter = next(cursor)

  return most_productive_reporter




if __name__ == '__main__':
  basicConfig(format = '%(asctime)s %(message)s')
  print('Testing pymongo and mongo connections and queries...')
  # db = client.get_database('usdata')
  # print('All the collections of `usdata` db : {}'.format(db.collection_names()))
  