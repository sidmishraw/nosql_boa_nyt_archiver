# nyt_archiver.py
# -*- coding: utf-8 -*-
# @Author: Sidharth Mishra
# @Date:   2017-05-05 10:52:40
# @Last Modified by:   Sidharth Mishra
# @Last Modified time: 2017-05-05 22:48:02


'''
``````````````````````````````````````````````````````````````````````````````
``````````````````````````````````````````````````````````````````````````````
`````````````````````````````NYT Archiver`````````````````````````````````````
``````````````````````````````````````````````````````````````````````````````
``````````````````````````````````````````````````````````````````````````````

Team name - Boa

Members:

Sidharth Mishra
Sonal Kabra
Weimeng Pu
Title - NYTArchiver

Dataset used:

NYT archives [from NYT Archives API]
Archives for years (2005-2007 and 2015-2017)

Dataset description:

The Archive API provides lists of NYT articles by month going back to 1851.
We plan to build our dataset by calling the NYT Archives API for the years 2005-2007 and 2015-2017.

Below is a sample document for the collection we will be building:

{
  "web_url": "http://www.nytimes.com/2000/04/01/business/a-role-model-s-new-clothes.html",
  "snippet": "Ever mindful of the need to stay au courant, Mattel staged a makeoverthis year for \
  Barbie, the 41-year-old fashion doll with $1.3 billion in annual sales. Sheemerged with a navel \
  and a smile that shows some teeth, giving her a slightly morenatural...",
  "lead_paragraph": "Ever mindful of the need to stay au courant, Mattel staged amakeover \
  this year for Barbie, the 41-year-old fashion doll with $1.3 billion in annual sales.She \
  emerged with a navel and a smile that shows some teeth, giving her a slightly morenatural \
  look. These were not, however, the only concessions to reality that Barbie'sdesigners have \
  felt compelled to make recently. In the last couple of years, Mattel Inc. hasbeen under \
  increasing pressure from some parents to lay aside Barbie's trademarkvagueness and make \
  her more career-oriented to build credibility -- not to mention sales --among the \
  primary-school set and their two-career parents.",
  "abstract": "Mattel Inc is trying to update Barbie's image, partly because ofcompetition \
  from more career-minded role models; in last couple of years, Mattel has beenunder increasing \
  pressure from some parents to lay aside Barbie's trademark vaguenessand make \
  her more career-oriented to build credibility among primary-school set and their \
  two-career parents; Mattel is packaging dolls with literature or CD-ROM's that \
  emphasizeeducation and other requirements for employability; shoppers can expect \
  to see Jessica theJournalist, Get Real Girl and Barbie for President; graphs; photos (M)",
  "print_page": "1",
  "blog": [],
  "source": "The New York Times",
  "multimedia": [],
  "headline": {
    "main": "A Role Model's New Clothes"
  },
  "keywords": [
    {
      "name": "organizations",
      "value": "MATTEL INC"
    },
    {
      "name": "subject",
      "value": "DOLLS"
    },
    {
      "name": "subject",
      "value": "TOYS"
    },
    {
      "name": "subject",
      "value": "BARBIE (DOLL)"
    }
  ],
  "pub_date": "2000-04-01T00:00:00Z",
  "document_type": "article",
  "news_desk": "Business/Financial Desk",
  "section_name": "Business",
  "subsection_name": null,
  "byline": {
    "person": [
      {
        "firstname": "Constance",
        "middlename": "L.",
        "lastname": "HAYS",
        "rank": 1,
        "role": "reported",
        "organization": ""
      }
    ],
    "original": "By CONSTANCE L. HAYS"
  },
  "type_of_material": "News",
  "_id": "4fd1f60b8eb7c8105d7504c0",
  "word_count": 1820,
  "slideshow_credits": null
}

Usecases :

1. Compare the top news keywords for the years 2015-2017 and 2005-2007 to see
what the news has been about. (Basically try and find the difference that has
come about in last 10 years.)
2. Find the most popular news keywords from the entire archives collection.
3. Search for articles based on user entry.
4. Find articles by reporter name.
5. Find articles about specific people or organizations. For eg - search forarticles
 about Leonardo Dicaprio etc.
6. Find the articles that have occured on page# x over these years.
7. Find the most productive reporter (reporter)
8. Find the longest article (page or word count)
9. Find the number of original article from NYT (source)
10. Find the articles published in certain time range (date)
11. Find the organization that appears the most in NYT (organization)
12. Find the section-name for which maximum number of articles written
13. List all the types of material with article count
14. Find which month had highest number of articles written
15. Find 10 most popular article in the given timeframe
'''

# Python standard lib imports
from argparse import ArgumentParser
from pprint import pprint

# NYT Archiver queries import
from nyt_queries import *
from nyt_queries import __DATABASE_NAME__
from nyt_queries import __COLLECTION_NAME__


def execute_query(*, query_index):
  '''
  execute_query(*, query_index)

  Executes the query for the given query index[1-15]
  '''

  if query_index == 1:
    phase1_words, phase2_words = compare_news_keywords()
    print('Phase 1 keywords and their counts:')
    pprint(phase1_words)
    print('Phase 2 keywords and their counts:')
    pprint(phase2_words)

  elif query_index == 2:
    news_keywords = most_popular_news_keywords()
    print('5 Most popular keywords are:')
    pprint(news_keywords)

  elif query_index == 3:
    user_entry = input('Input what you want to search within: ')
    articles = search_in_articles(user_entry)
    print('Matching articles:')
    pprint(articles)

  elif query_index == 4:
    first_name, middle_name, last_name = input(
        '''Please enter the First name, Middle name \
        and Last name of the reporter separated by spaces.
        Note: If the person doesn't have a middle name, please use `-` instead.
        For eg: Lays - Higgs
        Higgs Lays Mannaer
        Nicholas - Cage
        ''').split(' ')
    articles = search_articles_reporter_name(
        first_name=first_name, middle_name=middle_name, last_name=last_name)
    print('Articles by the person:')
    pprint(articles)

  elif query_index == 5:
    search_string = input(
        '''Input the organization or person's name to search: ''')
    flag_person = False if input('Is it a person? [Y/N]') == 'N' else True
    articles = search_people_or_organization(search_string, flag_person)
    print('Matching articles: ')
    pprint(articles)

  elif query_index == 6:
    page_number = int(input('Enter the page number to search for: '))
    articles = xpage_articles(page_number)
    print('Articles on the page#{} are:'.format(page_number))
    pprint(articles)

  elif query_index == 7:
    print('Most productive reporter: ')
    pprint(most_productive_reporter())

  elif query_index == 8:
    print('Longest article:')
    pprint(longest_article())

  elif query_index == 9:
    print('Number of original articles: ')
    pprint(count_original_articles())

  elif query_index == 10:
    begin_time, end_time = input(
        '''
        Enter the begin time of the range (yyyy-mm-dd) format
        and the end time of the range (yyyy-mm-dd) format.
        Both should be separated by spaces.
        For eg -
        2005-09-11 2006-10-01
        ::
        ''').split(' ')
    articles = articles_between(begin_time, end_time)
    print('Matching articles are:')
    pprint(articles)

  elif query_index == 11:
    print('Most frequent organization: ')
    pprint(most_organization())

  elif query_index == 12:
    print('Section with most number of articles: ')
    pprint(most_section())

  elif query_index == 13:
    print('Types of articles with their count: ')
    pprint(list_articles_type_of_materials())

  elif query_index == 14:
    date, count = highest_articles_month()
    print('{count} articles were published in {month}'.format(
        count=count,
        month=date
    ))

  elif query_index == 15:
    begin_time, end_time = input(
        '''
        Enter the begin time of the range (yyyy-mm-dd) format
        and the end time of the range (yyyy-mm-dd) format.
        Both should be separated by spaces.
        For eg -
        2005-09-11 2006-10-01
        ::
        ''').split(' ')
    articles = front_page_articles(begin_time, end_time)
    print('Matching articles are:')
    pprint(articles)

  else:
    print('Choose between 1 and 15 only...')

  return


if __name__ == '__main__':
  print('Starting NYT Archiver...')

  parser = ArgumentParser(description='NYT Archiver application')

  parser.add_argument('-host', '--hostname')
  parser.add_argument('-p', '--port')
  parser.add_argument('-db', '--database')
  parser.add_argument('-cl', '--collection')
  parser.add_argument('-f', '--force', action='store_true')

  args = parser.parse_args()

  database_name, collection_name, hostname, port, force_create = args.database, args.collection, \
      args.hostname, args.port, args.force

  # set db name and collection names
  set_db_collection_names(
      hostname=hostname, port=port, database_name=database_name, collection_name=collection_name)

  client = get_client()

  if force_create or __DATABASE_NAME__ not in client.database_names():
    print('Need to create the dataset...')
    try:
      create_archives_dataset()
    except Exception as e:
      warning('Failed to create dataset, check mongodb logs', e)

  consent = 'N'
  consent = input('Query the dataset? [Y/N]')

  while consent == 'Y':
    query_index = input('''
`````````````````````````````````````````````````
      `````````````````````````````````````````````````
      ::NYT Archiver::
      `````````````````````````````````````````````````
`````````````````````````````````````````````````
      Choose the query you want to execute [1-15]:

      1. Compare the top news keywords for the years 2015-2017 and 2005-2007 to see
      what the news has been about. (Basically try and find the difference that has
      come about in last 10 years.)

      2. Find the most popular news keywords from the entire archives collection.

      3. Search for articles based on user entry.

      4. Find articles by reporter name.

      5. Find articles about specific people or organizations. For eg - search forarticles
       about Leonardo Dicaprio etc.

      6. Find the articles that have occured on page# x over these years.

      7. Find the most productive reporter (reporter)

      8. Find the longest article (page or word count)

      9. Find the number of original article from NYT (source)

      10. Find the articles published in certain time range (date)

      11. Find the organization that appears the most in NYT (organization)

      12. Find the section-name for which maximum number of articles written

      13. List all the types of material with article count

      14. Find which month had highest number of articles written

      15. Find 10 most popular article in the given timeframe
      ''')

    execute_query(query_index=int(query_index))

    consent = input('Query again? [Y/N]')

  print('Thanks for using NYT Archiver...')
  warning('Shutting down...')
