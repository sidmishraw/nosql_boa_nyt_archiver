# CS 185C NoSQL Team Project

### Team name - Boa

### Members:
* Sidharth Mishra
* Sonal Kabra
* Weimeng Pu

### Title - NYTArchiver

### Dataset used:

NYT archives [from NYT Archives API]   
Archives for years (2005-2007 and 2015-2017)

### Dataset description:

The Archive API provides lists of NYT articles by month going back to 1851. We plan to build our dataset by calling the NYT Archives API for the years 2005-2007 and 2015-2017. Below is a sample document for the collection we will be building:

```javascript
{
  "web_url": "http://www.nytimes.com/2000/04/01/business/a-role-model-s-new-clothes.html",
  "snippet": "Ever mindful of the need to stay au courant, Mattel staged a makeoverthis year for Barbie, the 41-year-old fashion doll with $1.3 billion in annual sales. Sheemerged with a navel and a smile that shows some teeth, giving her a slightly morenatural...",
  "lead_paragraph": "Ever mindful of the need to stay au courant, Mattel staged amakeover this year for Barbie, the 41-year-old fashion doll with $1.3 billion in annual sales.She emerged with a navel and a smile that shows some teeth, giving her a slightly morenatural look. These were not, however, the only concessions to reality that Barbie'sdesigners have felt compelled to make recently. In the last couple of years, Mattel Inc. hasbeen under increasing pressure from some parents to lay aside Barbie's trademarkvagueness and make her more career-oriented to build credibility -- not to mention sales --among the primary-school set and their two-career parents.",
  "abstract": "Mattel Inc is trying to update Barbie's image, partly because ofcompetition from more career-minded role models; in last couple of years, Mattel has beenunder increasing pressure from some parents to lay aside Barbie's trademark vaguenessand make her more career-oriented to build credibility among primary-school set and theirtwo-career parents; Mattel is packaging dolls with literature or CD-ROM's that emphasizeeducation and other requirements for employability; shoppers can expect to see Jessica theJournalist, Get Real Girl and Barbie for President; graphs; photos (M)",
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
```


### Usecases :

1. Compare the top news keywords for the years 2015-2017 and 2005-2007 tosee what the news has been about. (Basically try and find the difference thathas come about in last 10 years.)
2. Find the most popular news keywords from the entire archives collection.
3. Search for articles based on user entry.
4. Find articles by reporter name.
5. Find articles about specific people or organizations. For eg - search forarticles about Leonardo Dicaprio etc.
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

