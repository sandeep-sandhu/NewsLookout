#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: mod_en_in_livemint.py
 Application: The NewsLookout Web Scraping Application
 Date: 2020-01-11
 Purpose: Plugin for LiveMint


 DISCLAIMER: This software is intended for demonstration and educational purposes only.
 Before using it for web scraping any website, always consult that website's terms of use.
 Do not use this software to fetch any data from any website that has forbidden use of web
 scraping or similar mechanisms, or violates its terms of use in any other way. The author is
 not responsible for such kind of inappropriate use of this software.
"""

##########

# import standard python libraries:
import logging

# import web retrieval and text processing python libraries:
# from bs4 import BeautifulSoup
# import lxml
# import cchardet

# import this project's python libraries:
from base_plugin import basePlugin
# from scraper_utils import retainValidArticles
from data_structs import Types

##########

logger = logging.getLogger(__name__)


class mod_en_in_livemint(basePlugin):
    """ Web Scraping plugin: mod_en_in_livemint
    For Live mint
    """

    # define a minimum count of characters for text body, article content below this limit will be ignored
    minArticleLengthInChars = 400

    # implies web-scraper for news content, see data_structs.py for other types
    pluginType = Types.MODULE_NEWS_CONTENT

    # main webpage URL
    mainURL = "https://www.livemint.com/latest-news"

    # RSS feeds to pick up latest news article links
    all_rss_feeds = ['https://www.livemint.com/rss/markets',
                     'https://www.livemint.com/rss/money']

    # fetch only URLs containing the following substrings:
    validURLStringsToCheck = ['www.livemint.com']

    # never fetch URLs containing these strings:
    invalidURLSubStrings = [
                            'https://www.livemint.com/politics/news/',
                            'https://www.livemint.com/sports/',
                            'https://www.livemint.com/videos/',
                            'https://www.livemint.com/food/',
                            'https://www.livemint.com/how-to-lounge/art-culture/',
                            'https://www.livemint.com/food/cook/',
                            'https://www.livemint.com/how-to-lounge/',
                            'https://www.livemint.com/relationships/',
                            'https://lifestyle.livemint.com/fashion/',
                            'https://lifestyle.livemint.com/smart-living/',
                            'https://lifestyle.livemint.com/food/discover/',
                            'https://www.livemint.com/static/code-of-ethics',
                            'https://www.livemint.com/static/disclaimer',
                            'https://www.livemint.com/static/subscriber-tnc',
                            '/termsofuse.html',
                            '/contactus.html',
                            '/aboutus.html',
                            'mintiphone.page.link'
                            ]

    # this list of URLs will be visited to get links for articles,
    # but their content will not be scraped to pick up news content
    nonContentURLs = [mainURL,
                      'https://www.livemint.com/mostpopular',
                      'https://www.livemint.com/companies/news',
                      'https://www.livemint.com/budget/news',
                      'https://www.livemint.com/auto-news',
                      'https://www.livemint.com/mint-lounge',
                      'https://www.livemint.com/mutual-fund/mf-news',
                      'https://www.livemint.com/mint-lounge/business-of-life',
                      'https://www.livemint.com/brand-stories',
                      'https://www.livemint.com/politics',
                      'https://www.livemint.com/news/opinion',
                      'https://www.livemint.com/opinion/',
                      'https://www.livemint.com/topic/mint-views-',
                      'https://www.livemint.com/news/talking-point',
                      'https://www.livemint.com/brand-post'
                      ]

    # write regexps in three groups ()()() so that the third group
    # gives a unique identifier such as a long integer at the end of a URL
    # this third group will be selected as the unique identifier:
    # urlUniqueRegexps = []

    # write the following regexps dict with each key as regexp to match the required date text,
    # group 2 of this regular expression should match the date string
    # in this dict, put the key will be the date format expression
    # to be used for datetime.strptime() function, refer to:
    # https://docs.python.org/3/library/datetime.html#datetime.datetime.strptime
    # For examples, see this dictionary's key-value pairs defined in the basePlugin class
    # articleDateRegexps = {}

    invalidTextStrings = []

    allowedDomains = ['www.livemint.com']

    articleIndustryRegexps = []

    authorRegexps = []

    # members used by functions of the class:
    authorMatchPatterns = []
    urlMatchPatterns = []
    dateMatchPatterns = dict()
    listOfURLS = []

    # --- Methods to be implemented ---

    # *** MANDATORY to implement ***
    def extractIndustries(self, uRLtoFetch, htmlText):
        """ Extract the industry of the articles from its URL or contents
        """
        industries = []

        return(industries)

    # *** MANDATORY to implement ***
    def extractAuthors(self, htmlText):
        """ extract the author from the html content
        """
        authors = []

        return(authors)

    # *** MANDATORY to implement ***
    def extractArticleBody(self, htmlContent):
        """ extract the text body of the article
        """
        body_text = ""

        return(body_text)

# # end of file ##
