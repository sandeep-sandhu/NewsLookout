#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: mod_en_in_business_std.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-01-14
 Purpose: Business Standard Newspaper
 Copyright 2021, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu, sandeep.sandhu@gmx.com



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
# from scraper_utils import getNetworkLocFromURL
from data_structs import Types

##########

logger = logging.getLogger(__name__)


class mod_en_in_business_std(basePlugin):
    """ Web Scraping plugin: mod_en_in_business_std
    Description: Business Standard
    Language: English
    Country: India
    """

    # define a minimum count of characters for text body, article content below this limit will be ignored
    minArticleLengthInChars = 400

    # implies web-scraper for news content, see data_structs.py for other types
    pluginType = Types.MODULE_NEWS_CONTENT

    # main webpage URL
    mainURL = "https://www.business-standard.com/"

    # RSS feeds to pick up latest news article links
    all_rss_feeds = ['https://www.business-standard.com/rss/home_page_top_stories.rss',
                     'https://www.business-standard.com/rss/latest.rss',
                     'https://www.business-standard.com/rss/markets-106.rss',
                     'https://www.business-standard.com/rss/economy-policy-102.rss',
                     'https://www.business-standard.com/rss/finance-103.rss']

    # fetch only URLs containing the following substrings:
    validURLStringsToCheck = ['www.business-standard.com/article/']

    nonContentURLs = [
                      mainURL
                      ]

    # never fetch URLs containing these strings:
    invalidURLSubStrings = []

    urlUniqueRegexps = [r'(^http.+\/\/)(www.business\-standard.com\/.+\-)([0-9]{5,})']

    # write the following regexps dict with each key as regexp to match the required date text,
    # group 2 of this regular expression should match the date string
    # in this dict, put the key will be the date format expression
    # to be used for datetime.strptime() function, refer to:
    # https://docs.python.org/3/library/datetime.html#datetime.datetime.strptime
    articleDateRegexps = {
        # "datePublished": "2021-02-25T22:59:00+05:30"
        r"(\"datePublished\": \")(20[0-9]{2}\-[0-9]{2}\-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(\+05:30\")":
        "%Y-%m-%dT%H:%M:%S",

        # content = "Fri, 26 Feb 2021 02:33:38 +0530">
        r"(content = \")([a-zA-Z]{3}, [0-9]{1,2} [a-zA-Z]{3} 20[0-9]{2} [0-9]{1,2}:[0-9]{2}:[0-9]{2} \+0530)(\">)":
        "%a, %d %b %Y %H:%M:%S %z"}

    invalidTextStrings = ['Support quality journalism and subscribe to Business Standard']

    allowedDomains = ['www.business-standard.com']

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
