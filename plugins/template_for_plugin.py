#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: template_for_plugin.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-01-14
 Purpose: Template to aid writing a custom plugin for the application
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
from data_structs import Types

from base_plugin import basePlugin

##########

logger = logging.getLogger(__name__)


class myplugin(basePlugin):
    """ Web Scraping plugin: mod_<lang>_<country>_myplugin
    Description:
    Language: English
    Country: India
    """

    # define a minimum count of characters for text body, article content below this limit will be ignored
    minArticleLengthInChars = 400

    # implies web-scraper for news content, see data_structs.py for other types
    pluginType = Types.MODULE_NEWS_CONTENT

    # main webpage URL
    mainURL = ""

    # RSS feeds to pick up latest news article links
    all_rss_feeds = []

    # fetch only URLs containing the following substrings:
    validURLStringsToCheck = []

    # this list of URLs will be visited to get links for articles,
    # but their content will not be scraped to pick up news content
    nonContentURLs = [
        mainURL
       ]

    # never fetch URLs containing these strings:
    invalidURLSubStrings = []

    # write regexps in three groups ()()() so that the third group
    # gives a unique identifier such as a long integer at the end of a URL
    # this third group will be selected as the unique identifier:
    urlUniqueRegexps = [
                        r'(http.+\/\/)(www\..+\.com\/.+\-)([0-9]{5,})',
                        r'(http.+\/\/)(www\..+\.com\/.+\-)([0-9]{5,})(\.html)',
                        r'(http.+\/\/)(www\..+\.in\/.+\/)([0-9]{5,})(\.html)',
                        r'(http.+\/\/)(www\..+\.in\/.+\-)([0-9]{5,})',
                        r'(http.+\/\/)(www\..+\.in\/.+\/)([0-9]{5,})',
                        ]

    # write the following regexps dict with each key as regexp to match the required date text,
    # group 2 of this regular expression should match the date string
    # in this dict, put the key will be the date format expression
    # to be used for datetime.strptime() function, refer to:
    # https://docs.python.org/3/library/datetime.html#datetime.datetime.strptime
    articleDateRegexps_custom = {}

    # merge base class's attribute with this one
    articleDateRegexps = {**super().articleDateRegexps, **articleDateRegexps_custom}

    invalidTextStrings = []

    allowedDomains = []

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
