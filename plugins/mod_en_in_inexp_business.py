#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: mod_en_in_inexp_business.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-01-14
 Purpose: Plugin for the Indian Express, Business
 Copyright 2021, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu, sandeep.sandhu@gmx.com


 DISCLAIMER: This software is intended for demonstration and educational purposes only.
 Before using it for web scraping any website, always consult that website's terms of use.
 Do not use this software to fetch any data from any website that has forbidden use of web
 scraping or similar mechanisms, or violates its terms of use in any other way. The author is
 not responsible for such kind of inappropriate use of this software.
"""

##########

# import standard python libraries:
from datetime import datetime
import logging

# import web retrieval and text processing python libraries:
from bs4 import BeautifulSoup

# import this project's python libraries:
from base_plugin import basePlugin
from scraper_utils import cutStrBetweenTags, calculateCRC32
from data_structs import Types

##########

logger = logging.getLogger(__name__)

##########


class mod_en_in_inexp_business(basePlugin):
    """ Web Scraping plugin: mod_en_in_inexp_business
    For Indian Express Newspaper
    """

    minArticleLengthInChars = 250

    pluginType = Types.MODULE_NEWS_CONTENT  # implies web-scraper for news content

    mainURL = 'https://www.newindianexpress.com/business/'

    all_rss_feeds = ["https://www.newindianexpress.com/Nation/rssfeed/?id=170&getXmlFeed=true"]

    # fetch only URLs containing the following substrings:
    validURLStringsToCheck = ['https://www.newindianexpress.com/nation/',
                              'business',
                              'https://www.newindianexpress.com/opinions/',
                              'https://www.newindianexpress.com/world/',
                              'https://indianexpress.com/']

    nonContentStrings = ['https://www.newindianexpress.com/opinions/']

    # get URL links from these URLs but done fetch content from them:
    nonContentURLs = ['https://www.newindianexpress.com/opinions/editorials',
                      'https://www.newindianexpress.com/opinions/columns',
                      'https://www.newindianexpress.com/opinions/columns/karamatullah-k-ghori',
                      'https://www.newindianexpress.com/opinions/columns/shampa-dhar-kamath',
                      'https://www.newindianexpress.com/opinions/columns/shankkar-aiyar',
                      'https://www.newindianexpress.com/opinions/columns/ravi-shankar',
                      'https://www.newindianexpress.com/opinions/columns/s-gurumurthy',
                      'https://www.newindianexpress.com/opinions/columns/t-j-s-george']

    # never fetch these URLs:
    invalidURLSubStrings = []

    urlUniqueRegexps = [r"(^https.*)(\-)([0-9]+)(\.html$)",
                        r"(^https\://indianexpress.com/article/.*)(\-)([0-9]+)(/$)",
                        r"(^https\://indianexpress.com/article/.*)(\-)([0-9]+)(\.html$)"
                        ]

    invalidTextStrings = []

    articleDateRegexps = dict()
    authorRegexps = []
    dateMatchPatterns = dict()
    urlMatchPatterns = []
    authorMatchPatterns = []

    allowedDomains = ["indianexpress.com", "www.newindianexpress.com"]
    listOfURLS = []
    uRLdata = dict()
    urlMatchPatterns = []

    def __init__(self):
        """ Initialize the object
        Use base class's lists and dicts in searching for unique url and published date strings
        """
        self.articleDateRegexps.update(basePlugin.articleDateRegexps)
        self.urlUniqueRegexps = super().urlUniqueRegexps + self.urlUniqueRegexps
        super().__init__()

    def extractUniqueIDFromURL(self, uRLtoFetch):
        """ extract Unique ID From URL
        """
        uniqueString = ""
        try:
            # calculate CRC string if unique identifier cannot be located in the URL:
            uniqueString = str(calculateCRC32(uRLtoFetch.encode('utf-8')))

        except Exception as e:
            logger.error("Error calculating CRC32 of URL: %s , URL was: %s",
                         e,
                         uRLtoFetch.encode('ascii', 'ignore'))

        if len(uRLtoFetch) > 6:
            for urlPattern in self.urlMatchPatterns:
                try:
                    result = urlPattern.search(uRLtoFetch)
                    uniqueString = result.group(3)
                    # if not error till this point then exit
                    break

                except Exception as e:
                    logger.debug("Error identifying unique ID of URL: %s , URL was: %s, Pattern: %s",
                                 e,
                                 uRLtoFetch.encode('ascii'),
                                 urlPattern)
        else:
            logger.error("Giving up identifying unique ID for URL: %s", uRLtoFetch.encode('ascii'))
        return(uniqueString)

    def extractIndustries(self, uRLtoFetch, htmlText):
        """  Extract Industries relevant to the article from URL or html content
        """
        industries = []
        try:
            logger.debug("Extracting industries identified by the article.")
            # docRoot = BeautifulSoup(htmlText, 'lxml')
            # section = article_html.find( "span", "ag")

        except Exception as e:
            logger.error("When extracting industries: %s", e)
        return(industries)

    def extractAuthors(self, htmlText):
        """ Extract Authors/Agency/Source from html
        """
        authors = []
        try:
            strNewsAgent = cutStrBetweenTags(htmlText, '<span class = "author_des">By', '</span></span>')
            strNewsAgent = cutStrBetweenTags(strNewsAgent, 'target = "_blank">', '</a>')

            if len(strNewsAgent) < 1:
                raise Exception("Could not identify news agency/source.")
            else:
                authors = [strNewsAgent]

        except Exception as e:
            logger.error("Error extracting news agent from text: %s", e)
        return(authors)

    def extractPublishedDate(self, htmlText):
        """ Extract Published Date from html
        """
        # default is todays date-time:
        date_obj = datetime.now()
        # extract published date
        strJSDatePart = cutStrBetweenTags(htmlText, '"datePublished":"', '+05:30","dateModified"')

        try:
            if len(strJSDatePart) > 0:
                date_obj = datetime.strptime(strJSDatePart, '%Y-%m-%dT%H:%M:%S')
            else:
                logger.error("Error parsing published date text: %s", strJSDatePart)

        except Exception as e:
            logger.error("Error parsing published date string (%s) to date object: %s",
                         strJSDatePart,
                         e)

        return(date_obj)

    def extractArticleBody(self, htmlContent):
        """ Extract article's text using the Beautiful Soup library """
        articleText = ""
        try:
            # get article text data by parsing specific tags:
            article_html = BeautifulSoup(htmlContent, 'lxml')

            # <div id = "storyContent" class = "articlestorycontent">
            body_root = article_html.find_all("div", "articlestorycontent")
            if len(body_root) > 0:
                articleText = body_root[0].getText()
            # except Warning as w:
            #    logger.warn("Warning when extracting text via BeautifulSoup: %s", w)

        except Exception as e:
            logger.error("Exception extracting article via tags: %s", e)

        return(articleText)

# # end of file ##
