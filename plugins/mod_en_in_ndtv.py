#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: mod_en_in_ndtv.py
 Application: The NewsLookout Web Scraping Application
 Date: 2020-01-11
 Purpose: Plugin for NDTV


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
from bs4 import BeautifulSoup
# import lxml
# import cchardet

# import this project's python libraries:
from base_plugin import basePlugin
from scraper_utils import retainValidArticles
from data_structs import Types

##########

logger = logging.getLogger(__name__)


class mod_en_in_ndtv(basePlugin):
    """ Web Scraping plugin: mod_en_in_ndtv
    For NDTV
    """

    # define a minimum count of characters for text body, article content below this limit will be ignored
    minArticleLengthInChars = 250

    # implies web-scraper for news content, see data_structs.py for other types
    pluginType = Types.MODULE_NEWS_CONTENT

    # main webpage URL
    mainURL = "https://www.ndtv.com/business?pfrom = home-ndtv_header-globalnav"

    # RSS feeds to pick up latest news article links
    all_rss_feeds = ['https://feeds.feedburner.com/ndtvprofit-latest',
                     'https://feeds.feedburner.com/ndtvnews-latest']

    # fetch only URLs containing the following substrings:
    validURLStringsToCheck = ['www.ndtv.com/']

    # this list of URLs will be visited to get links for articles,
    # but their content will not be scraped to pick up news content
    nonContentURLs = [mainURL,
                      'https://www.ndtv.com/',
                      'https://www.ndtv.com/?pfrom = ',
                      'https://www.ndtv.com/budget?pfrom = ',
                      'https://www.ndtv.com/topic/stock-talk',
                      'https://www.ndtv.com/business/marketdata',
                      'https://www.ndtv.com/business',
                      'https://www.ndtv.com/business/latest',
                      'https://www.ndtv.com/business/marketdata',
                      'https://www.ndtv.com/business/your-money',
                      'https://www.ndtv.com/business/corporates',
                      'https://www.ndtv.com/business/life-and-careers',
                      'https://www.ndtv.com/business/tech-media-telecom',
                      'https://www.ndtv.com/business/banking-finance',
                      'https://www.ndtv.com/business/consumer-products',
                      'https://www.ndtv.com/business/pharma',
                      'https://www.ndtv.com/business/energy',
                      'https://www.ndtv.com/business/infrastructure',
                      'https://www.ndtv.com/business/people',
                      'https://www.ndtv.com/business/economic-indicators',
                      'https://www.ndtv.com/business/economic-policy',
                      'https://www.ndtv.com/business/global-economy',
                      'https://www.ndtv.com/business/industries',
                      'https://www.ndtv.com/business/auto',
                      'https://www.ndtv.com/convergence/ndtv/new/NDTVNewsAlert.aspx',
                      'https://www.ndtv.com/convergence/ndtv/new/Complaint.aspx',
                      'https://www.ndtv.com/convergence/ndtv/new/feedback.aspx',
                      'https://www.ndtv.com/convergence/ndtv/new/disclaimer.aspx',
                      'https://www.ndtv.com/careers/'
                      ]

    # never fetch URLs containing these strings:
    invalidURLSubStrings = ['//www.ndtv.com/entertainment/',
                            '//www.ndtv.com/video/',
                            'https://www.ndtv.com/convergence/ndtv/new/codeofethics.aspx',
                            'https://www.ndtv.com/convergence/ndtv/new/disclaimer.aspx',
                            'https://www.ndtv.com/business/gadgets',
                            'https://www.ndtv.com/convergence/ndtv/new/dth.aspx',
                            'https://www.ndtv.com/sites/ureqa-epg-ott/',
                            'https://www.ndtv.com/convergence/ndtv/advertise/ndtv_leaderboard.aspx',
                            'https://www.ndtv.com/convergence/ndtv/corporatepage/index.aspx'
                            ]

    # write regexps in three groups ()()() so that the third group
    # gives a unique identifier such as a long integer at the end of a URL
    # this third group will be selected as the unique identifier:
    urlUniqueRegexps = [r'(^http.+\/\/)(www.ndtv.com\/.+\-)([0-9]{5,})']

    # write the following regexps dict with each key as regexp to match the required date text,
    # group 2 of this regular expression should match the date string
    # in this dict, put the key will be the date format expression
    # to be used for datetime.strptime() function, refer to:
    # https://docs.python.org/3/library/datetime.html#datetime.datetime.strptime
    articleDateRegexps = {
        # content = "2021-02-26T17:45:55+05:30"
        r"(content = \")(20[0-9]{2}\-[0-9]{2}\-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(\+05:30\")": "%Y-%m-%dT%H:%M:%S",
        # Updated: February 26, 2021 5:45 pm IST
        r"(Updated: )([a-zA-Z]+ [0-9]{1,2}, 20[0-9]{2} [0-9]{1,2}:[0-9]{2})( [a-zA-Z]{2} IST)": "%B %d, %Y %H:%M"
        }

    invalidTextStrings = []

    allowedDomains = ["www.ndtv.com"]

    articleIndustryRegexps = []

    authorRegexps = []

    # members used by functions of the class:
    authorMatchPatterns = []
    urlMatchPatterns = []
    dateMatchPatterns = dict()
    listOfURLS = []

    def __init__(self):
        """ Initialize the object """
        super().__init__()

    def getArticlesListFromRSS(self):
        """ extract Article listing using the BeautifulSoup library
        to identify the list from its RSS feed
        """
        # <item>
        # <link><![CDATA[https://www.ndtv.com/business/sbi-readies-mutual-fund-venture-for-ipo-2379481]]></link>
        for thisFeedURL in self.all_rss_feeds:
            try:
                rawData = self.networkHelper.fetchRawDataFromURL(thisFeedURL, self.pluginName)

                rss_feed_xml = BeautifulSoup(rawData, 'lxml-xml')

                for item in rss_feed_xml.channel:
                    if item.name == "item":
                        link_contents = item.link.contents[0]
                        self.listOfURLS.append(link_contents)

            except Exception as e:
                logger.error("%s: Error getting urls listing from RSS feed %s: %s",
                             self.pluginName,
                             thisFeedURL,
                             e)
        self.listOfURLS = retainValidArticles(self.listOfURLS,
                                              self.validURLStringsToCheck)

    def extractArticleBody(self, htmlContent):
        """ Extract article's text using the Beautiful Soup library
        """
        body_text = ""
        try:
            # get article text data by parsing specific tags:
            docRoot = BeautifulSoup(htmlContent, 'lxml')
            section = docRoot.find_all(class_=['ins_storybody', 'content_text row description', 'fullstoryCtrl_fulldetails'])
            paragraphList = []

            for node in section:
                paragraphList = paragraphList + node.find_all('p', text=True)

            for item in paragraphList:
                body_text = body_text + str(item.get_text())

            section = docRoot.findAll('span', {"itemprop": 'articleBody'})
            if len(section) > 0:
                for item in section:
                    body_text = body_text + str(item.get_text())

            section = docRoot.findAll('div', {"itemprop": 'articleBody'})
            if len(section) > 0:
                for item in section:
                    body_text = body_text + str(item.get_text())

        except Exception as e:
            logger.error("Exception extracting article via tags: %s", e)
        return(body_text)

    def extractArticleTitle(self, htmlContent):
        """ Extract article's text using the Beautiful Soup library
        """
        title_text = ""
        try:
            # get article text data by parsing specific tags:
            docRoot = BeautifulSoup(htmlContent, 'lxml')

            section = docRoot.findAll('h1', {"itemprop": 'headline'})
            if len(section) > 0:
                for item in section:
                    title_text = title_text + str(item.get_text())
        except Exception as e:
            logger.error("Exception extracting article via tags: %s", e)
        return(title_text)

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

# # end of file ##
