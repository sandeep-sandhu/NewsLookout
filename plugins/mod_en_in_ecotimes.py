#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: mod_en_in_ecotimes.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-01-14
 Purpose: Plugin for the Economic Times
 Copyright 2021, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu

 Provides:
    mod_en_in_ecotimes
        extractIndustries
        extractAuthors
        extractPublishedDate
        extractArticleBody
        extractArticleBodyFormat1
        extractArticleBodyFormat2
        extractArticleBodyFormat3
        extractArticleBodyFormat4
        extractUniqueIDFromURL


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
from scraper_utils import getNetworkLocFromURL
from data_structs import Types

##########

logger = logging.getLogger(__name__)


class mod_en_in_ecotimes(basePlugin):
    """ Web Scraping plugin: mod_en_in_ecotimes
    For Economic times Newspaper
    Language: English
    Country: India
    """

    minArticleLengthInChars = 400

    pluginType = Types.MODULE_NEWS_CONTENT  # implies web-scraper for news content

    mainURL = 'https://economictimes.indiatimes.com/industry'

    # rss_feed = "https://economictimes.indiatimes.com/rssfeedsdefault.cms"
    all_rss_feeds = ['https://economictimes.indiatimes.com/rssfeedsdefault.cms',
                     'https://economictimes.indiatimes.com/rssfeedstopstories.cms',
                     'https://economictimes.indiatimes.com/news/latest-news/rssfeeds/20989204.cms'
                     ]

    # fetch only URLs containing the following substrings:
    validURLStringsToCheck = ['economictimes.indiatimes.com/']

    # never fetch URLs containing these strings:
    invalidURLSubStrings = ['//www.indiatimes.com/',
                            '//timesofindia.indiatimes.com/',
                            '//economictimes.indiatimes.com/et-search/',
                            '//economictimes.indiatimes.com/hindi',
                            '/videoshow/',
                            '/slideshow/',
                            '/podcast/',
                            'economictimes.indiatimes.com/terms-conditions',
                            'economictimes.indiatimes.com/privacypolicy.cms',
                            'economictimes.indiatimes.com/codeofconduct.cms',
                            'economictimes.indiatimes.com/plans.cms',
                            'https://economictimes.indiatimes.com/subscription',
                            '/slideshowlist/'
                            ]

    # get URL links from these URLs but done fetch content from them:
    nonContentURLs = ['https://economictimes.indiatimes.com/marketstats/'
                      + 'duration-1d,marketcap-largecap,pageno-1,pid-0,sort-intraday,sortby-percentchange,sortorder-desc.cms',
                      'https://economictimes.indiatimes.com/marketstats/'
                      + 'pid-40,exchange-nse,sortby-value,sortorder-desc.cms',
                      'https://economictimes.indiatimes.com/marketstats/'
                      + 'pid-0,pageno-1,sortby-percentchange,sortorder-desc,sort-intraday.cms',
                      'https://economictimes.indiatimes.com/personal-finance',
                      'https://economictimes.indiatimes.com/mutual-funds',
                      'https://economictimes.indiatimes.com/amazon.cms',
                      'https://economictimes.indiatimes.com/breakingnewslist.cms',
                      'https://economictimes.indiatimes.com/environment',
                      'https://economictimes.indiatimes.com/et-now',
                      'https://economictimes.indiatimes.com/et-now/auto',
                      'https://economictimes.indiatimes.com/et-now/brand-equity',
                      'https://economictimes.indiatimes.com/et-now/commodities',
                      'https://economictimes.indiatimes.com/et-now/corporate',
                      'https://economictimes.indiatimes.com/et-now/daily',
                      'https://economictimes.indiatimes.com/et-now/entertainment',
                      'https://economictimes.indiatimes.com/et-now/experts',
                      'https://economictimes.indiatimes.com/et-now/finance',
                      'https://economictimes.indiatimes.com/et-now/markets',
                      'https://economictimes.indiatimes.com/et-now/policy',
                      'https://economictimes.indiatimes.com/et-now/results',
                      'https://economictimes.indiatimes.com/et-now/stocks',
                      'https://economictimes.indiatimes.com/et-now/technology',
                      'https://economictimes.indiatimes.com/industry/auto',
                      'https://economictimes.indiatimes.com/industry/cons-products',
                      'https://economictimes.indiatimes.com/industry/energy',
                      'https://economictimes.indiatimes.com/industry/healthcare/biotech',
                      'https://economictimes.indiatimes.com/industry/indl-goods/svs',
                      'https://economictimes.indiatimes.com/industry/media/entertainment',
                      'https://economictimes.indiatimes.com/industry/services',
                      'https://economictimes.indiatimes.com/industry/telecom',
                      'https://economictimes.indiatimes.com/industry/transportation',
                      'https://economictimes.indiatimes.com/markets/forex/indian-rupee',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker = 1',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker = 2',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker = 3',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker = 4',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker = 5',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker = 6',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker = 7',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker = 8',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker = 9',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker = a',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker = b',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker = c',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker = d',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker = e',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker = f',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker = g',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker = h',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker = i',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker = j',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker = k',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker = l',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker = m',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker = o',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker = p',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker = q',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker = r',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker = s',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker = t',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker = u',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker = v',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker = w',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker = x',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker = y',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker = z',
                      'https://economictimes.indiatimes.com/mobile',
                      'https://economictimes.indiatimes.com/news/indian-navy-news',
                      'https://economictimes.indiatimes.com/news/india-unlimited/csr',
                      'https://economictimes.indiatimes.com/news/irctc-news',
                      'https://economictimes.indiatimes.com/nri',
                      'https://economictimes.indiatimes.com/opinion',
                      'https://economictimes.indiatimes.com/panache',
                      'https://economictimes.indiatimes.com/small-biz/topmsmes',
                      'https://economictimes.indiatimes.com/spotlight/greatmanagerawards.cms',
                      'https://economictimes.indiatimes.com/stock-price-alerts/pricealerts.cms',
                      'https://economictimes.indiatimes.com/tech',
                      'https://economictimes.indiatimes.com/tech/funding',
                      'https://economictimes.indiatimes.com/tech/startups',
                      'https://economictimes.indiatimes.com/tech/tech-bytes',
                      'https://economictimes.indiatimes.com/tomorrowmakers.cms'
                      ]

    urlUniqueRegexps = [r"(http.+\/economictimes\.indiatimes\.com)(.*\/)([0-9]+)(\.cms)",
                        r"(\.economictimes\.indiatimes\.com\/)(.+\/)([0-9]+)"
                        ]

# articleDateRegexps = {
# # Thu, 23 Jan 2020 11:00:00 +0530
# r"(<meta name = \"created-date\" content = \")([a-zA-Z]{3}, [0-9]{1,2} [a-zA-Z]{3} 20[0-9]{2} [0-9]{1,2}:[0-9]{2}:[0-9]{2}
# \+0530)(\" \/>)": "%a, %d %b %Y %H:%M:%S %z"
# # Thu, 23 Jan 2020 11:00:00 +0530
# , r"(<meta name = \"publish-date\" content = \")([a-zA-Z]{3}, [0-9]{1,2} [a-zA-Z]{3} 20[0-9]{2} [0-9]{1,2}:[0-9]{2}:[0-9]{2}
# \+0530)(\" \/>)": "%a, %d %b %Y %H:%M:%S %z"
# # Thu, 23 Jan 2020 11:00:00 +0530
# , r"(\"datePublished\":\")([a-zA-Z]{3}, [0-9]{1,2} [a-zA-Z]{3} 20[0-9]{2} [0-9]{1,2}:[0-9]{2}:[0-9]{2} \+0530)(\")":
# "%a, %d %b %Y %H:%M:%S %z"
# # Thu, 23 Jan 2020 12:05:00 +0530
# , r"(\"dateModified\":\")([a-zA-Z]{3}, [0-9]{1,2} [a-zA-Z]{3} 20[0-9]{2} [0-9]{1,2}:[0-9]{2}:[0-9]{2} \+0530)(\")":
# "%a, %d %b %Y %H:%M:%S %z"
# # January 23, 2020, 12:05
# , r"(<li class = \"date\">Updated: )([a-zA-Z]+ [0-9]{1,2}, 20[0-9]{2}, [0-9]{1,2}:[0-9]{2})( IST<\/li>)":
# "%B %d, %Y, %H:%M"
# # 2020-01-23
# , r"(data\-date = \")([0-9]{4}\-[0-9]{2}\-[0-9]{2})(\">)": "%Y-%m-%d"
# # 2020-01-23
# , r"(data\-article\-date = ')([0-9]{4}\-[0-9]{2}\-[0-9]{2})(')": "%Y-%m-%d"
# # "datePublished": "2020-01-30T22:12:00+05:30"
# , r"(\"datePublished\": \")(20[0-9]{2}\-[0-9]{2}\-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(\+05:30\")": "%Y-%m-%dT%H:%M:%S"
# # "dateModified": "2020-01-30T22:15:00+05:30"
# , r"(\"dateModified\": \")(20[0-9]{2}\-[0-9]{2}\-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(\+05:30\")": "%Y-%m-%dT%H:%M:%S"
# # 'publishedDate': '2020-01-01T22:39:00+05:30'
# , r"('publishedDate': ')(20[0-9]{2}\-[0-9]{2}\-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(\+05:30')": "%Y-%m-%dT%H:%M:%S"
# # <meta http-equiv = "last-modified" content = "Fri, 26 Feb 2021 02:33:38 +0530">
# }

    dateMatchPatterns = dict()

    articleIndustryRegexps = [r"(data-category-name = ')([a-zA-Z0-9 \-,]+)(')"]

    authorRegexps = [r"(agency:')([a-zA-Z0-9]+)(')",
                     r"(channel :  ')([a-zA-Z0-9]+)(',)",
                     r"(agename = ')([a-zA-Z0-9]+)(';)",
                     r"(<div class = \"ag tac\">)([a-zA-Z0-9]+)(<\/div>)",
                     r"(\"publisher\":{\"@type\":\"Organization\",\"name\":\")([a-zA-Z0-9]+)(\")",
                     r"(\.economictimes\.indiatimes\.com\/agency\/.+\" target = \"_blank\">)([a-zA-Z0-9]+)(<\/a>)"
                     ]

    authorMatchPatterns = []

    urlMatchPatterns = []

    invalidTextStrings = ["If you choose to ignore this message, we'll assume that you are happy to receive all cookies"]

    allowedDomains = ["economictimes.indiatimes.com"]
    listOfURLS = []
    uRLdata = dict()

    def __init__(self):
        """ Initialize the object
        """
        super().__init__()

    def extractIndustries(self, uRLtoFetch, htmlText):
        """  Extract Industries from text and URL
        """
        industries = []

        try:
            logger.debug("Attempting to extract industries identified by the article.")

            # first identify network location of the URL:
            netwLocation = getNetworkLocFromURL(uRLtoFetch).split('.')
            # use the smallest sub-domain:
            if not ('economictimes' == netwLocation[0]):
                industries.append(netwLocation[0])
            # else:
                # docRoot = BeautifulSoup( htmlText, 'lxml')
                # TODO: parse html and get industry

        except Exception as e:
            logger.error("Error identifying industries for URL %s: %s",
                         uRLtoFetch.encode("ascii", "ignore"),
                         e)

        return(industries)

    def extractAuthors(self, htmlText):
        """ Extract Authors/Agency/Source of the article from its raw html code
        """
        authors = []
        authorStr = ""
        for authorMatch in self.authorMatchPatterns:
            logger.debug("Trying match pattern: %s", authorMatch)

            try:
                result = authorMatch.search(htmlText)
                authorStr = result.group(2)
                authors = authorStr.split(',')
                # At this point, the text was correctly extracted, so exit the loop
                break
            except Exception as e:
                logger.debug("Exception identifying article authors: %s; string to parse: %s, URL: %s",
                             e, authorStr, self.URLToFetch)

        if authorStr == "":
            logger.debug("Re-attempting identifying article's author for URL: %s",
                         self.URLToFetch)
            try:
                docRoot = BeautifulSoup(htmlText, 'lxml')
                body_root = docRoot.find("span", "ag")
                if body_root is not None:

                    if len(body_root.getText()) < 1:
                        if body_root.find("img") is None:
                            authors = []
                        else:
                            authors = [body_root.img['alt']]
                    else:
                        authors = [body_root.getText()]

            except Exception as e:
                logger.error("Error when re-attempting extraction of authors from article content: %s, URL: %s",
                             e,
                             self.URLToFetch)
        return(authors)

    def extractArticleBody(self, htmlContent):
        """ Extract Article Text content using Beautiful Soup library
        """
        body_text = ""
        try:
            # get article text data by parsing article-body tag:
            docRoot = BeautifulSoup(htmlContent, 'lxml')
            body_text = self.extractArticleBodyFormat1(docRoot)

            # Try this for paywall: <article data-apw = "1" class = "artData clr paywall">
            if len(body_text) < 5 and len(docRoot.find_all("article", attrs={"class": "artData clr paywall"})) > 0:
                body_text = self.extractArticleBodyFormat2(docRoot)

            # Try this for blogs:
            if len(body_text) < 5 and len(docRoot.find_all("div", attrs={"class": "blog-show"})) > 0:
                body_text = self.extractArticleBodyFormat3(docRoot)

            # alternative format 4
            if len(body_text) < 5 and len(docRoot.find_all("article", attrs={"class": "artData clr "})) > 0:
                body_text = self.extractArticleBodyFormat4(docRoot)

            # alternative format 5
            if len(body_text) < 5:
                body_text = self.extractArticleBodyFormat5(docRoot)

            # except Warning as w:
            #    logger.warn("Warning when extracting text via BeautifulSoup: %s", w)

        except Exception as e:
            logger.error("Exception extracting article content via tags: %s", e)
        return(body_text)

    def extractArticleBodyFormat1(self, docRoot):
        """ Extract Article Text content in format 1
        """
        body_text = ""
        try:
            # get article text data by parsing article-body tag:
            body_root = docRoot.find_all("div",
                                         attrs={"itemprop": "mainContentOfPage",
                                                "class": "article-body"}
                                         )
            if len(body_root) > 0:
                firstTag = body_root[0]
                sub_section = firstTag.find_all("div", attrs={"class": "post-text artcle-txt article-type-news"})
                if len(sub_section) > 0:
                    sub_sub_section = sub_section[0].find_all("div", attrs={"class": "Normal"})

                    if len(sub_sub_section) > 0:
                        body_text = sub_sub_section[0].getText()
                        logger.debug("Successfully extracted article content in format 1")

        except Exception as e:
            logger.error("Extracting article content in format 1: %s", e)
        return(body_text)

    def extractArticleBodyFormat2(self, docRoot):
        """ Extract Article Text content in format 2
        """
        body_text = ""
        try:
            # only get contents if its a blog type of article:
            body_root = docRoot.find_all("article", attrs={"class": "artData clr paywall"})
            if len(body_root) > 0:
                firstTag = body_root[0]
                # <div data-brcount = "43" class = "artText medium">
                sub_section = firstTag.find_all("div", attrs={"class": "artText"})

                if len(sub_section) > 0:
                    body_text = sub_section[0].getText()
                    logger.debug("Successfully extracted article content in format 2")

        except Exception as e:
            logger.error("Extracting article content in format 2: %s", e)

        return(body_text)

    def extractArticleBodyFormat3(self, docRoot):
        """ Extract Article Text content in format 3
        """
        body_text = ""
        try:
            # only get contents if its a blog type of article:
            body_root = docRoot.find_all("div", attrs={"class": "main-content"})
            if len(body_root) > 0:
                firstTag = body_root[0]
                # get only <p> contents
                for paragraph in firstTag.children:
                    if paragraph.name == 'p':
                        body_text = body_text + paragraph.getText()

                logger.debug("Successfully extracted article content in format 3")

        except Exception as e:
            logger.error("Extracting article content in format 3: %s", e)

        return(body_text)

    def extractArticleBodyFormat4(self, docRoot):
        """ Extract Article Text content in format 4
        """
        body_text = ""
        try:
            # only get contents if its a blog type of article:
            body_root = docRoot.find_all("article", attrs={"class": "artData clr "})

            if len(body_root) > 0:
                firstTag = body_root[0]
                body_text = firstTag.getText()
                logger.debug("Successfully extracted article content in format 4")

        except Exception as e:
            logger.error("When extracting article content in format 4: %s", e)
        return(body_text)

    def extractArticleBodyFormat5(self, docRoot):
        """ Extract Article Text content in format 5
        # <div data-brcount = "10" class = "artText">
        """
        body_text = ""
        try:
            # only get contents if its a blog type of article:
            body_root = docRoot.find_all("div", attrs={"class": "artText"})

            if len(body_root) > 0:
                firstTag = body_root[0]
                body_text = firstTag.getText()
                logger.debug("Successfully extracted article content in format 5")

        except Exception as e:
            logger.error("When extracting article content in format 5: %s", e)
        return(body_text)

# # end of file ##
