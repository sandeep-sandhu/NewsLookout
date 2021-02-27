#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: baseModule.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-01-14
 Purpose: base class that is the parent for all plugins for the application
 Copyright 2021, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu, sandeep.sandhu@gmx.com

Provides:
    basePlugin
        readConfigObj
        setNetworkHelper
        setURLQueue
        config
        makeUniqueFileName
        extractArticlesListWithNewsP
        extractPublishedDate
        getArticlesListFromRSS
        getURLsListForDate
        extractArticleListFromMainURL
        extractLinksFromURLList
        extractUniqueIDFromURL
        fetchDataFromURL
        parseFetchedData


 DISCLAIMER: This software is intended for demonstration and educational purposes only.
 Before using it for web scraping any website, always consult that website's terms of use.
 Do not use this software to fetch any data from any website that has forbidden use of web
 scraping or similar mechanisms, or violates its terms of use in any other way. The author is
 not responsible for such kind of inappropriate use of this software.

"""

##########

# import standard python libraries:

import re
import logging
import sys
from datetime import datetime

# import web retrieval and text processing python libraries:
from bs4 import BeautifulSoup
import newspaper
from newspaper import Article

# import this project's python libraries:
from network import NetworkFetcher
from data_structs import Types, NewsArticle, URLListHelper

from scraper_utils import normalizeURL, extractLinks, calculateCRC32
from scraper_utils import retainValidArticles, removeInValidArticles

##########

logger = logging.getLogger(__name__)


class basePlugin:
    """This is the parent class for all plugins.
    It implements several methods for basic common funcitonality.
    """

    historicURLs = 0
    configData = {}
    configReader = None
    baseDirName = ""
    tempArticleData = None

    # write regexps in three groups ()()() so that the third group
    # gives a unique identifier such as a long integer at the end of a URL
    # this third group will be selected as the unique identifier:
    urlUniqueRegexps = [
                        r'(http.+\/\/)(www\..+\.com\/.+\-)([0-9]{5,})',
                        r'(http.+\/\/)(www\..+\.com\/.+\-)([0-9]{5,})(\.html)',
                        r'(http.+\/\/)(www\..+\.in\/.+\/)([0-9]{5,})(\.html)',
                        r'(http.+\/\/)(www\..+\.in\/.+\-)([0-9]{5,})',
                        r'(http.+\/\/)(www\..+\.in\/.+\/)([0-9]{5,})'
                        ]

    # write the following regexps dict with each key as regexp to match the required date text,
    # group 2 of this regular expression should match the date string
    # in this dict, put the key will be the date format expression
    # to be used for datetime.strptime() function, refer to:
    # https://docs.python.org/3/library/datetime.html#datetime.datetime.strptime
    articleDateRegexps = {
         # Thu, 23 Jan 2020 11:00:00 +0530
         r"(<meta name = \"created-date\" content = \")"
         + r"([a-zA-Z]{3}, [0-9]{1,2} [a-zA-Z]{3} 20[0-9]{2} [0-9]{1,2}:[0-9]{2}:[0-9]{2} \+0530)(\" \/>)":
         "%a, %d %b %Y %H:%M:%S %z",
         # Thu, 23 Jan 2020 11:00:00 +0530
         r"(<meta name = \"publish-date\" content = \")"
         + r"([a-zA-Z]{3}, [0-9]{1,2} [a-zA-Z]{3} 20[0-9]{2} [0-9]{1,2}:[0-9]{2}:[0-9]{2} \+0530)(\" \/>)":
         "%a, %d %b %Y %H:%M:%S %z",
         # Thu, 23 Jan 2020 11:00:00 +0530
         r"(\"datePublished\":\")"
         + r"([a-zA-Z]{3}, [0-9]{1,2} [a-zA-Z]{3} 20[0-9]{2} [0-9]{1,2}:[0-9]{2}:[0-9]{2} \+0530)(\")":
         "%a, %d %b %Y %H:%M:%S %z",
         # Thu, 23 Jan 2020 12:05:00 +0530
         r"(\"dateModified\":\")"
         + r"([a-zA-Z]{3}, [0-9]{1,2} [a-zA-Z]{3} 20[0-9]{2} [0-9]{1,2}:[0-9]{2}:[0-9]{2} \+0530)(\")":
         "%a, %d %b %Y %H:%M:%S %z",
         # "datePublished": "2021-02-25T22:59:00+05:30"
         r"(\"datePublished\": \")"
         + r"(20[0-9]{2}\-[0-9]{2}\-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(\+05:30\")": "%Y-%m-%dT%H:%M:%S",
         # content = "Fri, 26 Feb 2021 02:33:38 +0530">
         r"(content = \")([a-zA-Z]{3}, [0-9]{1,2} [a-zA-Z]{3} 20[0-9]{2} [0-9]{1,2}:[0-9]{2}:[0-9]{2} \+0530)(\">)":
         "%a, %d %b %Y %H:%M:%S %z",
         # content = "2021-02-26T17:45:55+05:30"
         r"(content = \")(20[0-9]{2}\-[0-9]{2}\-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(\+05:30\")": "%Y-%m-%dT%H:%M:%S",
         # Updated: February 26, 2021 5:45 pm IST
         r"(Updated: )([a-zA-Z]+ [0-9]{1,2}, 20[0-9]{2} [0-9]{1,2}:[0-9]{2})( [a-zA-Z]{2} IST)": "%B %d, %Y %H:%M",
         # January 23, 2020, 12:05
         r"(<li class = \"date\">Updated: )([a-zA-Z]+ [0-9]{1,2}, 20[0-9]{2}, [0-9]{1,2}:[0-9]{2})( IST<\/li>)":
         "%B %d, %Y, %H:%M",
         # 2020-01-23
         r"(data\-date = \")([0-9]{4}\-[0-9]{2}\-[0-9]{2})(\">)": "%Y-%m-%d",
         # 2020-01-23
         r"(data\-article\-date = ')([0-9]{4}\-[0-9]{2}\-[0-9]{2})(')": "%Y-%m-%d",
         # "datePublished": "2020-01-30T22:12:00+05:30"
         r"(\"datePublished\": \")(20[0-9]{2}\-[0-9]{2}\-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(\+05:30\")": "%Y-%m-%dT%H:%M:%S",
         # "dateModified": "2020-01-30T22:15:00+05:30"
         r"(\"dateModified\": \")(20[0-9]{2}\-[0-9]{2}\-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(\+05:30\")": "%Y-%m-%dT%H:%M:%S",
         # 'publishedDate': '2020-01-01T22:39:00+05:30'
         r"('publishedDate': ')(20[0-9]{2}\-[0-9]{2}\-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(\+05:30')": "%Y-%m-%dT%H:%M:%S"
        }

    # #
    def __init__(self):
        """ Initializes the class object.
        Verifies whether the required attributes and methods have been overridden in the plugin classes.
        Logs an error and exits if these are not found in the plugin.
        """
        self.pluginName = type(self).__name__
        self.pluginState = Types.STATE_GET_URL_LIST
        self.URLToFetch = ""
        self.networkHelper = None
        self.newsPaperArticle = None

        if self.pluginType in [Types.MODULE_NEWS_CONTENT]:
            # check required attributes:
            attributesTocheck = ['mainURL', 'validURLStringsToCheck', 'invalidURLSubStrings',
                                 'allowedDomains', 'urlUniqueRegexps', 'pluginType',
                                 'minArticleLengthInChars', 'nonContentURLs']

            for attrToCheck in attributesTocheck:
                if attrToCheck not in dir(self):
                    logger.error("%s plugin must define attribute: %s",
                                 self.pluginName, attrToCheck)
                    sys.exit(-1)

            # check required methods:
            methodsTocheck = ['getURLsListForDate', 'parseFetchedData', 'extractUniqueIDFromURL',
                              'extractArticleBody']

            for methodName in methodsTocheck:
                if methodName not in dir(self):
                    logger.error("%s plugin must implement method: %s",
                                 self.pluginName, methodName)
                    sys.exit(-1)

    def readConfigObj(self, configElement):
        """ Helper function to read plugin specific configuration
        """
        return(self.configReader.get('plugins', configElement))

    def setNetworkHelper(self, networkHelperObj):
        self.networkHelper = networkHelperObj

    def setURLQueue(self, urlQueue):
        self.urlQueue = urlQueue

    def config(self, configDict):
        """ Configure the plugin
        """
        self.configData = configDict

        try:
            logger.debug("%s: Reading the configuration parameters", self.pluginName)

            self.baseDirName = self.configData['data_dir']

            if self.configData['save_html'].lower() == "true":
                self.bSaveHTMLFile = True
            else:
                self.bSaveHTMLFile = False

            self.configReader = self.configData['configReader']

        except Exception as e:
            logger.error("%s: Could not read configuration parameters: %s", self.pluginName, e)

        try:
            logger.debug("%s: Applying the configuration parameters", self.pluginName)

            for urlRegex in self.urlUniqueRegexps:
                # logger.debug("Compiling match pattern for URL identification: %s", urlRegex)
                self.urlMatchPatterns.append(re.compile(urlRegex))

            for authorRegex in self.authorRegexps:
                # logger.debug("Compiling match pattern for Authors: %s", authorRegex)
                self.authorMatchPatterns.append(re.compile(authorRegex))

            for dateRegex in self.articleDateRegexps.keys():
                # logger.debug("Compiling match pattern for dates: %s", dateRegex )
                self.dateMatchPatterns[dateRegex] = (re.compile(dateRegex), self.articleDateRegexps[dateRegex])

        except Exception as e:
            logger.error("%s: Could not apply configuration parameters: %s", self.pluginName, e)

    def makeUniqueFileName(self, uniqueID):
        """ Create a Unique File Name for this article
        """
        return(self.pluginName + "_" + str(uniqueID))

    def extractArticlesListWithNewsP(self):
        """ extract Article Text using the Newspaper library """
        try:
            # replace default HTTP get method with custom method:
            newspaper.network.get_html_2XX_only = NetworkFetcher.NewsPpr_get_html_2XX_only

            thisNewsPSource = newspaper.source.Source(self.mainURL,
                                                      onfig=self.networkHelper.newspaper_config)
            thisNewsPSource.download()
            thisNewsPSource.parse()
            thisNewsPSource.set_categories()
            thisNewsPSource.download_categories()  # mthread
            thisNewsPSource.parse_categories()
            thisNewsPSource.set_feeds()
            thisNewsPSource.download_feeds()  # mthread
            thisNewsPSource.generate_articles()

            # alternative version:
            # thisNewsPSource = newspaper.build( self.mainURL, config = self.networkHelper.newspaper_config )
            newspaperSourcedURLS = retainValidArticles(thisNewsPSource.articles,
                                                       self.validURLStringsToCheck)
            # normalize the list of URLs:
            for uRLIndex in range(len(newspaperSourcedURLS)):
                self.listOfURLS.append(normalizeURL(newspaperSourcedURLS[uRLIndex]))

        except Exception as e:
            logger.error("%s: Error extracting articles list using the Newspaper library: %s",
                         self.pluginName,
                         e)

    def getArticlesListFromRSS(self):
        """ extract Article listing using the BeautifulSoup library
        to identify the list from its RSS feed
        """
        for thisFeedURL in self.all_rss_feeds:
            try:
                rawData = self.networkHelper.fetchRawDataFromURL(thisFeedURL, self.pluginName)
                rss_feed_xml = BeautifulSoup(rawData, 'lxml-xml')

                for item in rss_feed_xml.channel:
                    if item.name == "item":
                        self.listOfURLS.append(normalizeURL(item.link.contents[0]))

            except Exception as e:
                logger.error("%s: Error getting urls listing from RSS feed %s: %s",
                             self.pluginName,
                             thisFeedURL,
                             e)

        self.listOfURLS = retainValidArticles(self.listOfURLS,
                                              self.validURLStringsToCheck)

    def getURLsListForDate(self, runDate):
        """ Retrieve the URLs List for the given run date
        """
        logger.info("%s: Fetching list of urls for date: %s",
                    self.pluginName,
                    str(runDate.strftime("%Y-%m-%d")))
        self.listOfURLS = []

        try:
            self.getArticlesListFromRSS()

            self.extractArticlesListWithNewsP()

            mainURLLinks = self.extractArticleListFromMainURL(self.mainURL)

            self.listOfURLS = self.listOfURLS + mainURLLinks

        except Exception as e:
            logger.error("%s: Error trying to retrieve listing of URLs: %s",
                         self.pluginName,
                         e)

        try:
            self.listOfURLS = URLListHelper.deDupeList(self.listOfURLS)
            # remove invalid articles:
            self.listOfURLS = removeInValidArticles(self.listOfURLS, self.invalidURLSubStrings)

        except Exception as e:
            logger.error("%s: Error trying to validate retrieved listing of URLs: %s",
                         self.pluginName,
                         e)

    def extractPublishedDate(self, htmlText):
        """ Extract Published Date from html content
        """
        # default is todays date:
        date_obj = datetime.now()
        curr_datetime = date_obj

        dateString = ""
        datetimeFormatStr = ""

        for dateRegex in self.dateMatchPatterns.keys():
            (datePattern, datetimeFormatStr) = self.dateMatchPatterns[dateRegex]

            try:
                result = datePattern.search(htmlText)
                dateString = result.group(2)
                date_obj = datetime.strptime(dateString, datetimeFormatStr)
                # if we did not encounter any error till this point, and were able to get a date object
                # then, this is the answer, so exit loop
                break

            except Exception as e:
                logger.debug("%s: Exception identifying article date: %s, string to parse: %s, using regexp: %s, URL: %s",
                             self.pluginName, e, dateString, dateRegex, self.URLToFetch)

        if curr_datetime == date_obj:
            logger.error("%s: Exception identifying article's date for URL: %s",
                         self.pluginName, self.URLToFetch)

        return date_obj

    def extractArticleListFromMainURL(self, uRLtoFetch):
        """ Extract article list from main URL
        """
        try:
            htmlContent = self.networkHelper.fetchRawDataFromURL(uRLtoFetch, self.pluginName)

            docRoot = BeautifulSoup(htmlContent, 'lxml')

            linksLevel1 = extractLinks(uRLtoFetch, docRoot)

            linksLevel1 = retainValidArticles(
                 URLListHelper.deDupeList(linksLevel1),
                 self.validURLStringsToCheck)

            logger.info("%s: Extracted %s links from main URL: %s",
                        self.pluginName,
                        len(linksLevel1),
                        uRLtoFetch)

        except Exception as e:
            logger.error("%s: Error extracting links from main URL: %s",
                         self.pluginName,
                         e)

        return(linksLevel1)

    def extractLinksFromURLList(self, urlsToBeExtracted):
        """ Extract links inside the content of given list of URLs
        The function argument 'runDate' is not used here.
        """
        linksLevel1 = urlsToBeExtracted
        resultListOfURLs = []
        htmlContent = ""

        for linkL1Item in linksLevel1:
            try:
                htmlContent = self.networkHelper.fetchRawDataFromURL(linkL1Item, self.pluginName)
                docRoot = BeautifulSoup(htmlContent, 'lxml')

                linksLevel2 = extractLinks(linkL1Item, docRoot)

                linksLevel2 = retainValidArticles(
                                                  removeInValidArticles(
                                                                        URLListHelper.deDupeList(linksLevel2),
                                                                        self.invalidURLSubStrings
                                                                        ),
                                                  self.validURLStringsToCheck
                                                  )

                resultListOfURLs = URLListHelper.deDupeList(resultListOfURLs + linksLevel2)

                logger.info("%s: Extracted %s additional URLs from page at: %s, total links count = %s",
                            self.pluginName,
                            len(linksLevel2),
                            linkL1Item,
                            len(resultListOfURLs))

            except Exception as e2:
                logger.error("%s: Was fetching links 2 levels deep for URL %s: %s",
                             self.pluginName,
                             linkL1Item,
                             e2)

            # clean up by retaining only valid URLs
            return(
                retainValidArticles(resultListOfURLs, self.validURLStringsToCheck)
                )

    def extractUniqueIDFromURL(self, URLToFetch):
        """ get Unique ID From URL by extracting RegEx patterns matching any of urlMatchPatterns
        """
        uniqueString = ""
        crcValue = "zzz-zzz-zzz"
        try:
            # calculate CRC string if url are not usable:
            crcValue = str(calculateCRC32(URLToFetch.encode('utf-8')))
            uniqueString = crcValue

        except Exception as e:
            logger.error("%s: Error calculating CRC32 of URL: %s , URL was: %s",
                         self.pluginName,
                         e,
                         URLToFetch.encode('ascii'))

        if len(URLToFetch) > 6:
            for urlPattern in self.urlMatchPatterns:

                try:
                    result = urlPattern.search(URLToFetch)
                    uniqueString = result.group(3)
                    # if we did not encounter any error till this point, then this is the answer, so exit loop
                    break

                except Exception as e:
                    logger.debug("%s: Retrying identifying unique ID of URL, error: %s , URL was: %s, Pattern: %s",
                                 self.pluginName,
                                 e,
                                 URLToFetch.encode('ascii'),
                                 urlPattern)

        else:
            logger.error("%s: Invalid URL found when trying to identify unique ID: %s", URLToFetch.encode('ascii'))

        if uniqueString == crcValue:
            logger.error("%s: Error identifying unique ID of URL: %s, hence using CRC32 code: %s",
                         self.pluginName,
                         URLToFetch.encode('ascii', 'ignore'),
                         uniqueString)

        return(uniqueString)

    def fetchDataFromURL(self, uRLtoFetch, WorkerID):
        """ Fetch Data From URL
        """
        logger.debug("%s: Fetching %s, Worker ID %s",
                     self.pluginName,
                     uRLtoFetch.encode("ascii", "error"),
                     WorkerID)

        # output tuple structure: (uRL, len_raw_data, len_text, publish_date)
        resultVal = (uRLtoFetch, None, None, None)

        self.URLToFetch = uRLtoFetch

        try:
            for item in self.nonContentURLs:
                if uRLtoFetch.find(item) > -1:
                    return(resultVal)

            if uRLtoFetch not in self.nonContentURLs:
                articleUniqueID = self.extractUniqueIDFromURL(uRLtoFetch)

                htmlContent = self.networkHelper.fetchRawDataFromURL(uRLtoFetch, self.pluginName)

                # create newspaper library's article object:
                newsPaperArticle = Article(uRLtoFetch, config=self.networkHelper.newspaper_config)
                #  set data from the fetched content
                newsPaperArticle.set_html(htmlContent)

                validData = self.parseFetchedData(uRLtoFetch, newsPaperArticle, WorkerID)

                self.tempArticleData = validData

                if validData.getTextSize() > self.minArticleLengthInChars:

                    validData.setArticleID(articleUniqueID)

                    validData.setModuleName(self.pluginName)
                    filename = self.makeUniqueFileName(articleUniqueID)

                    # write news article object 'validData' to file:
                    validData.writeFiles(filename,
                                         self.baseDirName,
                                         str(htmlContent),
                                         saveHTMLFile=self.bSaveHTMLFile)
                    # save count of characters of downloaded data for the given URL
                    resultVal = (uRLtoFetch, len(htmlContent), validData.getTextSize(), validData.getPublishDate())

                else:
                    logger.error("%s: Insufficient or invalid data (%s characters) retrieved for URL: %s",
                                 self.pluginName,
                                 validData.getTextSize(),
                                 uRLtoFetch.encode('ascii', "ignore"))

        except Exception as e:
            logger.error("%s: Error fetching data from URL %s: %s",
                         self.pluginName,
                         uRLtoFetch.encode('ascii', "ignore"),
                         e)
        self.URLToFetch = ""

        return(resultVal)

    def parseFetchedData(self, uRLtoFetch, newpArticleObj, WorkerID):
        """Parse the fetched news article data to validate it
        , and re-extract vital elements if these are missing
        """
        logger.debug("%s: Parsing the fetched Data, WorkerID = %s",
                     self.pluginName,
                     WorkerID)

        parsedCleanData = NewsArticle()

        try:
            newpArticleObj.parse()
            invalidFlag = False

            for badString in self.invalidTextStrings:

                if newpArticleObj.text.find(badString) >= 0:

                    logger.debug("%s: Found invalid text in data extracted: %s; URL was: %s",
                                 self.pluginName,
                                 badString,
                                 uRLtoFetch)
                    invalidFlag = True
                    newpArticleObj.text = self.extractArticleBody(newpArticleObj.html)

            # check if article content is not valid or is too little
            if invalidFlag is True or len(newpArticleObj.text) < self.minArticleLengthInChars:
                newpArticleObj.text = self.extractArticleBody(newpArticleObj.html)

            if newpArticleObj.publish_date is None or newpArticleObj.publish_date == '' or (not newpArticleObj.publish_date):
                # extract published date by searching for specific tags
                newpArticleObj.publish_date = self.extractPublishedDate(newpArticleObj.html)

            # identify news agency/source if it is not properly recognized:
            if (len(newpArticleObj.authors) < 1 or
                    (len(newpArticleObj.authors) > 0 and newpArticleObj.authors[0].find('<') >= 0)):
                newpArticleObj.set_authors(self.extractAuthors(newpArticleObj.html))

            newpArticleObj.nlp()

        except Exception as e:
            logger.error("%s: Error parsing raw data for URL %s: %s",
                         self.pluginName,
                         uRLtoFetch,
                         e)

        try:
            parsedCleanData.importNewspaperArticleData(newpArticleObj)

            parsedCleanData.setIndustries(
                self.extractIndustries(uRLtoFetch, parsedCleanData.html)
                )

        except Exception as e:
            logger.error("%s: Error storing parsed data for URL %s: %s",
                         self.pluginName,
                         uRLtoFetch,
                         e)

        return(parsedCleanData)

# # end of file # #
