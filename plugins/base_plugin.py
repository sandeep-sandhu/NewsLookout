#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: baseModule.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-06-01
 Purpose: base class that is the parent for all plugins for the application
 Copyright 2021, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu, sandeep.sandhu@gmx.com

Provides:
    basePlugin
        readConfigObj
        setNetworkHelper
        setURLQueue
        addURLsListToQueue
        putQueueEndMarker
        config
        filterInvalidURLs
        filterNonContentURLs
        makeUniqueFileName
        extractArticlesListWithNewsP
        extractPublishedDate
        getArticlesListFromRSS
        getURLsListForDate
        extractArticleListFromMainURL
        extractLinksFromURLList
        extractUniqueIDFromURL
        downloadDataArchive
        fetchDataFromURL
        parseFetchedData


 Notice:
 This software is intended for demonstration and educational purposes only. This software is
 experimental and a work in progress. Under no circumstances should these files be used in
 relation to any critical system(s). Use of these files is at your own risk.

 Before using it for web scraping any website, always consult that website's terms of use.
 Do not use this software to fetch any data from any website that has forbidden use of web
 scraping or similar mechanisms, or violates its terms of use in any other way. The author is
 not liable for such kind of inappropriate use of this software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
 PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
 FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 DEALINGS IN THE SOFTWARE.

"""

##########

# import standard python libraries:

import re
import logging
import sys
import os
from datetime import datetime
# from datetime import timezone

# import web retrieval and text processing python libraries:
from bs4 import BeautifulSoup
import newspaper
from newspaper import Article

# import this project's python libraries:
from network import NetworkFetcher
from data_structs import Types, NewsArticle, ScrapeError, ExecutionResult

from scraper_utils import normalizeURL, extractLinks, calculateCRC32
from scraper_utils import retainValidArticles, removeInValidArticles
from scraper_utils import sameURLWithoutQueryParams, deDupeList

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
                        r'(http.+\/\/)(www\..+\.in\/.+\/)([0-9]{5,})',
                        r'(http.+\/\/)(www\..+\.com\/.+=)([0-9]{5,})'
                        ]

    # write the following regexps dict with each key as regexp to match the required date text,
    # group 2 of this regular expression should match the date string
    # in this dict, the key will be the date format expression
    articleDateRegexps = {
         # Thu, 23 Jan 2020 11:00:00 +0530
         r"(<meta name=\"created-date\" content=\")"
         + r"([a-zA-Z]{3}, [0-9]{1,2} [a-zA-Z]{3} 20[0-9]{2} [0-9]{1,2}:[0-9]{2}:[0-9]{2} \+0530)(\" \/>)":
         "%a, %d %b %Y %H:%M:%S %z",
         # <meta http-equiv="Last-Modified" content="Sat, 15 May 2021 08:43:47 AM"/>
         r"(<meta http-equiv=\"Last-Modified\" content=\")"
         + r"([a-zA-Z]{3}, [0-9]{1,2} [a-zA-Z]{3} 20[0-9]{2} [0-9]{1,2}:[0-9]{2}:[0-9]{2})( [AMPamp]{2}\"\/>)":
         "%a, %d %b %Y %H:%M:%S",
         # Thu, 23 Jan 2020 11:00:00 +0530
         r"(<meta name = \"publish-date\" content = \")"
         + r"([a-zA-Z]{3}, [0-9]{1,2} [a-zA-Z]{3} 20[0-9]{2} [0-9]{1,2}:[0-9]{2}:[0-9]{2} \+0530)(\" \/>)":
         "%a, %d %b %Y %H:%M:%S %z",
         r"(<meta name=\"publish-date\" content=\")"
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
         r"(\"datePublished\": \")(20[0-9]{2}\-[0-9]{2}\-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(\+05:30\")":
         "%Y-%m-%dT%H:%M:%S",
         # content = "Fri, 26 Feb 2021 02:33:38 +0530">
         r"(content=\")([a-zA-Z]{3}, [0-9]{1,2} [a-zA-Z]{3} 20[0-9]{2} [0-9]{1,2}:[0-9]{2}:[0-9]{2} \+0530)(\">)":
         "%a, %d %b %Y %H:%M:%S %z",
         # content = "2021-02-26T17:45:55+05:30"
         r"(content=\")(20[0-9]{2}\-[0-9]{2}\-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(\+05:30\")":
         "%Y-%m-%dT%H:%M:%S",
         # Updated: February 26, 2021 5:45 pm IST
         r"(Updated: )([a-zA-Z]+ [0-9]{1,2}, 20[0-9]{2} [0-9]{1,2}:[0-9]{2})( [a-zA-Z]{2} IST)":
         "%B %d, %Y %H:%M",
         # January 23, 2020, 12:05
         r"(<li class=\"date\">Updated: )([a-zA-Z]+ [0-9]{1,2}, 20[0-9]{2}, [0-9]{1,2}:[0-9]{2})( IST<\/li>)":
         "%B %d, %Y, %H:%M",
         # 2020-01-23
         r"(data\-date=\")([0-9]{4}\-[0-9]{2}\-[0-9]{2})(\">)":
         "%Y-%m-%d",
         # 2020-01-23
         r"(data\-article\-date=')([0-9]{4}\-[0-9]{2}\-[0-9]{2})(')":
         "%Y-%m-%d",
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
        self.URLToFetch = None
        self.networkHelper = None
        self.newsPaperArticle = None
        self.urlQueue = None
        self.urlQueueTotalSize = 0
        self.urlProcessedCount = 0
        self.pluginState = Types.STATE_GET_URL_LIST
        if self.pluginType in [Types.MODULE_NEWS_CONTENT]:
            # check required attributes:
            attributesTocheck = ['mainURL', 'validURLStringsToCheck', 'invalidURLSubStrings',
                                 'allowedDomains', 'urlUniqueRegexps', 'pluginType',
                                 'minArticleLengthInChars', 'nonContentURLs', 'nonContentStrings']
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
        elif self.pluginType in [Types.MODULE_DATA_PROCESSOR]:
            self.pluginState = Types.STATE_PROCESS_DATA

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
            if self.pluginType not in [Types.MODULE_NEWS_AGGREGATOR, Types.MODULE_DATA_PROCESSOR]:
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

    def readConfigObj(self, configElement):
        """ Helper function to read plugin specific configuration
        """
        return(self.configReader.get('plugins', configElement))

    def getStatusString(self):
        """ Prepare status text to be printed out by the worker thread in the log
        """
        statusString = ''
        if self.pluginType in [Types.MODULE_NEWS_CONTENT]:
            statusString = 'State = ' + Types.decodeNameFromIntVal(self.pluginState)
        elif self.pluginType in [Types.MODULE_DATA_PROCESSOR]:
            statusString = 'State = ' + Types.decodeNameFromIntVal(self.pluginState)
        elif self.pluginType in [Types.MODULE_NEWS_AGGREGATOR]:
            statusString = 'State = ' + Types.decodeNameFromIntVal(self.pluginState)
        return(statusString)

    def setNetworkHelper(self):
        """ Initialise the object that communicates over the network
        """
        self.networkHelper = NetworkFetcher(self.configData, self.allowedDomains)

    def setURLQueue(self, urlQueue):
        """ This is the list of URLS specific to this plugin module
        """
        self.urlQueue = urlQueue
        self.priority_number = 0

    def getURLList(self):
        return(self.listOfURLS)

    def addURLsListToQueue(self, listOfURLs):
        """ Add URLs List To Queue """
        listOfURLs = self.filterNonContentURLs(listOfURLs)
        self.urlQueueTotalSize = 0
        for listItem in listOfURLs:
            if listItem is not None:
                # add tuple of priority and URL to priority queue:
                self.urlQueue.put((self.priority_number, listItem))
                logger.debug("%s: Adding URL into queue: %s", self.pluginName, listItem.encode('ascii', 'ignore'))
                self.priority_number = self.priority_number + 1
                self.urlQueueTotalSize = self.urlQueueTotalSize + 1

    def putQueueEndMarker(self):
        """ Add URLs List To Queue """
        # add sentinel object with last priority
        self.urlQueue.put((10000000, None))
        # change state of plugin to indicate url gathering is over.
        self.pluginState = Types.STATE_FETCH_CONTENT
        # nothing more to do for a news aggregator:
        if self.pluginType == Types.MODULE_NEWS_AGGREGATOR:
            self.pluginState = Types.STATE_STOPPED
        # empty out the url list in the plugin:
        self.listOfURLS = []
        logger.info("%s: Final count of articles to be retrieved = %s",
                    self.pluginName,
                    self.urlQueue.qsize() - 1
                    )

    def identifyDataPathForRunDate(self, runDateString):
        """ Identify Path For Run Date
        """
        return(os.path.join(self.configData['data_dir'], str(runDateString)))

    def makeUniqueFileName(pluginName, baseDirName, uniqueID, URL=None):
        """ Create a Unique File Name for this article to be saved
        It does not contain the extension .json, this has to be appended.
        """
        return(os.path.join(baseDirName, pluginName + "_" + str(uniqueID)))

    def filterInvalidURLs(self, urlList):
        """ Filter Invalid URLs:
        remove invalid articles,
        keep only valid URLs
        """
        resultList = []
        # retain only valid articles:
        resultList = retainValidArticles(
            urlList,
            self.validURLStringsToCheck
            )
        # remove all invalid articles:
        resultList = removeInValidArticles(
            resultList,
            self.invalidURLSubStrings
            )
        return(resultList)

    def filterNonContentURLs(self, urlList):
        """ filter non-content URLs """
        for uRLtoFetch in deDupeList(urlList):
            try:
                for item in deDupeList(self.nonContentURLs):
                    if sameURLWithoutQueryParams(uRLtoFetch, item) is True and uRLtoFetch in urlList:
                        urlList.remove(uRLtoFetch)  # remove those urls in list: nonContentURLs
            except Exception as e:
                logger.error("%s: When filtering non-content URLs, error was: %s", self.pluginName, e)
            try:
                for item in deDupeList(self.nonContentStrings):
                    if uRLtoFetch.find(item) > 0 and uRLtoFetch in urlList:
                        urlList.remove(uRLtoFetch)  # remove urls containing nonContentStrings
            except Exception as e:
                logger.error("%s: When filtering URLs with non-content string indicators, error was: %s", self.pluginName, e)
        return(urlList)

    def extractArticlesListWithNewsP(self):
        """ extract Article Text using the Newspaper library """
        try:
            # replace default HTTP get method with custom method:
            newspaper.network.get_html_2XX_only = NetworkFetcher.NewsPpr_get_html_2XX_only
            # instantiate the source object for newspaper
            thisNewsPSource = newspaper.source.Source(self.mainURL,
                                                      config=self.configData['newspaper_config'])
            # thisNewsPSource.download()  # disabled to handle proxy certificate problem, download using networkHelper
            thisNewsPSource.html = self.networkHelper.fetchRawDataFromURL(thisNewsPSource.url, self.pluginName)
            thisNewsPSource.is_downloaded = True
            thisNewsPSource.parse()
            thisNewsPSource.set_categories()
            for index, category in enumerate(thisNewsPSource.categories):
                category_html = self.networkHelper.fetchRawDataFromURL(category.url, self.pluginName)
                # logger.debug("Retrieved %s bytes from category URL: %s", len(category_html), category.url)
                thisNewsPSource.categories[index].html = category_html
            thisNewsPSource.categories = [c for c in thisNewsPSource.categories if c.html]
            # thisNewsPSource.download_categories()  # disabled to handle proxy certificate problem
            thisNewsPSource.parse_categories()
            thisNewsPSource.set_feeds()
            for index, feed in enumerate(thisNewsPSource.feeds):
                feed_html = self.networkHelper.fetchRawDataFromURL(feed.url, self.pluginName)
                # logger.debug("Retrieved %s bytes from feed URL: %s", len(feed_html), feed.url)
                thisNewsPSource.feeds[index].rss = feed_html
            thisNewsPSource.feeds = [f for f in thisNewsPSource.feeds if f.rss]
            # thisNewsPSource.download_feeds()  # disabled to handle proxy certificate problem
            thisNewsPSource.generate_articles()
            # remove invalid data
            newspaperSourcedURLS = self.filterInvalidURLs(thisNewsPSource.articles)
            # normalize the list of URLs:
            if newspaperSourcedURLS is not None:
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
                if len(rawData) > self.minArticleLengthInChars:
                    docRoot = BeautifulSoup(rawData, 'lxml-xml')
                    if docRoot.channel is not None:
                        for item in docRoot.channel:
                            if item.name == "item":
                                self.listOfURLS.append(normalizeURL(item.link.contents[0]))
            except Exception as e:
                logger.error("%s: Error getting urls listing from RSS feed %s: %s",
                             self.pluginName,
                             thisFeedURL,
                             e)
        self.listOfURLS = self.filterInvalidURLs(self.listOfURLS)

    def extractArchiveURLLinksForDate(self, runDate):
        """ Extracting archive URL links for given date """
        resultSet = []
        try:
            if 'mainURLDateFormatted' in dir(self):
                searchResultsURLForDate = runDate.strftime(self.mainURLDateFormatted)
                URLsListForDate = self.extractLinksFromURLList(runDate, [searchResultsURLForDate])
                if URLsListForDate is not None and len(URLsListForDate) > 0:
                    resultSet = URLsListForDate
                logger.debug("%s: Added URLs for current date from archive page at: %s",
                             self.pluginName,
                             searchResultsURLForDate)
        except Exception as e:
            logger.error("%s: Error extracting archive URL links for given date: %s", self.pluginName, e)
        return(resultSet)

    def getURLsListForDate(self, runDate, completedURLs):
        """ Retrieve the URLs List for the given run date
        """
        logger.debug("%s: Fetching list of urls for date: %s", self.pluginName, str(runDate.strftime("%Y-%m-%d")))
        self.listOfURLS = []
        try:
            self.getArticlesListFromRSS()
            self.extractArticlesListWithNewsP()
            # add main url derived links + pending url list from sqlite database:
            self.listOfURLS = deDupeList(self.listOfURLS +
                                         self.extractArticleListFromMainURL(runDate) +
                                         completedURLs.retrieveTodoURLList(self.pluginName))
        except Exception as e:
            logger.error("%s: Error retrieving list of URLs from main URL and pending table: %s", self.pluginName, e)
        try:
            if self.pluginType in [Types.MODULE_NEWS_CONTENT,
                                   Types.MODULE_DATA_CONTENT,
                                   Types.MODULE_NEWS_API
                                   ]:
                allURLs = self.getLinksRecursively(self.listOfURLS, runDate, self.configData['recursion_level'])
                allURLs = completedURLs.removeAlreadyFetchedURLs(allURLs, self.pluginName)
                self.addURLsListToQueue(allURLs)
                completedURLs.addURLsToPendingTable(allURLs, self.pluginName)
            elif self.pluginType == Types.MODULE_NEWS_AGGREGATOR:
                # Recursion is not applicable for news aggregators; simply add url into common queue:
                # TODO: identify the relevant pluginName from the allowed domain
                # completedURLs.addURLsToPendingTable(self.listOfURLS, self.pluginName)
                self.addURLsListToQueue(self.listOfURLS)
        except Exception as e:
            logger.error("%s: Error trying to validate retrieved listing of URLs: %s",
                         self.pluginName,
                         e)
        self.putQueueEndMarker()

    def getLinksRecursively(self, uRLSList, runDate, recursionLevel):
        """ Get Links Recursively
        """
        allURLs = uRLSList
        # initialize local variables:
        links2LevelsDeep = []
        links3LevelsDeep = []
        links4LevelsDeep = []
        try:
            if recursionLevel > 1:
                # go another level deeper
                logger.info("Started collecting URLs 2 levels deep, for plugin: %s", self.pluginName)
                links2LevelsDeep = self.extractLinksFromURLList(runDate, allURLs)
                if links2LevelsDeep is not None:
                    allURLs = allURLs + links2LevelsDeep
                if recursionLevel > 2 and links2LevelsDeep is not None:
                    # go yet another level deeper
                    logger.info("Started collecting URLs 3 levels deep, for plugin: %s", self.pluginName)
                    links3LevelsDeep = self.extractLinksFromURLList(runDate, links2LevelsDeep)
                    if links3LevelsDeep is not None:
                        allURLs = allURLs + links3LevelsDeep
                    if recursionLevel > 3 and links3LevelsDeep is not None:
                        # go yet another level deeper
                        logger.info("Started collecting URLs 4 levels deep, for plugin: %s", self.pluginName)
                        links4LevelsDeep = self.extractLinksFromURLList(runDate, links3LevelsDeep)
                        if links4LevelsDeep is not None:
                            allURLs = allURLs + links4LevelsDeep
            allURLs = deDupeList(allURLs)
            # remove invalid articles:
            allURLs = self.filterInvalidURLs(allURLs)
        except Exception as e:
            logger.error("Error getting links recursively: %s", e)
        return(allURLs)

    def extractPublishedDate(self, htmlText):
        """ Extract Published Date from html content
        """
        # default is todays date:
        date_obj = datetime.now()
        if type(htmlText) == bytes:
            htmlText = htmlText.decode('UTF-8')
        dateString = ""
        errorFlag = True
        for dateRegex in self.dateMatchPatterns.keys():
            (datePattern, datetimeFormatStr) = self.dateMatchPatterns[dateRegex]
            try:
                result = datePattern.search(htmlText)
                if result is not None:
                    dateString = result.group(2)
                    date_obj = datetime.strptime(dateString, datetimeFormatStr).replace(tzinfo=None)
                    currentDateTime = datetime.now()
                    if date_obj > currentDateTime:
                        logger.debug("%s: Invalid article date identified in the future: %s, for URL: %s",
                                     self.pluginName, date_obj, self.URLToFetch)
                    else:
                        # if we did not encounter any error till this point, and were able to get a date object
                        # then, this is the answer, so exit loop
                        errorFlag = False
                        break
            except Exception as e:
                logger.debug("%s: Could not identify article date: %s, string to parse: %s, using regexp: %s, URL: %s",
                             self.pluginName, e, dateString, dateRegex, self.URLToFetch)
        if errorFlag is True:
            logger.debug("%s: Could not identify published date for article at URL: %s",
                         self.pluginName,
                         self.URLToFetch)
            raise ScrapeError("Invalid article since the publish date of article could not be identified.")
        return date_obj

    def extractArticleListFromMainURL(self, runDate):
        """ Extract article list from main URL
        """
        linksLevel1 = []
        try:
            urlsToBeExtracted = [self.mainURL] + self.nonContentURLs
            linksLevel1 = self.extractLinksFromURLList(runDate, urlsToBeExtracted)
            linksLevel1 = linksLevel1 + self.extractArchiveURLLinksForDate(runDate)
        except Exception as e:
            logger.error("%s: When Extracting article list from main URL, error was: %s",
                         self.pluginName, e)
        return(linksLevel1)

    def extractLinksFromURLList(self, runDate, listOfURLs):
        """ Extract links inside the content of given list of URLs
        The function argument 'runDate' is not used here.
        """
        resultListOfURLs = []
        htmlContent = ""
        for linkL1Item in listOfURLs:
            try:
                htmlContent = self.networkHelper.fetchRawDataFromURL(linkL1Item, self.pluginName)
                docRoot = BeautifulSoup(htmlContent, 'lxml')
                extractedListOfURLs = extractLinks(linkL1Item, docRoot)
                resultListOfURLs = deDupeList(resultListOfURLs + extractedListOfURLs)
            except Exception as e2:
                logger.error("%s: Error fetching additional links for URL %s: %s",
                             self.pluginName,
                             linkL1Item,
                             e2)
        logger.info("%s: Identified %s additional URLs, filtered count of links = %s",
                    self.pluginName,
                    len(extractedListOfURLs),
                    len(resultListOfURLs))
        return(resultListOfURLs)

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
            logger.error("%s: When calculating CRC32 of URL: %s , URL was: %s",
                         self.pluginName,
                         e,
                         URLToFetch.encode('ascii'))
        if len(URLToFetch) > 6:
            for urlPattern in self.urlMatchPatterns:
                try:
                    result = urlPattern.search(URLToFetch)
                    if result is not None:
                        uniqueString = result.group(3)
                        # if we did not encounter any error till this point, then this is the answer, so exit
                        return(uniqueString)
                except Exception as e:
                    logger.debug("%s: Unable to identify unique ID, error is: %s , URL was: %s, Pattern: %s",
                                 self.pluginName,
                                 e,
                                 URLToFetch.encode('ascii'),
                                 urlPattern)
        else:
            logger.debug("%s: Invalid URL found since could not identify unique ID: %s", URLToFetch.encode('ascii'))
            raise ScrapeError("Invalid article since it does not have a unique identifier.")
        if uniqueString == crcValue:
            logger.debug("%s: Error identifying unique ID of URL: %s, hence using CRC32 code: %s",
                         self.pluginName,
                         URLToFetch.encode('ascii', 'ignore'),
                         uniqueString)
            raise ScrapeError("Invalid article since it does not have a unique identifier.")

    def downloadDataArchive(self, url, pluginName):
        """ Download Data Archive """
        htmlcontent = b""
        try:
            httpResp = self.networkHelper.getHTTPData(url)
            if httpResp is not None:
                htmlcontent = httpResp.content
        except Exception as e:
            logger.error("%s: Error when downloading Data Archive: %s", pluginName, e)
        return(htmlcontent)

    def fetchDataFromURL(self, uRLtoFetch, WorkerID):
        """ Fetch complete cleaned data From given URL
        The output tuple structure is: (uRL, len_raw_data, len_text, publish_date)
        """
        resultVal = None
        # set this plugin instance's URL attribute until its data retrieval is completed
        self.URLToFetch = uRLtoFetch
        try:
            self.urlProcessedCount = self.urlProcessedCount + 1
            for item in self.nonContentURLs:
                if sameURLWithoutQueryParams(uRLtoFetch, item) is True:
                    logger.debug("%s: Ignoring non-content URL/not retrieving it: %s",
                                 self.pluginName, uRLtoFetch.encode("ascii", "error"))
                    return(resultVal)
            # scrape data/html only if this url is not listed as part of nonContent URL list
            if uRLtoFetch not in self.nonContentURLs:
                htmlContent = self.networkHelper.fetchRawDataFromURL(uRLtoFetch, self.pluginName)
                if htmlContent is not None and len(htmlContent) > self.minArticleLengthInChars:
                    logger.debug("%s: Fetched %s characters of html data", self.pluginName, len(htmlContent))
                    # create newspaper library's article object:
                    newsPaperArticle = Article(uRLtoFetch, config=self.networkHelper.newspaper_config)
                    #  set html data from the fetched html content
                    newsPaperArticle.set_html(htmlContent)
                    # check data is valid, clean it:
                    validData = self.parseFetchedData(uRLtoFetch, newsPaperArticle, WorkerID)
                    # check if content is adequate, if so save it to file:
                    if validData.getTextSize() > self.minArticleLengthInChars:
                        # write news article object 'validData' to file:
                        savefileNameWithOutExt = basePlugin.makeUniqueFileName(
                            self.pluginName,
                            self.identifyDataPathForRunDate(str(validData.getPublishDate())),
                            validData.getArticleID(),
                            URL=validData.getURL())
                        validData.writeFiles(savefileNameWithOutExt,
                                             str(htmlContent),
                                             saveHTMLFile=self.bSaveHTMLFile)
                        resultVal = ExecutionResult(uRLtoFetch,
                                                    validData.getHTMLSize(),
                                                    validData.getTextSize(),
                                                    validData.getPublishDate(),
                                                    self.pluginName,
                                                    dataFileName=savefileNameWithOutExt)
                    else:
                        logger.debug("%s: Insufficient or invalid data (%s characters) retrieved for URL: %s",
                                     self.pluginName,
                                     validData.getTextSize(),
                                     uRLtoFetch.encode('ascii', "ignore"))
        except Exception as e:
            logger.info("%s: Ignoring URL %s due to: %s",
                        self.pluginName, uRLtoFetch.encode('ascii', "ignore"), e)
        # reset the plugin instance's attribute back to none
        self.tempArticleData = None
        self.URLToFetch = None
        return(resultVal)

    def parseFetchedData(self, uRLtoFetch, newpArticleObj, WorkerID):
        """Parse the fetched news article data to validate it, clean it,
         and then extract vital elements if these are missing.
         Return a NewsArticle object with complete and cleaned data
        """
        logger.debug("%s: Parsing the fetched Data, WorkerID = %s",
                     self.pluginName,
                     WorkerID)
        articleUniqueID = None
        # all data will be stored in this object:
        parsedCleanData = NewsArticle()
        try:
            newpArticleObj.parse()
            # run nlp to parse data for keywords, etc. (note: this requires nltk data to be downloaded)
            newpArticleObj.nlp()
        except Exception as e:
            logger.error("%s: Error parsing raw HTML from URL %s: %s", self.pluginName, uRLtoFetch, e)
        # run custom clean-up code on the text:
        newpArticleObj.text = self.checkAndCleanText(newpArticleObj.text, newpArticleObj.html)
        # check date validity:
        if (newpArticleObj.publish_date is None or newpArticleObj.publish_date == '' or
                (not newpArticleObj.publish_date) or (type(newpArticleObj.publish_date) == 'datetime.datetime' and
                                                      newpArticleObj.publish_date > datetime.now())):
            # extract published date by searching for specific tags:
            newpArticleObj.publish_date = self.extractPublishedDate(newpArticleObj.html)
        # identify news agency/source if it is not properly recognized:
        if (len(newpArticleObj.authors) < 1 or
                (len(newpArticleObj.authors) > 0 and newpArticleObj.authors[0].find('<') >= 0)):
            newpArticleObj.set_authors(self.extractAuthors(newpArticleObj.html))
        # for special cases, unique id is embedded in HTML content, in that case use this method to identify unique ID
        if 'extractUniqueIDFromContent' in dir(self):
            articleUniqueID = self.extractUniqueIDFromContent(newpArticleObj.html, uRLtoFetch)
            logger.debug("%s: Extracted unique ID from HTML content: %s",
                         self.pluginName, articleUniqueID)
        else:
            # otherwise, for almost all other cases, use URL to identify unique ID
            articleUniqueID = self.extractUniqueIDFromURL(uRLtoFetch)
        # put all the cleaned up data into an NewsArticle object: parsedCleanData
        try:
            parsedCleanData.importNewspaperArticleData(newpArticleObj)
            # use trigger words logic to generate flags:
            parsedCleanData.identifyTriggerWordFlags(self.configReader)
            # identify and set industries from url and content
            parsedCleanData.setIndustries(self.extractIndustries(uRLtoFetch, parsedCleanData.html))
            parsedCleanData.setArticleID(articleUniqueID)
            parsedCleanData.setModuleName(self.pluginName)
        except Exception as e:
            logger.error("%s: Error storing parsed data for URL %s: %s",
                         self.pluginName,
                         uRLtoFetch,
                         e)
        return(parsedCleanData)

# # end of file # #
