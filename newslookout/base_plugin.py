#!/usr/bin/env python
# -*- coding: utf-8 -*-

# #########################################################################################################
#                                                                                                         #
# File name: base_plugin.py                                                                                #
# Application: The NewsLookout Web Scraping Application                                                   #
# Date: 2021-06-23                                                                                        #
# Purpose: base class that is the parent for all plugins for the application                              #
# Copyright 2021, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu, sandeep.sandhu@gmx.com  #
#                                                                                                         #
# Provides:                                                                                               #
#    BasePlugin                                                                                           #
#        readConfigObj                                                                                    #
#        initNetworkHelper                                                                                 #
#        setURLQueue                                                                                      #
#        addURLsListToQueue                                                                               #
#        putQueueEndMarker                                                                                #
#        config                                                                                           #
#        filterInvalidURLs                                                                                #
#        filterNonContentURLs                                                                             #
#        makeUniqueFileName                                                                               #
#        extractArticlesListWithNewsP                                                                     #
#        extractPublishedDate                                                                             #
#        getArticlesListFromRSS                                                                           #
#        getURLsListForDate                                                                               #
#        extr_links_from_main_noncont                                                                    #
#        extr_links_from_urls_list                                                                          #
#        extractUniqueIDFromURL                                                                           #
#        downloadDataArchive                                                                              #
#        fetchDataFromURL                                                                                 #
#        parseFetchedData                                                                                 #
#                                                                                                         #
#                                                                                                         #
# Notice:                                                                                                 #
# This software is intended for demonstration and educational purposes only. This software is             #
# experimental and a work in progress. Under no circumstances should these files be used in               #
# relation to any critical system(s). Use of these files is at your own risk.                             #
#                                                                                                         #
# Before using it for web scraping any website, always consult that website's terms of use.               #
# Do not use this software to fetch any data from any website that has forbidden use of web               #
# scraping or similar mechanisms, or violates its terms of use in any other way. The author is            #
# not liable for such kind of inappropriate use of this software.                                         #
#                                                                                                         #
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,                     #
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR                #
# PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE               #
# FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR                    #
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER                  #
# DEALINGS IN THE SOFTWARE.                                                                               #
#                                                                                                         #
# #########################################################################################################

# import standard python libraries:
import queue
import re
import logging
import sys
import os
from datetime import datetime
from queue import Queue
import json

# import web retrieval and text processing python libraries:
from bs4 import BeautifulSoup
import newspaper
from newspaper import Article
from newspaper import network as newspaper_network

# import this project's python libraries:
from config import ConfigManager
from network import NetworkFetcher
from data_structs import PluginTypes, ScrapeError, ExecutionResult, PluginStatus
from news_event import NewsEvent

import scraper_utils
from scraper_utils import normalizeURL, extractLinks, calculateCRC32, getPreviousDaysDate, getNextDaysDate
from scraper_utils import is_valid_url
from scraper_utils import retainValidArticles, removeInValidArticles
from scraper_utils import sameURLWithoutQueryParams
from session_hist import SessionHistory
from archive_writer import get_archive_writer

##########

logger = logging.getLogger(__name__)


class BasePlugin:
    """This is the parent class for all plugins.
    It implements several methods for basic common functionality.
    """

    pluginName = "BasePlugin"
    executionPriority = 999
    historicURLs = 0
    app_config = None
    configReader = None
    baseDirName = ""
    archive_base_path = 'data'
    use_archive_storage = True
    archive_writer = None
    bSaveHTMLFile = True
    tempArticleData = None
    nonContentURLs = []
    nonContentStrings = []
    invalidURLSubStrings = []
    validURLStringsToCheck = []
    minArticleLengthInChars = 400
    urlMatchPatterns = []
    authorRegexps = []
    authorMatchPatterns = []
    dateMatchPatterns = {}
    allowedDomains = []
    mainURL = None
    mainURLDateFormatted = None
    all_rss_feeds = []
    pluginType = None
    status = None
    pluginState = PluginTypes.STATE_GET_URL_LIST
    URLToFetch = None
    listOfURLS = []
    networkHelper = None
    newsPaperArticle = None
    urlQueue = queue.Queue()
    urlQueueTotalSize = 0
    urlProcessedCount = 0

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
         # "dateModified": "2020-01-30T22:15:00+05:30"
         r"(\"dateModified\": \")(20[0-9]{2}\-[0-9]{2}\-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(\+05:30\")":
         "%Y-%m-%dT%H:%M:%S",
         # 'publishedDate': '2020-01-01T22:39:00+05:30'
         r"('publishedDate': ')(20[0-9]{2}\-[0-9]{2}\-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(\+05:30')":
         "%Y-%m-%dT%H:%M:%S",
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
         "%Y-%m-%d"
         }


    def __init__(self):
        """ Initializes the class object.
        Verifies whether the required attributes and methods have been overridden in the plugin classes.
        Logs an error and exits if these are not found in the plugin.
        """
        self.is_stopped = False
        self.pluginName = type(self).__name__
        self.status = PluginStatus()
        self.enable_recursion = False
        self.queue_manager = None  # Will be set by queue manager during initialization
        self.status.set_plugin_state(PluginTypes.STATE_GET_URL_LIST)
        if self.pluginType in [PluginTypes.MODULE_NEWS_CONTENT]:
            # check required attributes:
            attributesTocheck = ['mainURL', 'validURLStringsToCheck', 'invalidURLSubStrings',
                                 'allowedDomains', 'urlUniqueRegexps', 'pluginType',
                                 'minArticleLengthInChars', 'nonContentURLs', 'nonContentStrings']
            for attrToCheck in attributesTocheck:
                if attrToCheck not in dir(self):
                    logger.error(f"{self.pluginName} plugin must define attribute: {attrToCheck}, exiting now.")
                    sys.exit(-1)
            # check required methods:
            methodsTocheck = ['getURLsListForDate', 'parseFetchedData', 'extractUniqueIDFromURL',
                              'extractArticleBody']
            for methodName in methodsTocheck:
                if methodName not in dir(self):
                    logger.error(f"{self.pluginName} plugin must implement method: {methodName}, exiting now.")
                    sys.exit(-1)
        elif self.pluginType in [PluginTypes.MODULE_DATA_PROCESSOR]:
            self.pluginState = PluginTypes.STATE_PROCESS_DATA
            self.status.set_plugin_state(PluginTypes.STATE_PROCESS_DATA)
            # check required methods:
            methodsTocheck = ['processDataObj', 'additionalConfig']
            for methodName in methodsTocheck:
                if methodName not in dir(self):
                    logger.error(f"{self.pluginName} plugin must implement method: {methodName}, exiting now.")
                    sys.exit(-1)

    def config(self, app_config: ConfigManager):
        """ Configure the plugin.

        :param app_config: The application's configuration data.
        :type app_config: config.ConfigManager
        """
        self.app_config = app_config
        assert type(self.app_config) == ConfigManager, "Invalid configuration object for Base Plugin"
        try:
            logger.debug("%s: Reading the configuration parameters", self.pluginName)
            self.baseDirName = self.app_config.data_dir
            self.archive_base_path = self.app_config.archive_base_path
            self.use_archive_storage = self.app_config.use_archive_storage
            if self.use_archive_storage == True:
                self.archive_writer = get_archive_writer(self.archive_base_path)
            else:
                self.archive_writer = None
            if self.app_config.save_html.lower() == "true":
                self.bSaveHTMLFile = True
            else:
                self.bSaveHTMLFile = False
            self.configReader = self.app_config.config_parser
        except Exception as e:
            logger.error("%s: Could not read configuration parameters: %s", self.pluginName, e)
        try:
            logger.debug("%s: Applying the configuration parameters", self.pluginName)
            if self.pluginType not in [PluginTypes.MODULE_NEWS_AGGREGATOR, PluginTypes.MODULE_DATA_PROCESSOR]:
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

    def save_article_to_archive(self, json_content: str, raw_html: bytes,
                                article_id: str, publish_date) -> tuple:
        """
        Save article data to archive instead of individual files.

        :param article_data: Dictionary containing article data
        :param raw_html: Raw HTML content (bytes, uncompressed)
        :param article_id: Unique article identifier
        :param publish_date: Publication date
        :return: Tuple of (archive_path, article_id)
        """
        if not self.use_archive_storage or self.archive_writer is None:
            raise ValueError("Archive storage not enabled")

        # Note: HTML should NOT be bz2 compressed before passing to archive writer
        # The 7z archive provides its own compression
        # If HTML is already bz2 compressed, the writer will decompress it

        # Write to archive
        return self.archive_writer.write_article(
            json_content=json_content,
            raw_html_content=raw_html,  # Pass uncompressed or bz2 (will be handled)
            article_id=article_id,
            publish_date=publish_date,
            plugin_name=self.pluginName
        )

    def getStatusString(self) -> str:
        """ Prepare status text to be printed out by the worker thread in the log

        :rtype: str
        :return: String indicating the state of this plugin, e.g. 'State = FETCH DATA'
        """
        if self.pluginType in [PluginTypes.MODULE_NEWS_CONTENT, PluginTypes.MODULE_DATA_CONTENT]:
            return 'State = ' + PluginTypes.decodeNameFromIntVal(self.pluginState)
        elif self.pluginType in [PluginTypes.MODULE_DATA_PROCESSOR]:
            return 'State = ' + PluginTypes.decodeNameFromIntVal(self.pluginState)
        elif self.pluginType in [PluginTypes.MODULE_NEWS_AGGREGATOR]:
            return 'State = ' + PluginTypes.decodeNameFromIntVal(self.pluginState)

    def initNetworkHelper(self):
        """ Initialise the object that communicates over the network
        """
        self.networkHelper = NetworkFetcher(self.app_config, self.allowedDomains)

    def setURLQueue(self, urlQueue: Queue):
        """ This is the list of URLS specific to this plugin module
        """
        self.urlQueue = urlQueue

    def getURLList(self) -> list:
        """
        Gets the List of URLs identified for scraping by this plugin.

        :return: List of URL strings
        :rtype: list[str]
        """
        return self.listOfURLS

    def addURLsListToQueue(self, listOfURLs: list, sessionHistoryDB: SessionHistory) -> None:
        """ Add the list of URLs to this plugin's Queue

        :parameter listOfURLs: List of URL strings to be fetched for web scraping by this plugin
        :type listOfURLs: list[str]
        """
        # validate and filter url list before adding to queue.
        listOfURLs = self.filterNonContentURLs(listOfURLs)
        logger.info(f'{self.pluginName}: After filtering non-content URLs, URLs remaining: {len(listOfURLs)}')
        # TODO: use alternative method to filter previously fetched urls, to speed up this process
        listOfURLs = sessionHistoryDB.removeAlreadyFetchedURLs(listOfURLs, self.pluginName)
        for listItem in listOfURLs:
            if listItem is not None:
                # add valid URLs to this plugin's queue:
                self.urlQueue.put(listItem)
                logger.debug(f"{self.pluginName}: Adding to queue, URL: {listItem.encode('ascii', 'ignore')}")
                self.urlQueueTotalSize = self.urlQueueTotalSize + 1
        logger.info(f'{self.pluginName}: After adding new urls, total number of URLs = {self.urlQueueTotalSize}')

    def putQueueEndMarker(self):
        """ Adds an end-of-queue marker sentinel object 'None' and update the state of this plugin.

        For news or data content plugins, changes the state of the plugin to -> PluginTypes.STATE_FETCH_CONTENT

        For news aggregator plugin, changes state to -> PluginTypes.STATE_STOPPED
        """
        # add sentinel object at the end
        self.urlQueue.put(None)
        # change state of plugin to indicate url gathering is over.
        self.pluginState = PluginTypes.STATE_FETCH_CONTENT
        # nothing more to do for a news aggregator:
        if self.pluginType == PluginTypes.MODULE_NEWS_AGGREGATOR:
            self.pluginState = PluginTypes.STATE_STOPPED
        # empty out the url list in the plugin:
        self.listOfURLS = []
        logger.info("%s: Final count of articles to be retrieved = %s, Current Queue Size = %s",
                    self.pluginName,
                    self.urlQueueTotalSize,
                    self.getQueueSize()
                    )

    def getQueueSize(self) -> int:
        """
        Get the current Queue size, reduce the internal queue size by 1 to adjust for the Sentinal object.
        :return: Number of URLs pending to be extracted in the queue for this plugin.
        """
        if self.urlQueue.qsize() > 0:
            return self.urlQueue.qsize() - 1
        else:
            return 0

    def isQueueEmpty(self) -> bool:
        return self.urlQueue.empty()

    def getNextItemFromFetchQueue(self, timeout: int = 30) -> str:
        """ Get Next item from fetch queue of this plugin

        :param timeout: Optional, no of seconds to block call.
        :return: URL from the queue
        :rtype: str
        """
        sURL = self.urlQueue.get(block=True, timeout=timeout)
        self.urlQueue.task_done()
        return sURL

    def clearQueue(self):
        """ Clears this object's own queue - self.urlQueue
        """
        try:
            if self.urlQueue is not None:
                with self.urlQueue.mutex:
                    self.urlQueue.queue.clear()
        except Exception as e:
            logger.error("When clearing the URL queue, exception was: %s", e)

    @staticmethod
    def getFullFilePathsInDir(directoryName: str) -> list:
        """ Get list of all files in directory

        :param directoryName: The directory whose files need to be listed.
        :type directoryName: str
        :return: List of files in this directory.
        :rtype: list[str]
        """
        list_of_files = []
        try:
            if os.path.isdir(directoryName) is True:
                list_of_files = [
                    os.path.join(directoryName, i)
                    for i in os.listdir(directoryName)
                    if os.path.isfile(os.path.join(directoryName, i))
                ]
        except Exception as e:
            logger.error("When retrieving list of file sin directory, error was: %s", e)
        return list_of_files

    @staticmethod
    def identifyDataPathForRunDate(data_directory: str, business_date: datetime) -> str:
        """ Identify the data directory path for a given run-date

        :rtype: str
        :param data_directory: The base data directory for the application.
        :type data_directory: str
        :param business_date: The business date for which the text needs to be processed.
        :type business_date: datetime
        :return: Full path for the directory storing files for the given run-date
        """
        date_string = ""
        if type(business_date) == str:
            date_string = str(business_date)
        if type(business_date) == datetime:
            date_string = business_date.strftime('%Y-%m-%d')
        return os.path.join(data_directory, date_string)

    @staticmethod
    def identifyFilesForDate(baseDirName: str, runDate: datetime, dayspan: int = 0) -> list:
        """ Get list of files for directories for today's run date.
         Optionally, get files from tomorow's run-date and yesterday's run-date directories too.

        :rtype: list[str]
        :param baseDirName: The base data directory for the application.
        :type baseDirName: str
        :param runDate: The business date for which the text needs to be processed.
        :type runDate: datetime
        :param dayspan: The number of dates before and after given run-date, for which
          the list of files need to be gathered. The default value is 0 which means
          gather files only from the given run-date's directory.
        :type dayspan: int
        :return: List of files for given run-date
        """
        newlist = []
        try:
            listOfFiles = BasePlugin.getFullFilePathsInDir(
                BasePlugin.identifyDataPathForRunDate(baseDirName, runDate)
                )
            if dayspan > 0:
                # # get articles from previous day too:
                listOfFiles = listOfFiles + BasePlugin.getFullFilePathsInDir(
                    BasePlugin.identifyDataPathForRunDate(
                        baseDirName,
                        getPreviousDaysDate(runDate)
                        )
                    )
                # # get for next day, in case data is available:
                listOfFiles = listOfFiles + BasePlugin.getFullFilePathsInDir(
                    BasePlugin.identifyDataPathForRunDate(
                        baseDirName, getNextDaysDate(runDate)
                        )
                    )
            # remove non-json files:
            newlist = [i for i in listOfFiles if i.endswith('json')]
        except Exception as e:
            logger.error("When identifying data files for date %s, error was: %s", runDate, e)
        return newlist

    @staticmethod
    def makeUniqueFileName(pluginName: str, baseDirName: str, uniqueID: object, URL: str = None) -> str:
        """ Create a Unique File Name for this article to be saved
        It does not contain the extension .json, this has to be appended.

        :parameter pluginName: Name of the plugin object.
        :type pluginName: str
        :parameter baseDirName: Parent directory where the file needs to be saved
         i.e. the combination of base directory and publish date.
        :type baseDirName: str
        :parameter uniqueID: Unique identifier for the given article being saved.
        :type uniqueID: str
        :rtype: str
        :return: Full file path for given article, excluding the file extension.
        """
        return os.path.join(
                baseDirName,
                pluginName + "_" + str(uniqueID)
                )

    def filterInvalidURLs(self, urlList: list) -> list:
        """ Filter invalid URLs before web scraping by removing invalid articles, and keeping only valid URLs.

        Refer to class fields - validURLStringsToCheck and invalidURLSubStrings.

        Calls the functions - retainValidArticles() and removeInValidArticles()

        :parameter urlList: The list of URL strings to check and filter
        :type urlList: list[str]
        :return: The filtered list of URLs
        :rtype: list[str]
        """
        # retain only valid articles:
        resultList = retainValidArticles(urlList, self.validURLStringsToCheck)
        # remove all invalid articles:
        resultList = removeInValidArticles(resultList, self.invalidURLSubStrings)
        return resultList

    def filterNonContentURLs(self, urlList: list) -> list:
        """ Filter out non-content URLs so these are not fetched.
        Refers to class member variables: nonContentURLs, and nonContentStrings

        :parameter urlList: The list of URL strings to check and filter
        :type urlList: list[str]
        :return: The filtered list of URLs
        :rtype: list[str]
        """
        try:
            if type(urlList) == str:
                # if a string was passed, fix it by converting it into a list of string
                urlList = [urlList]
            urlList = self.filterInvalidURLs(urlList)
            urlList = [i for i in scraper_utils.deDupeList(urlList) if
                       is_valid_url(i) is True and
                       self.has_noncont_url(i, self.nonContentURLs) is False and
                       self.has_noncont_str(i, self.nonContentStrings) is False]
        except Exception as e:
            logger.error(f"{self.pluginName}: When filtering out non-content URLs, error: {e}")
        return urlList

    def has_noncont_url(self, uRLtoFetch: str, nonContentURLs: list) -> bool:
        """ Check and flag True if this URL is listed in the 'non-content' urls list.

        :param uRLtoFetch: URL to check
        :param nonContentURLs: Non-content list of URLs to check
        :return: True if this URL needs to be excluded, False to keep it
        """
        for item in scraper_utils.deDupeList(nonContentURLs):
            # logger.debug(f'Check URL {uRLtoFetch} = {item}: result={sameURLWithoutQueryParams(uRLtoFetch, item)}')
            if sameURLWithoutQueryParams(uRLtoFetch, item):
                return True
        # in the end, if nothing else is found then flag false:
        return False

    def has_noncont_str(self, uRLtoFetch: str, nonContentStrings: list) -> bool:
        """ Check and flag True if non-content urls based on sub-trings found in them.

        :param uRLtoFetch: URL to check
        :param nonContentStrings: Non-content list of URL substrings to be excluded
        :return: True if this URL needs to be excluded, False to keep it
        """
        if uRLtoFetch is None or len(uRLtoFetch) < 2:
            return True
        for item in scraper_utils.deDupeList(nonContentStrings):
            # logger.debug(f'Checking if url {uRLtoFetch} has non-content {item}: result={uRLtoFetch.find(item)}')
            if item is not None and uRLtoFetch.find(item) >= 0:
                return True
        # in the end, if nothing else is found then flag false:
        return False

    def extractArticlesListWithNewsP(self) -> list:
        """ extracts a list of article URLs from a news website using the Newspaper library in Python.
        It takes in the main URL of the news website as input.
        The purpose is to scrape the site and find all the article links on the homepage, category pages, and RSS feeds.
        It returns a list of normalized article URLs as output.
        First it configures Newspaper to use a custom network fetch method instead of the default HTTP get. This is to handle things like proxy certificates.
        It creates a Newspaper Source object for the input news website URL.
        It downloads the homepage HTML using a custom network helper instead of Newspaper's default downloader. This again is to handle proxies etc.
        It parses the homepage to find categories and feeds.
        It loops through each category and feed, downloading the HTML using the custom network helper.
        It removes any invalid categories or feeds that had errors downloading.
        It parses the valid categories and feeds to find articles.
        It generates a list of all article URLs found.
        It filters out any invalid URLs from the list.
        It normalizes each URL in the list to a standard format.
        Finally it returns the list of article URLs.
        So in summary, it takes a news site URL, uses Newspaper to scrape the site and extract article links, normalizes the links, and returns them. The key steps are configuring Newspaper for custom network handling, downloading category/feed pages, and generating a clean list of URLs.
        """
        # TODO: Extract out some of the larger steps like downloading categories/feeds into separate functions. This improves modularity.
        # TODO: Handle exceptions more granularly - log and skip specific categories/feeds with errors instead of failing entirely. This improves robustness.

        listOfURLS = []
        try:
            # Get shutdown event if available
            shutdown_event = getattr(self, 'shutdown_event', None)

            # replace default HTTP get method with custom method:
            newspaper_network.get_html_2XX_only = NetworkFetcher.NewsPpr_get_html_2XX_only
            # instantiate the source object for newspaper
            thisNewsPSource = newspaper.source.Source(self.mainURL,
                                                      config=self.app_config.newspaper_config)

            # Handle tuple return
            fetch_result = self.networkHelper.fetchRawDataFromURL(
                thisNewsPSource.url,
                self.pluginName,
                shutdown_event=shutdown_event
            )

            if isinstance(fetch_result, tuple):
                html_content, http_error = fetch_result
                if http_error or html_content is None:
                    logger.error(f"{self.pluginName}: Failed to fetch main URL: {self.mainURL}")
                    # Queue database operation to save failed URL
                    if hasattr(self, 'queue_manager') and self.queue_manager:
                        self.queue_manager.queueDBOperation(
                            'add_failed',
                            (thisNewsPSource.url, self.pluginName, datetime.now()),
                            wait_for_result=False
                        )
                    return listOfURLS
            else:
                html_content = fetch_result

            thisNewsPSource.html = html_content
            thisNewsPSource.is_downloaded = True
            thisNewsPSource.parse()
            thisNewsPSource.set_categories()

            for index, category in enumerate(thisNewsPSource.categories):
                fetch_result = self.networkHelper.fetchRawDataFromURL(
                    category.url,
                    self.pluginName,
                    shutdown_event=shutdown_event
                )

                if isinstance(fetch_result, tuple):
                    category_html, http_error = fetch_result
                    if http_error or category_html is None:
                        logger.error(f"{self.pluginName}: Failed to fetch category URL: {category.url}, TODO: add to failed urls")
                        # add this url to failed_urls table
                        # Queue database operation to save failed URL
                        if hasattr(self, 'queue_manager') and self.queue_manager:
                            self.queue_manager.queueDBOperation(
                                'add_failed',
                                (category.url, self.pluginName, datetime.now()),
                                wait_for_result=False
                            )
                    if category_html:
                        thisNewsPSource.categories[index].html = category_html
                else:
                    thisNewsPSource.categories[index].html = fetch_result

            thisNewsPSource.categories = [c for c in thisNewsPSource.categories if c.html]
            thisNewsPSource.parse_categories()
            thisNewsPSource.set_feeds()

            for index, feed in enumerate(thisNewsPSource.feeds):
                fetch_result = self.networkHelper.fetchRawDataFromURL(
                    feed.url,
                    self.pluginName,
                    shutdown_event=shutdown_event
                )

                if isinstance(fetch_result, tuple):
                    feed_html, http_error = fetch_result
                    if http_error or feed_html is None:
                        logger.error(f"{self.pluginName}: Failed to fetch feed URL: {feed.url}, TODO: add to failed urls")
                        # add this url to failed_urls table
                        # Queue database operation to save failed URL
                        if hasattr(self, 'queue_manager') and self.queue_manager:
                            self.queue_manager.queueDBOperation(
                                'add_failed',
                                (feed.url, self.pluginName, datetime.now()),
                                wait_for_result=False
                            )
                    if feed_html:
                        thisNewsPSource.feeds[index].rss = feed_html
                else:
                    thisNewsPSource.feeds[index].rss = fetch_result

            thisNewsPSource.feeds = [f for f in thisNewsPSource.feeds if f.rss]
            thisNewsPSource.generate_articles()

            # remove invalid data
            newspaperSourcedURLS = self.filterInvalidURLs(thisNewsPSource.articles)
            # normalize the list of URLs:
            if newspaperSourcedURLS is not None:
                for uRLIndex in range(len(newspaperSourcedURLS)):
                    listOfURLS.append(normalizeURL(newspaperSourcedURLS[uRLIndex]))
        except Exception as e:
            logger.error("%s: Error extracting articles list using the Newspaper library: %s",
                         self.pluginName,
                         e)
        logger.info(f'{self.pluginName}: Identified {len(listOfURLS)} links using the Newspaper library.')
        return listOfURLS

    def getArticlesListFromRSS(self, rss_urls: list) -> list:
        """ Extracts a list of article URLs from a list of RSS feed URLs.
        It takes as input a list of RSS feed URLs (rss_urls). For each URL in that list, it fetches the raw XML data for that RSS feed. It uses the BeautifulSoup library to parse the XML data and extract the   elements, which represent articles or posts in the feed.
        The key thing it looks for in each   is the
         tag, which contains the URL for that article/post. It extracts the text content of the   tag and adds it to a result list.
        After looping through all the RSS feeds, it does some cleanup on the result list - filtering out any invalid URLs and logging how many total URLs were found.
        The end result is a list of article URLs extracted from the input list of RSS feeds. This list of URLs can then be used to visit and scrape each article page.
        So in summary, it takes a list of RSS feed URLs as input, loops through them to extract article URLs, and returns a list of those article URLs as output. The main logic is using BeautifulSoup to parse the XML data and pull out the
         tags from   elements.
        """
        # TODO: Use more specific exception handling instead of a broad Exception catch. Catch and handle specific exceptions like URLError or XMLSyntaxError where possible. This makes error handling more robust.
        # TODO: Validate and sanitize any user-provided input URLs to avoid security issues like SSRF or XXE attacks.

        resultList = []
        for thisFeedURL in rss_urls:
            try:
                # Get shutdown event if available
                shutdown_event = getattr(self, 'shutdown_event', None)

                # Handle tuple return from fetchRawDataFromURL
                fetch_result = self.networkHelper.fetchRawDataFromURL(
                    thisFeedURL,
                    self.pluginName,
                    shutdown_event=shutdown_event
                )

                # Handle tuple return (content, http_error)
                if isinstance(fetch_result, tuple):
                    rawData, http_error = fetch_result
                    if http_error:
                        logger.warning(f"{self.pluginName}: HTTP {http_error.status_code} for RSS feed {thisFeedURL}")
                        # add this url to failed_urls table
                        # Queue database operation to save failed URL
                        if hasattr(self, 'queue_manager') and self.queue_manager:
                            self.queue_manager.queueDBOperation(
                                'add_failed',
                                (thisFeedURL, self.pluginName, datetime.now()),
                                wait_for_result=False
                            )
                        continue
                    if rawData is None:
                        continue
                else:
                    # Backward compatibility
                    rawData = fetch_result
                    if rawData is None:
                        continue

                # if retrieved HTML data is of sufficient size, then parse it using the xml parser:
                if len(rawData) > self.minArticleLengthInChars:
                    docRoot = BeautifulSoup(markup=rawData, features='lxml-xml')
                    # get the <channel> element at the root of the XML document
                    if docRoot.channel is not None:
                        # loop through each <item> element to get the <link> tags
                        for item in docRoot.channel:
                            if item.name == "item":
                                # add each link to the list of URL strings
                                resultList.append(normalizeURL(item.link.contents[0]))
            except Exception as e:
                logger.error("%s: Error getting urls listing from RSS feed %s: %s",
                             self.pluginName,
                             thisFeedURL,
                             e)
        resultList = self.filterInvalidURLs(resultList)
        logger.info(f'{self.pluginName}: Identified {len(resultList)} links from RSS feeds.')
        return resultList

    def loadDocument(self, fileName: str) -> NewsEvent:
        """ Read document object from given JSON filename

        :parameter fileName: Name of JSON file with news event data
        :type fileName: str
        :return: NewsEvent object instantiated with data form json file
        :rtype: news_event.NewsEvent
        """
        document = None
        try:
            if fileName.endswith('.json') is False:
                fileName = fileName + '.json'
            if os.path.isfile(fileName):
                document = NewsEvent()
                # load data from fileName:
                document.readFromJSON(fileName)
                document.setFileName(fileName)
            else:
                logger.error(f'News event data file {fileName} does not exist.')
        except Exception as e:
            logger.error("When trying to read and parse JSON file %s, error: %s", fileName, e)
        return document

    def extractArchiveURLLinksForDate(self, runDate: datetime) -> list:
        """ Extracts archive URL links for a given date.
        It takes as input a datetime object called runDate representing the date to extract links for.
        The function first initializes an empty list called resultSet to store the results.
        It then checks if the self object has a mainURLDateFormatted attribute, and if so, formats the input runDate into a string using that format. This formatted string is likely a URL pattern with the date placeholders filled in.
        The formatted string searchResultsURLForDate is passed in a list to another function extr_links_from_urls_list along with the runDate. This seems to extract links from that search result page.
        Any extracted links are assigned to URLsListForDate. If this list contains links, they are added to the resultSet list.
        The function logs the number of links extracted and the date they are for.
        Finally, resultSet containing the extracted archive links for the given date is returned.
        So in summary, it takes a date, generates a search result URL for that date, extracts archive links from that page, saves them to a list, and returns the list. The main logic is generating the properly formatted search URL and extracting links from the page.
         """
        resultSet = []
        try:
            if 'mainURLDateFormatted' in dir(self) and self.mainURLDateFormatted is not None:
                searchResultsURLForDate = runDate.strftime(self.mainURLDateFormatted)
                URLsListForDate = self.extr_links_from_urls_list(runDate, [searchResultsURLForDate])
                if URLsListForDate is not None and len(URLsListForDate) > 0:
                    resultSet = URLsListForDate
                logger.info("%s: Added %s URLs from news archives for date: %s",
                            self.pluginName,
                            len(resultSet),
                            runDate
                            )
        except Exception as e:
            logger.error("%s: Error extracting archive URL links for given date: %s", self.pluginName, e)
        return resultSet

    @staticmethod
    def concat_lists(allURLs, rssURLList, newsPaperLibURLList, main_page_list, pending_urls):
        # concatenate all lists of URLs:
        if rssURLList is not None:
            allURLs = allURLs + rssURLList
        if newsPaperLibURLList is not None:
            allURLs = allURLs + newsPaperLibURLList
        if main_page_list is not None:
            allURLs = allURLs + main_page_list
        if pending_urls is not None:
            allURLs = allURLs + pending_urls
        return allURLs

    def getURLsListForDate(self, runDate: datetime, sessionHistoryDB: SessionHistory) -> list:
        """ Retrieve the URLs List for the given run date
        """
        logger.debug("%s: Fetching list of urls for date: %s",
                     self.pluginName,
                     str(runDate.strftime("%Y-%m-%d")))
        self.listOfURLS = []
        allURLs = []
        try:
            if self.is_stopped: return []
            rssURLList = self.getArticlesListFromRSS(self.all_rss_feeds)

            if self.is_stopped: return []
            newsPaperLibURLList = self.extractArticlesListWithNewsP()

            if self.is_stopped: return []
            main_page_list = self.extr_links_from_main_noncont(runDate)

            if self.is_stopped: return []
            pending_urls = sessionHistoryDB.retrieveTodoURLList(self.pluginName)
            # concatenate all lists of URLs, and de-duplicate them:
            allURLs = scraper_utils.deDupeList(
                BasePlugin.concat_lists(
                    allURLs, rssURLList, newsPaperLibURLList, main_page_list, pending_urls
                )
            )
        except Exception as e:
            logger.error("%s: Error retrieving list of URLs from main URL and pending table: %s", self.pluginName, e)
        try:
            if self.app_config.recursion_level > 1 and hasattr(self, 'enable_recursion') and self.enable_recursion:
                if self.is_stopped: return allURLs
                recursive_urls = self.getLinksRecursively(allURLs, runDate, self.app_config.recursion_level)
                if recursive_urls:
                    allURLs = scraper_utils.deDupeList(allURLs + recursive_urls)
        except Exception as e:
            logger.error(f"{self.pluginName}: Error trying to validate retrieved listing of URLs: {e}")
        return allURLs

    def getLinksRecursively(self, allURLs: list, run_date: datetime, recursionLevel: int) -> list:
        """
        Get Links Recursively up to given level of recursion using iterative approach.

        This method replaces the previous recursive implementation with an iterative loop
        to avoid stack overflow and provide better control over depth limiting.

        Args:
            allURLs (list): Initial list of URL strings to parse
            run_date (datetime): Business date for processing
            recursionLevel (int): Maximum depth to recurse (limited to 4 levels)

        Returns:
            list: Combined list of all discovered URLs from all recursion levels
        """
        # Limit recursion to maximum of 4 levels
        maxRecursionLevel = min(recursionLevel, 4)
        logger.info(f"{self.pluginName}: Starting recursive URL extraction " +
                    f"(max depth: {maxRecursionLevel})")

        # Track all discovered URLs across all levels
        allDiscoveredURLs = list(allURLs)  # Start with initial URLs

        # Track URLs to process at current level
        currentLevelURLs = list(allURLs)

        try:
            # Iterate through each recursion level
            for depth in range(1, maxRecursionLevel + 1):
                # Check if plugin has been stopped
                if self.is_stopped:
                    logger.info(f"{self.pluginName}: Recursion stopped at depth {depth}")
                    break

                # Skip if no URLs to process at this level
                if not currentLevelURLs:
                    logger.info(f"{self.pluginName}: No URLs to process at depth {depth}")
                    break

                logger.info(f"{self.pluginName}: Extracting URLs at depth {depth}, " +
                            f"processing {len(currentLevelURLs)} URLs")

                # Extract links from current level URLs
                nextLevelURLs = self.extr_links_from_urls_list(run_date, currentLevelURLs)

                # Check if we found any new URLs
                if nextLevelURLs:
                    logger.info(f"{self.pluginName}: Found {len(nextLevelURLs)} URLs at depth {depth}")

                    # Add newly discovered URLs to the master list
                    allDiscoveredURLs.extend(nextLevelURLs)

                    # Prepare for next level
                    currentLevelURLs = nextLevelURLs
                else:
                    logger.info(f"{self.pluginName}: No URLs found at depth {depth}, stopping recursion")
                    break

                # Check again if plugin has been stopped during processing
                if self.is_stopped:
                    logger.info(f"{self.pluginName}: Recursion stopped after depth {depth}")
                    break

            # Remove duplicates from all discovered URLs
            allDiscoveredURLs = scraper_utils.deDupeList(allDiscoveredURLs)

            # Filter out invalid URLs
            allDiscoveredURLs = self.filterInvalidURLs(allDiscoveredURLs)

            logger.info(f"{self.pluginName}: Recursive extraction complete. " +
                        f"Total unique valid URLs: {len(allDiscoveredURLs)}")

        except Exception as e:
            logger.error(f"{self.pluginName}: Error during recursive link extraction: {e}")

        return allDiscoveredURLs

    @staticmethod
    def extractPublishedDate(htmlText,
                             date_regex_patterns: dict,
                             URL: str = '',
                             plugin_name: str = '') -> datetime:
        """ Extract the published date from html content. Set default value as today's date.

        :param htmlText: HTML content to search for published date, handles both bytes and str objects.
        :param date_regex_patterns: Dictionary/Map of compiled regular expressions as keys and date format as values
        :param URL: URL for which date is being searched and identified
        :param plugin_name: Name of the plugin
        :return: Published date
        """
        date_obj = datetime.now()
        currentDateTime = datetime.now()

        # convert to string if the input is in bytes:
        if type(htmlText) == bytes:
            htmlText = htmlText.decode('UTF-8')

        dateString = ""
        errorFlag = True
        for dateRegex in date_regex_patterns.keys():
            (datePattern, datetimeFormatStr) = date_regex_patterns[dateRegex]
            try:
                result = datePattern.search(htmlText)
                logger.debug("For date pattern = %s , Result = %s", datePattern, result)
                if result is not None:
                    dateString = result.group(2)
                    # logger.debug("Matched and extracted date string: %s", dateString)
                    date_obj = datetime.strptime(dateString, datetimeFormatStr).replace(tzinfo=None)
                    if date_obj > currentDateTime:
                        logger.debug(f"{plugin_name}: ERROR: Publish date is in the future: {date_obj}, for URL: {URL}")
                    else:
                        # if we did not encounter any error till this point, and were able to get a date object
                        # hence this is the answer, so exit the loop
                        errorFlag = False
                        break
            except Exception as e:
                logger.debug("%s: Could not identify article date: %s, string to parse: %s, using regexp: %s, URL: %s",
                             plugin_name, e, dateString, dateRegex, URL)
        if errorFlag is True:
            logger.debug("%s: Could not identify published date for article at URL: %s",
                         plugin_name,
                         URL)
            raise ScrapeError("Invalid article since the publish date of article could not be identified.")
        return date_obj

    def extr_links_from_main_noncont(self, runDate: datetime) -> list:
        """ Extract article list from main URL
        """
        listof_URLs = []
        try:
            urlsToBeExtracted = [self.mainURL] + self.nonContentURLs
            listof_URLs = self.extr_links_from_urls_list(runDate, urlsToBeExtracted)
            listof_URLs = listof_URLs + self.extractArchiveURLLinksForDate(runDate)
        except Exception as e:
            logger.error("%s: When Extracting article list from main URL, error was: %s",
                         self.pluginName, e)
        logger.info(f'{self.pluginName}: Identified {len(listof_URLs)} URLs from the main page and non-content URLs.')
        return listof_URLs

    def extr_links_from_urls_list(self, runDate: datetime, listOfURLs: list) -> list:
        """ Extract links from each of the contents of the given list of URLs
        The function argument runDate is not used here, but kept for future possible use.

        :param runDate: Unused argument, may be None
        :param listOfURLs: List of URLs to fetch and parse for discovering additional links
        :return: List of additional URL strings
        """
        listof_URLs = []
        extractedListOfURLs = []
        for url_string in scraper_utils.deDupeList(listOfURLs):
            try:
                htmlContent, httpError = self.networkHelper.fetchRawDataFromURL(url_string, self.pluginName)
                if httpError:
                    logger.warning(f"{self.pluginName}: HTTP {httpError.status_code} for extra link {url_string}")
                    # add this url to failed_urls table
                    # Queue database operation to save failed URL
                    if hasattr(self, 'queue_manager') and self.queue_manager:
                        self.queue_manager.queueDBOperation(
                            'add_failed',
                            (url_string, self.pluginName, datetime.now()),
                            wait_for_result=False
                        )
                extractedListOfURLs = self.extractLinksFromHTML(url_string, htmlContent)
                listof_URLs.extend(scraper_utils.deDupeList(extractedListOfURLs))
            except Exception as e2:
                logger.error("%s: Error fetching additional links for URL %s: %s",
                             self.pluginName,
                             url_string,
                             e2)
        logger.info("%s: Identified %s additional URLs, filtered count of links = %s",
                    self.pluginName,
                    len(extractedListOfURLs),
                    len(listof_URLs))
        return listof_URLs

    def extractLinksFromHTML(self, linkURL: str, htmlContent) -> list:
        """ Extract links from HTML content

        :param linkURL: URL of the content.
        :param htmlContent: HTML content text
        :return: List of new URLs
        """
        htmlContent = scraper_utils.clean_non_utf8(htmlContent)
        docRoot = BeautifulSoup(markup=htmlContent, features='lxml')
        extractedListOfURLs = extractLinks(linkURL, docRoot)
        return scraper_utils.deDupeList(extractedListOfURLs)

    def extractUniqueIDFromURL(self, URLToFetch: str) -> str:
        """ Identify the unique ID from the URL by extracting RegEx patterns matching any of urlMatchPatterns

        :param URLToFetch: URL string from which the unique ID needs to be identified and extracted
        :return: Unique identifier as a text string.
        """
        uniqueString = ""
        crcValue = "zzz-zzz-zzz"
        try:
            # calculate CRC string if url are not usable:
            crcValue = calculateCRC32(URLToFetch)
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
                        return uniqueString
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

    def downloadDataArchive(self, url: str, pluginName: str) -> bytes:
        """ Download data archive using HTTP(s) GET protocol.

        :param url: URL to fetch
        :param pluginName: Name of the plugin
        :return: bytes
        """
        htmlcontent = b""
        try:
            httpResp = self.networkHelper.getHTTPData(url)
            if httpResp is not None:
                htmlcontent = httpResp.content
        except Exception as e:
            logger.error("%s: Error when downloading Data Archive: %s", pluginName, e)
        return htmlcontent

    def writeFiles(self, article: NewsEvent, fileNameWithOutExt: str, htmlContent, saveHTMLFile: bool = False):
        jsonContent = article.toJSON()
        article_id = article.getArticleID()
        publish_date = article.getPublishDate()
        try:
            archive_path, internal_path = self.save_article_to_archive(
                json_content=jsonContent,
                raw_html=htmlContent,
                article_id=str(article_id),
                publish_date=publish_date
            )
            logger.info(f"Saved article {article_id} to archive: {archive_path}")
            return archive_path, internal_path
        except Exception as e:
            logger.error(f"Error saving to archive: {e}", exc_info=True)
            # Fall back to legacy storage if archive fails
            logger.warning("Falling back to legacy file storage")


    def fetchDataFromURL(self, uRLtoFetch: str, WorkerID: int) -> ExecutionResult:
        """
        Fetches and cleans data from a given URL. It takes in two inputs - the URL to fetch (uRLtoFetch), and an identifier for the worker thread executing the plugin (WorkerID).
        It returns an ExecutionResult object containing the fetched data, status, and metadata like content lengths and publish date.
        The main steps are:
          - Create an empty ExecutionResult object to hold the result.
          - Try fetching raw HTML data from the URL using a helper method.
          - Check that the URL and data are valid. If invalid, return the empty result.
          - Extract any additional links from the HTML. Filter for valid URLs.
          - Create a Newspaper Article object and set the HTML content.
          - Parse and clean the article data, checking for valid content.
          - If the text content exceeds a minimum length, save the article to file uniquely named by date and ID.
          - Populate and return the ExecutionResult object with the article data, text length, raw HTML size etc.
        So in summary, it takes in a URL, fetches and cleans the HTML content, extracts the main article data, checks its validity, saves the content and returns an object containing the final article text and metadata. The key steps are data fetching, cleaning, parsing and validation before saving the result.

        :param uRLtoFetch: The URL to be fetched by the plugin
        :param WorkerID: The identifier of the worker thread executing this plugin.
        :return: An ExecutionResult object that contains the data and status after fetch has completed.
        """
        # create empty data strcut to hold the fetch result:
        resultVal = ExecutionResult(uRLtoFetch, 0, 0, None, self.pluginName)
        additionalLinks = []
        self.URLToFetch = uRLtoFetch

        # Get shutdown event from queue manager if available
        shutdown_event = getattr(self, 'shutdown_event', None)

        try:
            self.urlProcessedCount = self.urlProcessedCount + 1

            # Check shutdown before starting
            if shutdown_event and shutdown_event.is_set():
                return resultVal

            if is_valid_url(uRLtoFetch) is False:
                logger.info(f'{self.pluginName}: Invalid URL, hence ignoring it: {uRLtoFetch}')
                return resultVal

            for item in self.nonContentURLs:
                if sameURLWithoutQueryParams(uRLtoFetch, item) is True:
                    logger.debug("%s: Ignoring non-content URL/not retrieving it: %s",
                                 self.pluginName, uRLtoFetch.encode("ascii", "error"))
                    return resultVal

            if uRLtoFetch not in self.nonContentURLs:
                # Pass shutdown_event to network fetcher
                fetch_result = self.networkHelper.fetchRawDataFromURL(
                    uRLtoFetch,
                    self.pluginName,
                    shutdown_event=shutdown_event
                )

                # Handle tuple return (content, http_error) or backward compatible single return
                if isinstance(fetch_result, tuple):
                    htmlContent, http_error = fetch_result

                    # Check shutdown after fetch
                    if shutdown_event and shutdown_event.is_set():
                        return resultVal

                    if http_error and http_error.is_permanent:
                        logger.warning(f'{self.pluginName}: Skipping URL due to HTTP {http_error.status_code}: {uRLtoFetch}')
                        resultVal.http_error = http_error
                        # Queue database operation to save failed URL
                        if hasattr(self, 'queue_manager') and self.queue_manager:
                            self.queue_manager.queueDBOperation(
                                'add_failed',
                                (uRLtoFetch, self.pluginName, datetime.now()),
                                wait_for_result=False
                            )
                        return resultVal
                    elif http_error:
                        logger.warning(f'{self.pluginName}: Temporary HTTP error {http_error.status_code} for URL: {uRLtoFetch}')
                        # Queue database operation to save failed URL
                        if hasattr(self, 'queue_manager') and self.queue_manager:
                            self.queue_manager.queueDBOperation(
                                'add_failed',
                                (uRLtoFetch, self.pluginName, datetime.now()),
                                wait_for_result=False
                            )
                        return resultVal
                else:
                    # Backward compatibility - single return value
                    htmlContent = fetch_result
                    http_error = None

                if htmlContent is not None and len(htmlContent) > self.minArticleLengthInChars:
                    resultVal.rawDataSize = len(htmlContent)
                    logger.debug("%s: Fetched %s characters of html data", self.pluginName, len(htmlContent))

                    # Check shutdown before processing
                    if shutdown_event and shutdown_event.is_set():
                        return resultVal

                    htmlContent = NewsEvent.cleanText(htmlContent)
                    additionalLinks = self.filterNonContentURLs(self.extractLinksFromHTML(uRLtoFetch, htmlContent))
                    additionalLinks = self.filterInvalidURLs(additionalLinks)
                    # Limit additional links to prevent overwhelming the queue
                    if len(additionalLinks) > 500:
                        logger.warning(f"{self.pluginName}: Truncating {len(additionalLinks)} additional links to 500")
                        additionalLinks = additionalLinks[:500]

                    newsPaperArticle = Article(uRLtoFetch, config=self.networkHelper.newspaper_config)
                    newsPaperArticle.download(input_html=htmlContent)

                    # Check shutdown before parsing
                    if shutdown_event and shutdown_event.is_set():
                        return resultVal

                    validData = self.parseFetchedData(uRLtoFetch, newsPaperArticle, WorkerID)
                    resultVal.textSize = validData.getTextSize()

                    if validData.getTextSize() > self.minArticleLengthInChars:
                        savefileNameWithOutExt = BasePlugin.makeUniqueFileName(
                            self.pluginName,
                            self.identifyDataPathForRunDate(self.baseDirName,
                                                            validData.getPublishDate().strftime("%Y-%m-%d")),
                            validData.getArticleID(),
                            URL=validData.getURL())
                        # validData.writeFiles(savefileNameWithOutExt,
                        #                      str(htmlContent),
                        #                      saveHTMLFile=self.bSaveHTMLFile)
                        self.writeFiles(validData, savefileNameWithOutExt, htmlContent.encode('utf-8')) # TOD convert into bytes
                        resultVal = ExecutionResult(uRLtoFetch,
                                                    validData.getHTMLSize(),
                                                    validData.getTextSize(),
                                                    validData.getPublishDate(),
                                                    self.pluginName,
                                                    dataFileName=savefileNameWithOutExt,
                                                    additionalLinks=additionalLinks,
                                                    success=True)
                        resultVal.articleID = validData.getArticleID()
                    else:
                        logger.debug("%s: Insufficient or invalid data (%s characters) retrieved for URL: %s",
                                     self.pluginName,
                                     validData.getTextSize(),
                                     uRLtoFetch.encode('ascii', "ignore"))
        except Exception as e:
            logger.info("%s: Ignoring URL %s due to: %s",
                        self.pluginName, uRLtoFetch.encode('ascii', "ignore"), e)

        self.tempArticleData = None
        self.URLToFetch = None
        return resultVal

    def checkAndCleanText(self, inputText: str, rawData: str, url: str) -> str:
        pass

    def parseFetchedData(self, uRLtoFetch: str, newpArticleObj, WorkerID: int) -> NewsEvent:
        """Parse the fetched news article data to validate it, clean it,
         and then extract vital elements if these are missing.
         Return a NewsEvent object with complete and cleaned data
        """
        logger.debug("%s: Parsing the fetched Data from %s, WorkerID = %s",
                     self.pluginName,
                     uRLtoFetch,
                     WorkerID)
        articleUniqueID = None
        # all data will be stored in this object:
        parsedCleanData = NewsEvent()
        try:
            newpArticleObj.parse()
            # run nlp to parse data for keywords, etc. (note: this requires nltk data to be downloaded)
            newpArticleObj.nlp()
        except Exception as e:
            logger.error("%s: Error parsing raw HTML from URL %s: %s", self.pluginName, uRLtoFetch, e)

        # run custom clean-up code on the text:
        newpArticleObj.text = self.checkAndCleanText(newpArticleObj.text, newpArticleObj.html, newpArticleObj.url)
        logger.debug("Published date: %s", newpArticleObj.publish_date)
        # check date validity:
        if (newpArticleObj.publish_date is None or newpArticleObj.publish_date == '' or
                (not newpArticleObj.publish_date) or (type(newpArticleObj.publish_date) == 'datetime.datetime' and
                                                      newpArticleObj.publish_date > datetime.now())):
            # extract published date by searching for specific tags:
            newpArticleObj.publish_date = BasePlugin.extractPublishedDate(newpArticleObj.html,
                                                                          self.dateMatchPatterns,
                                                                          URL=self.pluginName,
                                                                          plugin_name=self.pluginName)

        try:
            # identify news agency/source if it is not properly recognized:
            if (len(newpArticleObj.authors) < 1 or
                    (len(newpArticleObj.authors) > 0 and newpArticleObj.authors[0].find('<') >= 0)):
                newpArticleObj.authors = self.extractAuthors(newpArticleObj.html)

            # for special cases, unique id is embedded in HTML content,
            # in such cases use the method extractUniqueIDFromContent() to identify the unique ID
            if 'extractUniqueIDFromContent' in dir(self):
                articleUniqueID = self.extractUniqueIDFromContent(newpArticleObj.html, uRLtoFetch)
                logger.debug("%s: Extracted unique ID from HTML content: %s",
                             self.pluginName, articleUniqueID)
            else:
                # otherwise, for almost all other cases, use URL to identify unique ID
                articleUniqueID = self.extractUniqueIDFromURL(uRLtoFetch)
        except Exception as e:
            logger.error("%s: Error setting author / unique id for URL %s: %s",
                         self.pluginName,
                         uRLtoFetch,
                         e)

        # put all the cleaned up data into an NewsEvent object: parsedCleanData
        try:
            parsedCleanData.importNewspaperArticleData(newpArticleObj)
            # deprecated - use plugin instead: parsedCleanData.identifyTriggerWordFlags(self.configReader)
            # identify and set industries from url and content
            parsedCleanData.setIndustries(self.extractIndustries(uRLtoFetch, parsedCleanData.html))
            parsedCleanData.setArticleID(articleUniqueID)
            parsedCleanData.setModuleName(self.pluginName)
        except Exception as e:
            logger.error("%s: Error storing parsed data for URL %s: %s",
                         self.pluginName,
                         uRLtoFetch,
                         e)
        return parsedCleanData

# # end of file # #
