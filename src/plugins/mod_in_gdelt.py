#!/usr/bin/env python
# -*- coding: utf-8 -*-

##########################################################################################################
# File name: mod_in_gdelt.py                                                                             #
# Application: The NewsLookout Web Scraping Application                                                  #
# Date: 2021-06-23                                                                                       #
# Purpose: Plugin for GDELT news aggregator                                                              #
# Copyright 2021, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu, sandeep.sandhu@gmx.com #
#                                                                                                        #
#                                                                                                        #
# Notice:                                                                                                #
# This software is intended for demonstration and educational purposes only. This software is            #
# experimental and a work in progress. Under no circumstances should these files be used in              #
# relation to any critical system(s). Use of these files is at your own risk.                            #
#                                                                                                        #
# Before using it for web scraping any website, always consult that website's terms of use.              #
# Do not use this software to fetch any data from any website that has forbidden use of web              #
# scraping or similar mechanisms, or violates its terms of use in any other way. The author is           #
# not liable for such kind of inappropriate use of this software.                                        #
#                                                                                                        #
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,                    #
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR               #
# PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE              #
# FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR                   #
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER                 #
# DEALINGS IN THE SOFTWARE.                                                                              #
#                                                                                                        #
##########################################################################################################

# import standard python libraries:
import logging

# import web retrieval and text processing python libraries:
import os

import pandas as pd
import zipfile
from io import BytesIO

import scraper_utils
from data_structs import Types
from base_plugin import BasePlugin
from scraper_utils import filterRepeatedchars, deDupeList

##########

logger = logging.getLogger(__name__)


class mod_in_gdelt(BasePlugin):
    """ Web Scraping plugin: mod_in_gdelt
    Description: GDELt news aggregator
    Language: English
    Country: India
    """

    # define a minimum count of characters for text body, article content below this limit will be ignored
    minArticleLengthInChars = 400

    # implies web-scraper for news content, see data_structs.py for other types
    pluginType = Types.MODULE_NEWS_AGGREGATOR

    # main webpage URL
    mainURL = "http://data.gdeltproject.org/events/YYYYMMDD.export.CSV.zip"
    mainURLDateFormatted = "http://data.gdeltproject.org/events/%Y%m%d.export.CSV.zip"

    # RSS feeds to pick up latest news article links
    all_rss_feeds = ["http://data.gdeltproject.org/events/index.html"]

    # fetch only URLs containing the following substrings:
    validURLStringsToCheck = []

    # this list of URLs will be visited to get links for articles,
    # but their content will not be scraped to pick up news content
    nonContentURLs = [
        mainURL
       ]
    nonContentStrings = []

    # never fetch URLs containing these strings:
    invalidURLSubStrings = []

    # write regexps in three groups ()()() so that the third group
    # gives a unique identifier such as a long integer at the end of a URL
    # this third group will be selected as the unique identifier:
    urlUniqueRegexps = [r"(^http\://data.gdeltproject.org\/)(events\/)([0-9]+)(\.export\.CSV\.zip$)"]

    # write the following regexps dict with each key as regexp to match the required date text,
    # group 2 of this regular expression should match the date string
    # in this dict, put the key will be the date format expression
    articleDateRegexps = {}

    invalidTextStrings = []

    allowedDomains = ['data.gdeltproject.org']

    articleIndustryRegexps = []

    authorRegexps = []

    # members used by functions of the class:
    authorMatchPatterns = []
    urlMatchPatterns = []
    dateMatchPatterns = dict()
    listOfURLS = []

    # --- Methods to be implemented ---
    def __init__(self):
        """ Initialize the object
        Use base class's lists and dicts in searching for unique url and published date strings
        """
        self.articleDateRegexps.update(super().articleDateRegexps)
        self.urlUniqueRegexps = self.urlUniqueRegexps + super().urlUniqueRegexps
        super().__init__()

    def getURLsListForDate(self, runDate, sessionHistoryDB):
        """ Extract article list from the main URL.
        Since this is only a news aggregator, sets the plugin state to Types.STATE_STOPPED
         at the end of this method.

        :param sessionHistoryDB: Not used in this function
        :param runDate: Given query date for which news article URLs are to be retrieved
        :type runDate: datetime.datetime
        :return: List of URLs identified from this news source
        :rtype: list[str]
        """
        urlList = []
        searchResultsURLForDate = None
        try:
            prevDay = scraper_utils.getPreviousDaysDate(runDate)
            prevToPrevDay = scraper_utils.getPreviousDaysDate(prevDay)
            dataDirForDate = BasePlugin.identifyDataPathForRunDate(self.app_config.data_dir,
                                                                   prevToPrevDay)
            if 'mainURLDateFormatted' in dir(self) and self.mainURLDateFormatted is not None:
                searchResultsURLForDate = prevToPrevDay.strftime(self.mainURLDateFormatted)
                logger.debug('Downloading file from URL: %s', searchResultsURLForDate)
                csv_zip = self.downloadDataArchive(searchResultsURLForDate, self.pluginName)
                filebytes = BytesIO(csv_zip)
                zipDatafile = zipfile.ZipFile(filebytes, mode='r')
                # unzip csv data, write to file:
                for memberZipInfo in zipDatafile.infolist():
                    zipDatafile.extract(memberZipInfo, path=dataDirForDate)
                    csv_filename = os.path.join(dataDirForDate, memberZipInfo.filename)
                    logger.debug("Expanded the fetched Zip archive to: %s", csv_filename)
                    # load csv file in pandas:
                    # Columns (14,24) have mixed types. Specify dtype option on import or set low_memory=False.
                    urlDF = pd.read_csv(csv_filename, delimiter='\t', header=None, low_memory=False)
                    # filter and identify URLs for india:
                    # column 51 is country, column 57 is URL
                    for item in urlDF[urlDF.iloc[: , 51] == 'IN'].iloc[:,57].values:
                        # put urls into list:
                        urlList.append(item.strip())
                    # delete csv file:
                    os.remove(csv_filename)
            logger.info("Added %s URLs from aggregated news from %s", len(urlList), searchResultsURLForDate)
        except Exception as e:
            logger.error("%s: When Extracting URL list from main URL, error was: %s",
                         self.pluginName, e)
        self.pluginState = Types.STATE_STOPPED
        return(urlList)

    # *** MANDATORY to implement ***
    def checkAndCleanText(self, inputText, rawData):
        """ Check and clean article text
        """
        cleanedText = inputText
        invalidFlag = False
        try:
            for badString in self.invalidTextStrings:
                if cleanedText.find(badString) >= 0:
                    logger.debug("%s: Found invalid text strings in data extracted: %s", self.pluginName, badString)
                    invalidFlag = True
            # check if article content is not valid or is too little
            if invalidFlag is True or len(cleanedText) < self.minArticleLengthInChars:
                cleanedText = self.extractArticleBody(rawData)
            # replace repeated spaces, tabs, hyphens, '\n', '\r\n', etc.
            cleanedText = filterRepeatedchars(cleanedText,
                                              deDupeList([' ', '\t', '\n', '\r\n', '-', '_', '.']))
            # remove invalid substrings:
            for stringToFilter in deDupeList(self.subStringsToFilter):
                cleanedText = cleanedText.replace(stringToFilter, " ")
        except Exception as e:
            logger.error("Error cleaning text: %s", e)
        return(cleanedText)

# # end of file ##
