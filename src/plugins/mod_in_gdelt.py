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
from datetime import datetime

import pandas as pd
import zipfile
from io import BytesIO

import scraper_utils
from data_structs import Types
from base_plugin import BasePlugin
from scraper_utils import deDupeList

##########
from session_hist import SessionHistory

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

    def getURLsListForDate(self, runDate: datetime, sessionHistoryDB: SessionHistory) -> list:
        """ Extract article list from the main URL.
        Since this is only a news aggregator, sets the plugin state to Types.STATE_STOPPED
         at the end of this method.

        :param sessionHistoryDB: Not used in this function
        :param runDate: Given query date for which news article URLs are to be retrieved
        :type runDate: datetime.datetime
        :return: List of URLs identified from this news source
        :rtype: list
        """
        urlList = []
        try:
            searchResultsURLForDate, dataDirForDate = self.prepare_url_datadir_for_date(runDate)
            if searchResultsURLForDate is not None:
                logger.debug('Downloading file from URL: %s', searchResultsURLForDate)
                csv_zip = self.downloadDataArchive(searchResultsURLForDate, self.pluginName)
                csv_files = mod_in_gdelt.extract_csvlist_from_archive(csv_zip, dataDirForDate)
                for csv_filename in csv_files:
                    logger.debug("Expanded the fetched Zip archive to: %s", csv_filename)
                    url_items = mod_in_gdelt.extract_urls_from_csv(csv_filename, country_code='IN')
                    urlList = urlList + url_items
                urlList = deDupeList(urlList)
            logger.info("Added %s URLs from aggregated news from %s", len(urlList), searchResultsURLForDate)
        except Exception as e:
            logger.error("%s: When Extracting URL list from main URL, error was: %s",
                         self.pluginName, e)
        self.pluginState = Types.STATE_STOPPED
        return(urlList)

    def prepare_url_datadir_for_date(self, rundate_obj: datetime) -> str:
        """ Prepare URL from given Date.

        :param date_obj: Date for the URL
        :return:
        """
        url_prepared_for_date = None
        prevDay = scraper_utils.getPreviousDaysDate(rundate_obj)
        prevToPrevDay = scraper_utils.getPreviousDaysDate(prevDay)
        if 'mainURLDateFormatted' in dir(self) and self.mainURLDateFormatted is not None:
            url_prepared_for_date = prevToPrevDay.strftime(self.mainURLDateFormatted)
        dataDirForDate = BasePlugin.identifyDataPathForRunDate(self.app_config.data_dir,
                                                               prevToPrevDay)
        return(url_prepared_for_date, dataDirForDate)

    @staticmethod
    def extract_csvlist_from_archive(archive_bytes: bytes, dataDirForDate: str) -> list:
        """ Extract CSV file from compressed archive file

        :param archive_file: Filename of the compressed archive downloaded from the website
        :param dataDirForDate: Data directory where archive would be expanded into
        :return: a list of CSV filenames extracted from the archive
        """
        list_of_files = []
        filebytes = BytesIO(archive_bytes)
        zipDatafile = zipfile.ZipFile(filebytes, mode='r')
        # unzip csv data, write to file:
        for memberZipInfo in zipDatafile.infolist():
            zipDatafile.extract(memberZipInfo, path=dataDirForDate)
            csv_filename = os.path.join(dataDirForDate, memberZipInfo.filename)
            logger.debug(f"Expanded the Zip archive to file: {csv_filename}")
            list_of_files.append(csv_filename)
        zipDatafile.close()
        return(list_of_files)

    @staticmethod
    def extract_urls_from_csv(csv_filename: str, country_code='IN') -> list:
        """ Extract URL list from CSV file

        :param csv_filename: file to read from
        :param country_code: ISO 2-character country code to filter news
        :return: List of relevant URLs extracted from CSV file
        """
        urlList = []
        # load csv file in pandas:
        # Columns (14,24) have mixed types. Specify dtype option on import or set low_memory=False.
        urlDF = pd.read_csv(csv_filename, delimiter='\t', header=None, low_memory=False)
        # filter and identify URLs for india:
        # column 51 is country, column 57 is URL
        for item in urlDF[urlDF.iloc[:, 51] == country_code].iloc[:, 57].values:
            # put urls into list:
            urlList.append(item.strip())
        # delete csv file:
        os.remove(csv_filename)
        return(deDupeList(urlList))


# # end of file ##
