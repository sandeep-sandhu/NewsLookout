#!/usr/bin/env python
# -*- coding: utf-8 -*-

##########################################################################################################
# File name: mod_json_to_csv.py                                                                          #
# Application: The NewsLookout Web Scraping Application                                                  #
# Date: 2021-06-01                                                                                       #
# Purpose: Adds a parsed news event as a new row in a .csv file for a specific date.                     #
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

from data_structs import PluginTypes
from base_plugin import BasePlugin
from news_event import NewsEvent
from datetime import datetime

##########

logger = logging.getLogger(__name__)


class mod_json_to_csv(BasePlugin):
    """ Web Scraping plugin: mod_json_to_csv
    Description: Adds a parsed news event as a new row in a .csv file for a specific date.
    """

    # define a minimum count of characters for text body, article content below this limit will be ignored
    minArticleLengthInChars = 400

    # implies web-scraper for news content, see data_structs.py for other types
    pluginType = PluginTypes.MODULE_DATA_PROCESSOR

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
    nonContentStrings = []

    # never fetch URLs containing these strings:
    invalidURLSubStrings = []

    # remove these substrings from text during cleanup
    subStringsToFilter = []


    # write the following regexps dict with each key as regexp to match the required date text,
    # group 2 of this regular expression should match the date string
    # in this dict, put the key will be the date format expression
    # to be used for datetime.strptime() function, refer to:
    # https://docs.python.org/3/library/datetime.html#datetime.datetime.strptime
    articleDateRegexps = {}

    invalidTextStrings = []

    allowedDomains = []

    articleIndustryRegexps = []

    authorRegexps = []

    # members used by functions of the class:
    authorMatchPatterns = []
    urlMatchPatterns = []
    dateMatchPatterns = dict()
    listOfURLS = []

    # Methods implemented
    def __init__(self):
        """ Initialize the object
        """
        super().__init__()

    def additionalConfig(self, sessionHistoryObj):
        """ Perform additional configuration that is specific to this plugin.

        :param sessionHistoryObj: The session history object to be used by this plugin
         for putting items into the data processing competed queue.
        :return:
        """
        pass


    def processDataObj(self, newsEventObj):
        """ Process given data object by this plugin.

        :param newsEventObj: The NewsEvent object to be processed.
        :type newsEventObj: NewsEvent
        """
        try:
            assert type(newsEventObj) == NewsEvent
            # TODO: lock file to avoid conflicting writes, release lock at the end of the method
            runDate = datetime.strptime(newsEventObj.getPublishDate(), '%Y-%m-%d')
            logger.debug("JSON-to-CSV: Adding news event data in: %s for date: %s",
                         newsEventObj.getFileName(), runDate)
            # open csv of given date, read csv into a pandas dataframe.
            # check if news event already exists in dataframe
            # if not, then extract all attributes from newsEventObj and add to pandas dataframe
            # write to csv file, without text body of news event.


        except Exception as e:
            logger.error(f'Error processing data: {e}')

# # end of file ##
