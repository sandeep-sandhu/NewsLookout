#!/usr/bin/env python
# -*- coding: utf-8 -*-

# #########################################################################################################
#                                                                                                         #
# File name: mod_dataprep.py                                                                              #
# Application: The NewsLookout Web Scraping Application                                                   #
# Date: 2021-06-23                                                                                        #
# Purpose: Plugin for custom data processing of news articles text                                        #
# Copyright 2021, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu, sandeep.sandhu@gmx.com  #
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
import logging
from datetime import datetime

# import this project's python libraries:
from base_plugin import BasePlugin
from data_structs import PluginTypes
from news_event import NewsEvent
from session_hist import SessionHistory

##########

logger = logging.getLogger(__name__)

###########


class mod_dataprep(BasePlugin):
    """ Web Scraping plugin: mod_dataprep
    For preparing already downloaded data
    """
    minArticleLengthInChars = 400
    pluginType = PluginTypes.MODULE_DATA_PROCESSOR  # implies data post-processor

    listOfFiles = []
    uRLdata = dict()

    def __init__(self):
        """ Initialize the object
        """
        super().__init__()

    def additionalConfig(self, sessionHistoryObj: SessionHistory):
        """ Perform additional configuration that is specific to this plugin.

        :param sessionHistoryObj: The session history object to be used by this plugin
         for putting items into the data processing competed queue.
        :return:
        """
        self.workDir = self.app_config.data_dir
        self.sessionHistDB = sessionHistoryObj

    def processDataObj(self, newsEventObj: NewsEvent):
        """ Process given data object by this plugin, read the contents from the file and
        directly write back the transformed data back to the same file.

        :param newsEventObj: The NewsEvent object to be processed.
        :type newsEventObj: NewsEvent
        """
        try:
            assert type(newsEventObj) == NewsEvent
            # TODO: lock file to avoid conflicting writes, release lock at the end of the method
            runDate = newsEventObj.getPublishDate()
            logger.debug("Started data pre-processing for news event data in: %s for date: %s",
                         newsEventObj.getFileName(), runDate)
            newsEventObj.setText(
                self.cleanText(newsEventObj.getText())
                )
            # prepare filename:
            fileNameWOExt = newsEventObj.getFileName().replace('.json', '')
            # save document to file:
            newsEventObj.writeFiles(fileNameWOExt, '', saveHTMLFile=False)
        except Exception as e:
            logger.error(f'Error processing data: {e}')

    def cleanText(self, inputText: str) -> str:
        """
        Examine and clean the text from the document and return cleaned text.

        :param inputText: Text to be examined and cleaned.
        :type inputText: str
        :return: Cleaned text
        :rtype: str
        """
        # TODO: apply text cleaning logic
        outputText = inputText.strip()
        return outputText

# # end of file ##
