#!/usr/bin/env python
# -*- coding: utf-8 -*-

# #########################################################################################################
# File name: data_structs.py                                                                              #
# Application: The NewsLookout Web Scraping Application                                                   #
# Date: 2021-06-23                                                                                        #
# Purpose: Helper class with data structures supporting the web scraper                                   #
# Copyright 2021, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu, sandeep.sandhu@gmx.com  #
#                                                                                                         #
# Provides:                                                                                               #
#    Types                                                                                                #
#    ScrapeError                                                                                          #
#    ExecutionResult                                                                                      #
#    QueueStatus                                                                                          #
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

# import internal libraries


##########

# setup logging
logger = logging.getLogger(__name__)

##########


class Types:
    MODULE_NEWS_CONTENT = 1
    MODULE_NEWS_AGGREGATOR = 2
    MODULE_NEWS_API = 4
    MODULE_DATA_CONTENT = 8
    MODULE_DATA_PROCESSOR = 16

    TASK_GET_URL_LIST = 32
    TASK_GET_DATA = 64
    TASK_PROCESS_DATA = 128

    STATE_GET_URL_LIST = 10
    STATE_FETCH_CONTENT = 20
    STATE_PROCESS_DATA = 40
    STATE_STOPPED = 80
    STATE_NOT_STARTED = 160

    @staticmethod
    def decodeNameFromIntVal(typeIntValue):
        attrNames = dir(Types)
        for name in attrNames:
            attrIntVal = getattr(Types, name, None)
            if attrIntVal == typeIntValue:
                return(name)


##########

# objects/data structures for URL lists, and news article data


class ScrapeError(Exception):
    pass


class ExecutionResult:
    """ Object that encapsulates the result of data retrieval/web-scraping
    """
    URL = None
    rawDataFileName = None
    savedDataFileName = None
    rawDataSize = 0
    textSize = 0
    publishDate = None
    pluginName = None

    def __init__(self, sURL, htmlContentLen, textContentLen, publishDate, pluginName,
                 dataFileName=None, rawDataFile=None, success=False):
        self.URL = sURL
        self.rawDataFileName = rawDataFile
        self.savedDataFileName = dataFileName
        self.rawDataSize = htmlContentLen
        self.textSize = textContentLen
        self.publishDate = publishDate
        self.pluginName = pluginName
        self.wasSuccessful = success

    def getAsTuple(self):
        return((self.URL, self.pluginName, self.publishDate, self.rawDataSize, self.textSize))


class QueueStatus:
    queue_mgr = None
    qsizeMap = {}
    totalQsizeMap = {}
    currentState = {}
    totalURLCount = 0
    countOfPluginsInURLSrcState = 0
    isPluginStillFetchingoverNetwork = False
    fetchCompletQsize = 0
    fetchCompletCount = 0
    dataInputQsize = 0
    dataOutputQsize = 0

    def __init__(self, queue_manager):
        self.queue_mgr = queue_manager

    def updateStatus(self):
        # update
        for pluginName in self.queue_mgr.pluginNameToObjMap.keys():
            self.qsizeMap.update({pluginName: self.queue_mgr.pluginNameToObjMap[pluginName].getQueueSize()})
            self.totalQsizeMap[pluginName] = self.queue_mgr.pluginNameToObjMap[pluginName].urlQueueTotalSize
            self.totalURLCount = self.totalURLCount + self.queue_mgr.pluginNameToObjMap[pluginName].urlQueueTotalSize
            self.isPluginStillFetchingoverNetwork = self.isPluginStillFetchingoverNetwork or (
                    self.queue_mgr.pluginNameToObjMap[pluginName].pluginState in [
                Types.STATE_GET_URL_LIST, Types.STATE_FETCH_CONTENT]
            )
            self.currentState.update(
                {pluginName: Types.decodeNameFromIntVal(self.queue_mgr.pluginNameToObjMap[pluginName].pluginState)}
            )
            if self.queue_mgr.pluginNameToObjMap[pluginName].pluginState == Types.STATE_GET_URL_LIST:
                self.countOfPluginsInURLSrcState = self.countOfPluginsInURLSrcState + 1
        # update other queue parameters
        self.fetchCompletQsize = self.queue_mgr.fetchCompletedQueue.qsize()
        self.fetchCompletCount = self.queue_mgr.fetchCompletedCount
        self.dataInputQsize = self.queue_mgr.getCompletedQueueSize()
        self.dataOutputQsize = self.queue_mgr.getDataProcessedQueueSize()


# # end of file ##
