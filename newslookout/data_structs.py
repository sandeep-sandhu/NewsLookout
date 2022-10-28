#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
# #########################################################################################################
# File name: data_structs.py                                                                              #
# Application: The NewsLookout Web Scraping Application                                                   #
# Date: 2021-06-23                                                                                        #
# Purpose: Helper class with data structures supporting the web scraper                                   #
# Copyright 2021, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu, sandeep.sandhu@gmx.com  #
#                                                                                                         #
# Provides:                                                                                               #
#    PluginTypes                                                                                                #
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


class PluginTypes:
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
    def decodeNameFromIntVal(typeIntValue: int) -> str:
        attrNames = dir(PluginTypes)
        for name in attrNames:
            attrIntVal = getattr(PluginTypes, name, None)
            if attrIntVal == typeIntValue:
                return name


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
    articleID = None
    additionalLinks = []

    def __init__(self,
                 sURL: str,
                 htmlContentLen: int,
                 textContentLen: int,
                 publishDate: any,
                 pluginName: str,
                 dataFileName: str = None,
                 rawDataFile: str = None,
                 success: bool = False,
                 additionalLinks: list = None):
        self.URL = sURL
        self.rawDataFileName = rawDataFile
        self.savedDataFileName = dataFileName
        self.rawDataSize = htmlContentLen
        self.textSize = textContentLen
        self.publishDate = publishDate
        self.pluginName = pluginName
        self.wasSuccessful = success
        if additionalLinks is not None:
            self.additionalLinks = additionalLinks
        else:
            self.additionalLinks = []

    def getAsTuple(self) -> tuple:
        """" Get execution result details as a tuple.
        """
        return (self.URL, self.pluginName, self.publishDate, self.rawDataSize, self.textSize)

class PluginStatus:
    start_time: datetime
    list_complete_time: datetime
    fetch_complete_time: datetime
    finish_time: datetime
    total_URL_count: int
    total_url_fetched: int
    plugin_state: int
    error_count: int

    def __init__(self):
        self.start_time = datetime.datetime.now()

    def set_URL_count(self, total_url_count):
        self.total_URL_count = total_url_count

    def set_URL_fetched_count(self, total_url_fetched):
        self.total_url_fetched = total_url_fetched

    def set_plugin_state(self, plugin_state):
        self.plugin_state = plugin_state
        # TODO: set timestamps for each stage

    def getStatusString(pluginType, pluginState) -> str:
        """ Prepare status text to be printed out by the worker thread in the log
        :rtype: str
        :return: String indicating the state of this plugin, e.g. 'State = FETCH DATA'
        """
        if pluginType in [PluginTypes.MODULE_NEWS_CONTENT, PluginTypes.MODULE_DATA_CONTENT]:
            return 'State = ' + PluginTypes.decodeNameFromIntVal(pluginState)
        elif pluginType in [PluginTypes.MODULE_DATA_PROCESSOR]:
            return 'State = ' + PluginTypes.decodeNameFromIntVal(pluginState)
        elif pluginType in [PluginTypes.MODULE_NEWS_AGGREGATOR]:
            return 'State = ' + PluginTypes.decodeNameFromIntVal(pluginState)

class QueueStatus:
    """ This object gets the status of all the plugins and the queues of the application
     and updates its corresponding attributes

    """
    queue_mgr = None
    # - Dictionary map with plugin names as the keys and their current queue sizes as the values.
    qsizeMap = dict()
    # - Count of plugins in URL sourcing state
    fetchPendingCount = 0
    # - Dictionary map with plugin names as the keys and their total queue sizes as values.
    totalQsizeMap = dict()
    # - Dictionary map with plugin names as the keys and their current plugin state as values.
    currentState = dict()
    # - Total count of all URLs attempted in this session
    totalURLCount = 0
    countOfPluginsInURLSrcState = 0
    totalPluginsURLSourcing = 0
    # - Flag indicating if any plugin is still fetching data over network (source URL list or content)
    isPluginStillFetchingoverNetwork = False
    areAllPluginsStopped = False
    # - Data fetch/scrape completed queue size
    fetchCompletQsize = 0
    # - Total fetch completed size
    fetchCompletCount = 0
    # - Data process input queue Size
    dataInputQsize = 0
    # - Data process completed queue size
    dataOutputQsize = 0

    def __init__(self, queue_manager: object):
        """ Instantiate the Queue status object.

        :param queue_manager: The QueueManager instance
        """
        self.queue_mgr = queue_manager
        self.totalPluginsURLSourcing = self.queue_mgr.totalPluginsURLSrcCount

    def get_plugin_state(self, plugin_name: str) -> str:
        """ Get the state of the queried plugin.

        :param plugin_name: Name of the plugin being queried
        :param plugin_name: str
        :return: pluginState value expressed as a PluginTypes attribute value
        :rtype: str
        """
        pluginStateInt = self.queue_mgr.pluginNameToObjMap[plugin_name].pluginState
        return PluginTypes.decodeNameFromIntVal(pluginStateInt)

    def any_newsagg_isactive(self) -> bool:
        """ Check if any news aggregator is still actively fetching URL listing.
        Specifically, check if any plugin that is of news aggregator type is in URL listing state.

        :return: True if any news aggregator is still actively fetching list of URLs
        :rtype: bool
        """
        result = False
        plugin_map = self.queue_mgr.pluginNameToObjMap
        for pluginName in plugin_map.keys():
            if plugin_map[pluginName].pluginType == PluginTypes.MODULE_NEWS_AGGREGATOR:
                result = result or plugin_map[pluginName].pluginState == PluginTypes.STATE_GET_URL_LIST
        return result

    @staticmethod
    def getStatusChange(previousState, currentState) -> list:
        """ Format the status change detected from comparing the previous state of plugins to its current state
        and output these are a list of messages to be logged into the event log.
        """
        statusMessages = []
        # logger.debug(f'Comparing previous state: {previousState}')
        # logger.debug(f'      with current state: {currentState}')
        # current status is: self.currentState, previous status is: self.previousState
        for pluginName in currentState.keys():
            try:
                # for each key in current status, check and compare value in previous state
                if pluginName in previousState and currentState[pluginName] != previousState[pluginName]:
                    # print For plugin z, status changed from x to y
                    statusMessages.append(
                        str(pluginName) +
                        ' changed state to -> ' +
                        currentState[pluginName].replace('STATE_', '').replace('_', ' '))
            except Exception as e:
                logger.error("Progress watcher thread: Error comparing previous state of plugin: %s", e)
        return statusMessages

    def updateStatus(self):
        """ Update the queue status
        """
        self.fetchPendingCount = 0
        self.areAllPluginsStopped = False
        self.isPluginStillFetchingoverNetwork = False
        self.countOfPluginsInURLSrcState = 0
        self.totalURLCount = 0
        self.totalPluginsURLSourcing = self.queue_mgr.totalPluginsURLSrcCount
        for pluginName in self.queue_mgr.pluginNameToObjMap.keys():
            self.qsizeMap.update({pluginName: self.queue_mgr.pluginNameToObjMap[pluginName].getQueueSize()})
            self.fetchPendingCount = (
                    self.fetchPendingCount +
                    self.queue_mgr.pluginNameToObjMap[pluginName].getQueueSize()
                )
            self.totalQsizeMap[pluginName] = self.queue_mgr.pluginNameToObjMap[pluginName].urlQueueTotalSize
            self.totalURLCount = self.totalURLCount + self.queue_mgr.pluginNameToObjMap[pluginName].urlQueueTotalSize
            self.isPluginStillFetchingoverNetwork = (
                self.isPluginStillFetchingoverNetwork or
                self.queue_mgr.pluginNameToObjMap[pluginName].pluginState in [
                    PluginTypes.STATE_GET_URL_LIST, PluginTypes.STATE_FETCH_CONTENT]
                )
            self.areAllPluginsStopped = (
                    self.areAllPluginsStopped and
                    self.queue_mgr.pluginNameToObjMap[pluginName].pluginState == PluginTypes.STATE_STOPPED)
            self.currentState[pluginName] = PluginTypes.decodeNameFromIntVal(
                self.queue_mgr.pluginNameToObjMap[pluginName].pluginState
                )
            if self.queue_mgr.pluginNameToObjMap[pluginName].pluginState == PluginTypes.STATE_GET_URL_LIST:
                self.countOfPluginsInURLSrcState = self.countOfPluginsInURLSrcState + 1
        # update other queue parameters from the queue manager object
        self.fetchCompletQsize = self.queue_mgr.fetchCompletedQueue.qsize()
        self.fetchCompletCount = self.queue_mgr.fetchCompletedCount
        self.dataInputQsize = self.queue_mgr.getCompletedQueueSize()
        self.dataOutputQsize = self.queue_mgr.getDataProcessedQueueSize()


# # end of file ##
