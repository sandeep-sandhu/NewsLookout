#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: worker.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-01-14
 Purpose: This object encapsulates the worker thread that
  runs all multi-threading functionality to run the
  web scraper plugins loaded by the application.

 Copyright 2021, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu, sandeep.sandhu@gmx.com

 Provides:
    worker
        run
        setRunDate
        runURLListGatherTasks
        runDataRetrievalTasks
        runDataProcessingTasks

    histDBWorker
        run


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

# #########

# import standard python libraries:
import logging
import threading
import time
from datetime import datetime
import queue

# import this project's modules
from data_structs import Types

# #########

logger = logging.getLogger(__name__)

# #########


class worker(threading.Thread):
    """
    This worker object runs fetching and data extraction processes within each thread.
    """
    workerID = -1
    taskType = Types.TASK_GET_URL_LIST
    runDate = datetime.now()
    queueFillwaitTime = 20

    def __init__(self, pluginObj, taskType, workCompletedURLs,
                 daemon=None, target=None, name=None):
        """
        Initialize the worker with plugin, task type, and URL completed tracker object
        """
        self.workerID = name
        self.pluginObj = pluginObj
        self.pluginName = type(self.pluginObj).__name__
        self.taskType = taskType
        self.completedURLs = workCompletedURLs
        logger.debug("Worker %s initialized with the plugin: %s", self.workerID, self.pluginObj)
        super().__init__(daemon=daemon, target=target, name=name)

    def setRunDate(self, runDate):
        self.runDate = runDate

    def runURLListGatherTasks(self):
        """
        Run Tasks to gather the listing of URLs
        """
        pluginName = type(self.pluginObj).__name__
        additionalLinks = []
        moreAdditionalLinks = []
        try:
            logger.debug("Started URL gathering for plugin: %s", pluginName)
            # fetch URL list using each plugin's function:
            self.pluginObj.getURLsListForDate(self.runDate)
            # Remove URLs already retrieved earlier: removeAlreadyFetchedURLs(self, sqlCon, newURLsList, pluginName)
            self.pluginObj.listOfURLS = self.completedURLs.removeAlreadyFetchedURLs(
                    self.pluginObj.listOfURLS,
                    pluginName)
            if self.pluginObj.pluginType in [Types.MODULE_NEWS_CONTENT,
                                             Types.MODULE_DATA_CONTENT,
                                             Types.MODULE_NEWS_API
                                             ]:
                self.pluginObj.addURLsListToQueue(self.pluginObj.listOfURLS)
                # fetch links one level deeper from the contents of the URL list !!!!!
                additionalLinks = self.pluginObj.extractLinksFromURLList(self.runDate,
                                                                         self.pluginObj.listOfURLS
                                                                         )
                additionalLinks = self.completedURLs.removeAlreadyFetchedURLs(additionalLinks,
                                                                              pluginName)
                self.pluginObj.addURLsListToQueue(additionalLinks)
                # go another level deeper, extract more links from within the list of URLs:
                moreAdditionalLinks = self.pluginObj.extractLinksFromURLList(self.runDate, additionalLinks)
                if self.pluginObj.listOfURLS is not None and moreAdditionalLinks is not None:
                    for urlItem in self.pluginObj.listOfURLS:
                        if urlItem in moreAdditionalLinks:
                            moreAdditionalLinks.remove(urlItem)
                moreAdditionalLinks = self.completedURLs.removeAlreadyFetchedURLs(
                    moreAdditionalLinks,
                    pluginName)
                self.pluginObj.addURLsListToQueue(moreAdditionalLinks)

            elif self.pluginObj.pluginType == Types.MODULE_NEWS_AGGREGATOR:
                # for news aggregators, put url into common queue:
                self.pluginObj.addURLsListToQueue(self.pluginObj.listOfURLS)

            logger.debug('Thread %s finished getting URL listing for plugin %s',
                         self.workerID,
                         pluginName)
            # saveObjToJSON(self.pluginName + '_additional_list_of_URLS.json', additionalLinks + moreAdditionalLinks )
        except Exception as e:
            logger.error(
                "When trying to get URL listing using plugin: %s, Type TASK_GET_URL_LIST, Exception: %s",
                pluginName,
                e)
        finally:
            self.pluginObj.putQueueEndMarker()

    def runDataRetrievalTasks(self):
        """ Run Data Retrieval Tasks
        """
        retrievedItem = None
        sURL = None
        logger.info("Started data retrieval job for plugin: %s, queue size: %s",
                    self.pluginName,
                    self.pluginObj.urlQueue.qsize()
                    )
        while (not self.pluginObj.urlQueue.empty()) or (self.pluginObj.pluginState == Types.STATE_GET_URL_LIST):
            try:
                sURL = None
                if self.pluginObj.urlQueue.empty():
                    logger.debug('%s: Waiting %s seconds for input queue to fill up; plugin state = %s',
                                 self.pluginName,
                                 self.queueFillwaitTime,
                                 self.pluginObj.pluginState
                                 )
                    time.sleep(self.queueFillwaitTime)
                retrievedItem = self.pluginObj.urlQueue.get(timeout=self.queueFillwaitTime)
                if retrievedItem is not None:
                    (priority, sURL) = retrievedItem
                    self.pluginObj.urlQueue.task_done()
                if sURL is not None:
                    fetchMetrics = self.pluginObj.fetchDataFromURL(sURL, self.workerID)
                    if fetchMetrics is not None:
                        (uRL, len_raw_data, len_text, publish_date) = fetchMetrics
                        if len_text is not None and len_text > 3:
                            self.completedURLs.completedQueue.put(
                                (uRL, len_raw_data, len_text, publish_date, self.pluginName))
            except queue.Empty as qempty:
                logger.debug("%s: Queue was empty when trying to retrieve data: %s",
                             self.pluginName,
                             qempty
                             )
            except Exception as e:
                logger.error("%s: When trying to retrieve data the exception was: %s, retrievedItem = %s, URL = %s",
                             self.pluginName,
                             e,
                             retrievedItem,
                             sURL
                             )
        logger.debug('Thread %s finished tasks to retrieve data for plugin %s',
                     self.workerID,
                     self.pluginName)
        # once all data is fetched, set state to process data:
        self.pluginObj.pluginState = Types.STATE_PROCESS_DATA

    def runDataProcessingTasks(self):
        """ Run Data Processing Tasks """
        try:
            logger.debug('Thread %s given task to process data using plugin %s',
                         self.workerID, self.pluginName)

            if (self.pluginObj.pluginType == Types.MODULE_DATA_PROCESSOR):

                # get data processing initiated for each plugin's function:
                self.pluginObj.processData(self.runDate)

                logger.debug('Thread %s finished task to process data using plugin %s',
                             self.workerID,
                             self.pluginName)

        except Exception as e:
            logger.error("When trying to process data using plugin: %s, Type TASK_PROCESS_DATA: %s, Exception: %s",
                         self.pluginName,
                         self.taskType,
                         e)

    def run(self):
        """ Overridden to enable thread to be called for executing the Plugin jobs
        """
        if self.taskType == Types.TASK_GET_URL_LIST:
            self.runURLListGatherTasks()
        elif self.taskType == Types.TASK_GET_DATA:
            self.runDataRetrievalTasks()
        elif self.taskType == Types.TASK_PROCESS_DATA:
            self.runDataProcessingTasks()


##########


class aggQueueConsumer(threading.Thread):
    """ Worker object to consume aggregator sourced common URLs queue asynchronously
    """
    workerID = -1

    def __init__(self, pluginsList, commonURLsQueue, fetchCycleTime,
                 daemon=None, target=None, name=None):
        """ Initialize the worker object
        """
        self.workerID = name
        self.pluginsDict = pluginsList
        self.commonURLsQueue = commonURLsQueue
        self.fetchCycleTime = min(30, fetchCycleTime)
        logger.debug("Consumer for aggregator sourced common URLs queue worker %s initialized with the plugins: %s",
                     self.workerID,
                     self.pluginsDict)
        super().__init__(daemon=daemon, target=target, name=name)

    def run(self):
        """ Main method run on thread when it is started
        """
        arePluginsStillSourcingData = False
        logger.info('Consumer for aggregator sourced common URLs queue Worker Thread: Started now.')
        try:
            for pluginID in self.pluginsDict.keys():
                arePluginsStillSourcingData = arePluginsStillSourcingData or (
                    self.pluginsDict[pluginID].pluginState == Types.STATE_GET_URL_LIST
                   )
            while (not self.completedURLs.completedQueue.empty()) or arePluginsStillSourcingData:
                # todo: get urls from common queue, put these into each relevant plugins content fetch queue
                time.sleep(self.fetchCycleTime)
                for pluginID in self.pluginsDict.keys():
                    arePluginsStillSourcingData = arePluginsStillSourcingData or (
                        self.pluginsDict[pluginID].pluginState == Types.STATE_GET_URL_LIST
                       )
            logger.info('Consumer for aggregator sourced common URLs queue Worker Thread: Finished consuming the common URLs.')
        except Exception as e:
            logger.error("When trying to save common URLs queue, the exception was: %s", e)

##########


class histDBWorker(threading.Thread):
    """ Worker object to save completed URL to db asynchronously
    """
    workerID = -1

    def __init__(self, pluginsList, workCompletedURLs, fetchCycleTime,
                 daemon=None, target=None, name=None):
        """ Initialize the worker object
        """
        self.workerID = name
        self.pluginsDict = pluginsList
        self.completedURLs = workCompletedURLs
        # fetchCycleTime is an estimate of the time in seconds a thread takes to retrieve a url:
        self.fetchCycleTime = max(120, fetchCycleTime)
        logger.debug("Hist database recorder worker %s initialized with the plugins: %s",
                     self.workerID,
                     self.pluginsDict)
        # call base class:
        super().__init__(daemon=daemon, target=target, name=name)

    def run(self):
        """ Main method run on thread when it is started
        """
        isPluginStillFetchingData = False
        totalCountWrittenToDB = 0
        logger.info('HistDB Worker Thread: Started now.')
        try:
            for pluginID in self.pluginsDict.keys():
                isPluginStillFetchingData = isPluginStillFetchingData or (
                    self.pluginsDict[pluginID].pluginState == Types.STATE_FETCH_CONTENT
                   ) or (
                    self.pluginsDict[pluginID].pluginState == Types.STATE_GET_URL_LIST
                   )

            while (not self.completedURLs.completedQueue.empty()) or isPluginStillFetchingData:
                countOfURLsWrittenToDB = self.completedURLs.writeQueueToDB()
                totalCountWrittenToDB = totalCountWrittenToDB + countOfURLsWrittenToDB
                if self.completedURLs.completedQueue.empty():
                    logger.debug('HistDB Worker Thread: %s URLs saved to history, pausing %s seconds...',
                                 self.fetchCycleTime,
                                 countOfURLsWrittenToDB)
                    time.sleep(self.fetchCycleTime)
                    isPluginStillFetchingData = False

                for pluginID in self.pluginsDict.keys():
                    isPluginStillFetchingData = isPluginStillFetchingData or (
                        self.pluginsDict[pluginID].pluginState == Types.STATE_FETCH_CONTENT
                       ) or (
                        self.pluginsDict[pluginID].pluginState == Types.STATE_GET_URL_LIST
                       )
                    logger.debug('HistDB Worker Thread: Is plugin %s still fetching data? %s',
                                 type(self.pluginsDict[pluginID]).__name__,
                                 (self.pluginsDict[pluginID].pluginState == Types.STATE_FETCH_CONTENT))
            logger.info('HistDB Worker Thread: Finished saving the list of %s URLs in history database.',
                        totalCountWrittenToDB)
        except Exception as e:
            logger.error("When trying to save history data, the exception was: %s", e)


# # end of file ##
