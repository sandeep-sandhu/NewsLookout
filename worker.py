#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: worker.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-06-01
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

    aggQueueConsumer
        run

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
import copy
from tqdm import tqdm

# import this project's modules
from data_structs import Types

# #########

logger = logging.getLogger(__name__)

# #########


class worker(threading.Thread):
    """ This worker object runs fetching and data extraction processes within each thread.
    """
    workerID = -1
    taskType = None
    runDate = datetime.now()
    queueFillwaitTime = 1

    def __init__(self,
                 pluginObj,
                 taskType,
                 workCompletedURLs,
                 daemon=None, target=None, name=None):
        """ Initialize the worker with plugin of type: base_plugin.basePlugin,
         taskType of type: data_structs.Types.<task type>,
         and workCompletedURLs which is the URL completed tracker object of type: data_structs.URLListHelper
        """
        self.workerID = name
        self.pluginObj = pluginObj
        self.pluginName = type(self.pluginObj).__name__
        self.taskType = taskType
        self.completedURLs = workCompletedURLs
        self.queueFillwaitTime = 120
        logger.debug("Worker %s initialized with the plugin: %s", self.workerID, self.pluginObj)
        super().__init__(daemon=daemon, target=target, name=name)

    def setRunDate(self, runDate):
        self.runDate = runDate

    def runURLListGatherTasks(self):
        """ Run Tasks to gather the listing of URLs
        """
        pluginName = type(self.pluginObj).__name__
        try:
            logger.info("Started collecting URLs for plugin: %s", pluginName)
            # fetch URL list using each plugin's function:
            self.pluginObj.getURLsListForDate(self.runDate, self.completedURLs)
            logger.debug('Thread %s finished getting URL listing for plugin %s',
                         self.workerID,
                         pluginName)
        except Exception as e:
            logger.error(
                "When trying to get URL listing using plugin: %s, Type TASK_GET_URL_LIST, Exception: %s",
                pluginName,
                e)

    def runDataRetrievalTasks(self):
        """ Run Data Retrieval Tasks
        """
        logger.info("Started data retrieval job for plugin: %s, queue size: %s",
                    self.pluginName,
                    self.pluginObj.urlQueue.qsize()
                    )
        while (not self.pluginObj.urlQueue.empty()) or (self.pluginObj.pluginState == Types.STATE_GET_URL_LIST):
            retrievedItem = None
            try:
                if self.pluginObj.urlQueue.empty():
                    logger.debug('%s: Waiting %s seconds for input queue to fill up; plugin state = %s',
                                 self.pluginName,
                                 self.queueFillwaitTime,
                                 self.pluginObj.pluginState
                                 )
                    time.sleep(self.queueFillwaitTime)
                retrievedItem = self.pluginObj.urlQueue.get(timeout=self.queueFillwaitTime)
                self.pluginObj.urlQueue.task_done()
                sURL = None
                if retrievedItem is not None:
                    (priority, sURL) = retrievedItem
                fetchResult = None
                if sURL is not None:
                    fetchResult = self.pluginObj.fetchDataFromURL(sURL, self.workerID)
                    if fetchResult is not None and fetchResult.textSize is not None and fetchResult.textSize > 3:
                        self.completedURLs.completedQueue.put(fetchResult)
                    else:
                        # add url to failed table:
                        self.completedURLs.addURLToFailedTable(sURL, self.pluginName, datetime.now())
            except queue.Empty as qempty:
                logger.debug("%s: Queue was empty when trying to retrieve data: %s",
                             self.pluginName,
                             qempty
                             )
            except Exception as e:
                logger.error("%s: When trying to retrieve data the exception was: %s, retrievedItem = %s",
                             self.pluginName,
                             e,
                             retrievedItem
                             )
        logger.debug('Thread %s finished tasks to retrieve data for plugin %s',
                     self.workerID,
                     self.pluginName)
        # at this point, since all data has been fetched, change the plugin's state to process data:
        self.pluginObj.pluginState = Types.STATE_STOPPED

    def runDataProcessingTasks(self):
        """ Run Data Processing Tasks
        """
        try:
            logger.debug('Thread %s given task to process data using plugin %s', self.workerID, self.pluginName)
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
        """ Overridden to enable parent thread object to be called for executing the Plugin jobs
        """
        if self.taskType == Types.TASK_GET_URL_LIST:
            self.runURLListGatherTasks()
        elif self.taskType == Types.TASK_GET_DATA:
            self.runDataRetrievalTasks()
        elif self.taskType == Types.TASK_PROCESS_DATA:
            self.runDataProcessingTasks()


##########


class aggQueueConsumer(threading.Thread):
    """ Worker object that asynchronously consumes aggregator sourced common URLs queue
    """
    workerID = -1

    def __init__(self, pluginsList, commonURLsQueue, queueFetchWaitTime,
                 daemon=None, target=None, name=None):
        """ Initialize the worker object
        """
        self.workerID = name
        self.pluginsDict = pluginsList
        self.commonURLsQueue = commonURLsQueue
        self.fetchCycleTime = queueFetchWaitTime
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
                # TODO: get urls from common queue, put these into each relevant plugins content fetch queue
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
    """ Worker object to save completed URL to history database asynchronously
    """
    workerID = -1
    totalURLQueueSize = 0
    previousState = dict()
    currentState = dict()

    def __init__(self, pluginsList, workCompletedURLs, refreshProgressWaitTime,
                 daemon=None, target=None, name=None):
        """ Initialize the worker object
        """
        self.workerID = name
        self.pluginsDict = pluginsList
        self.completedURLs = workCompletedURLs
        # refreshProgressWaitTime is an estimate of the time in seconds a thread takes to retrieve a url:
        self.refreshProgressWaitTime = refreshProgressWaitTime
        self.previousState = dict()
        self.currentState = dict()
        logger.debug("History database recorder thread %s initialized with the plugins: %s",
                     self.workerID,
                     self.pluginsDict)
        # call base class:
        super().__init__(daemon=daemon, target=target, name=name)

    def getStatusChange(self):
        """ Get Status Change
        """
        statusMessages = []
        # current status is: self.currentState, previous status is: self.previousState
        for pluginName in self.currentState.keys():
            try:
                # for each key in current status, check and compare value in previous state
                currentState = self.currentState[pluginName]
                if len(self.previousState) > 0 and currentState != self.previousState[pluginName]:
                    # print For plugin z, status changed from x to y
                    statusMessages.append(pluginName +
                                          ' changed state to -> ' +
                                          currentState.replace('STATE_', '').replace('_', ' '))
            except Exception as e:
                logger.debug("Error comparing previous state of plugin: %s", e)
        return(statusMessages)

    def run(self):
        """ Main method that runs this history database thread when it is started
        """
        isPluginStillFetchingData = False
        totalCountWrittenToDB = 0
        totalTimeElapsed = self.refreshProgressWaitTime
        completedURLCount = 0
        logger.debug('History database thread started now.')
        try:
            for pluginID in self.pluginsDict.keys():
                isPluginStillFetchingData = isPluginStillFetchingData or (
                    self.pluginsDict[pluginID].pluginState == Types.STATE_FETCH_CONTENT
                   ) or (
                    self.pluginsDict[pluginID].pluginState == Types.STATE_GET_URL_LIST
                   )
                self.totalURLQueueSize = self.totalURLQueueSize + self.pluginsDict[pluginID].urlQueueTotalSize
            print("Web-scraping Progress:")
            while (not self.completedURLs.completedQueue.empty()) or isPluginStillFetchingData is True:
                countOfURLsWrittenToDB = self.completedURLs.writeQueueToDB()
                # keep a total count, this will be reported before closing the application
                totalCountWrittenToDB = totalCountWrittenToDB + countOfURLsWrittenToDB
                # wait for some time before checking again:
                time.sleep(self.refreshProgressWaitTime)
                totalTimeElapsed = totalTimeElapsed + self.refreshProgressWaitTime
                # log progress only if some data was fetched while waiting:
                if countOfURLsWrittenToDB > 0:
                    logger.info('Progress Status: %s URLs successfully scraped out of %s processed; %s URLs remain.',
                                totalCountWrittenToDB,
                                completedURLCount,
                                (self.totalURLQueueSize - completedURLCount)
                                )
                # reset flag before checking all plugins again within loop:
                isPluginStillFetchingData = False
                completedURLCount = 0
                self.totalURLQueueSize = 0
                # save previous state, use deepcopy
                self.previousState = copy.deepcopy(self.currentState)
                # re-initialize the variable to capture current state:
                self.currentState = dict()
                for pluginID in self.pluginsDict.keys():
                    self.currentState[type(self.pluginsDict[pluginID]).__name__] = Types.decodeNameFromIntVal(
                        self.pluginsDict[pluginID].pluginState)
                    # flag 'isPluginStillFetchingData' as True if any plugin is still fetching data,
                    # or, if it is still retrieving URL listing
                    isPluginStillFetchingData = isPluginStillFetchingData or (
                        self.pluginsDict[pluginID].pluginState == Types.STATE_FETCH_CONTENT
                       ) or (
                        self.pluginsDict[pluginID].pluginState == Types.STATE_GET_URL_LIST
                       )
                    # re-calculate totals since other threads may have increased list of urls to be fetched
                    completedURLCount = completedURLCount + self.pluginsDict[pluginID].urlProcessedCount
                    self.totalURLQueueSize = self.totalURLQueueSize + self.pluginsDict[pluginID].urlQueueTotalSize
                # after loop, prepare status-bar text, then print it out
                statusBarText = tqdm.format_meter(completedURLCount,
                                                  self.totalURLQueueSize,
                                                  totalTimeElapsed,
                                                  ncols=80,
                                                  ascii=False)
                print(statusBarText, '\b' * 100, end='')
                for statusMessage in self.getStatusChange():
                    logger.info(statusMessage)
            print(tqdm.format_meter(self.totalURLQueueSize, self.totalURLQueueSize, totalTimeElapsed, ncols=80, ascii=False))
            logger.info('History Database Thread: Finished saving a total of %s URLs in history database.',
                        totalCountWrittenToDB)
        except Exception as e:
            logger.error("When trying to save history data, the exception was: %s", e)


# # end of file ##
