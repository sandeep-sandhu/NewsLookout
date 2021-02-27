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

 DISCLAIMER: This software is intended for demonstration and educational purposes only.
 Before using it for web scraping any website, always consult that website's terms of use.
 Do not use this software to fetch any data from any website that has forbidden use of web
 scraping or similar mechanisms, or violates its terms of use in any other way. The author is
 not responsible for such kind of inappropriate use of this software.

"""

##########

# import standard python libraries:
import logging
import threading
# import queue
import random
import time
from datetime import datetime

# import this project's python libraries:
from data_structs import Types

##########

logger = logging.getLogger(__name__)

##########


class worker(threading.Thread):
    """ Worker object to process fetching and data extraction in each thread. """

    workerID = -1
    taskType = Types.TASK_GET_URL_LIST
    runDate = datetime.now()

    def __init__(self, pluginObj, taskType, workCompletedURLs,
                 daemon=None, target=None, name=None):
        """ Initialize the worker object """

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
        """ Run Tasks to gather the listing of URLs """
        pluginName = type(self.pluginObj).__name__

        try:
            logger.debug("Started URL gathering for plugin: %s", pluginName)

            # fetch URL list using each plugin's function:
            self.pluginObj.getURLsListForDate(self.runDate)

            # Remove URLs already retrieved earlier: removeAlreadyFetchedURLs(self, sqlCon, newURLsList, pluginName)
            self.pluginObj.listOfURLS = self.completedURLs.removeAlreadyFetchedURLs(
                    self.pluginObj.listOfURLS,
                    pluginName)

            priority_number = 0
            for listItem in self.pluginObj.listOfURLS:

                # logger.debug("%s: Adding URL into queue: %s", pluginName, listItem.encode('ascii', 'ignore'))

                self.pluginObj.urlQueue.put(
                        (priority_number, listItem)
                   )
                priority_number = priority_number + 1

            # in the end add sentinel to indicate end of queue for this plugin:
            self.pluginObj.urlQueue.put(
                        (priority_number, None)
                   )

            # fetch links from content of the URL list !!!!!
            # additionalLinks = self.extractLinksFromURLList(self.pluginObj.listOfURLS)

            # save list to temp file !!!!
            # saveObjToJSON(self.pluginName + '_additionsl_list_of_URLS.json', additionalLinks)

            logger.debug("%s: Total count of valid articles to be retrieved = %s, queue size = %s",
                         pluginName,
                         len(self.pluginObj.listOfURLS),
                         self.pluginObj.urlQueue.qsize())

            logger.debug('Thread %s finished getting URL listing for plugin %s',
                         self.workerID,
                         pluginName)

            self.pluginObj.pluginState = Types.STATE_FETCH_CONTENT
            # empty out the url list in the plugin:
            self.pluginObj.listOfURLS = []

        except Exception as e:
            logger.error(
                "When trying to get URL listing using plugin: %s, Type TASK_GET_URL_LIST: %s, Exception: %s",
                pluginName,
                self.taskType,
                e)

    def runDataRetrievalTasks(self):
        """ run Data Retrieval Tasks
        """
        try:
            logger.info("Started data retrieval job for plugin: %s, queue size: %s",
                        self.pluginName,
                        self.pluginObj.urlQueue.qsize()
                        )
            while (not self.pluginObj.urlQueue.empty()) or (self.pluginObj.pluginState == Types.STATE_GET_URL_LIST):

                sURL = None
                retrievedItem = self.pluginObj.urlQueue.get(timeout=20)

                if retrievedItem is not None:
                    (priority, sURL) = retrievedItem
                    self.pluginObj.urlQueue.task_done()

                # logger.debug('Input queue empty? %s, Input queue size: %s, Completed queue size: %s, URL: %s'
                #    , self.pluginName, self.pluginObj.urlQueue.empty()
                #    , self.pluginObj.urlQueue.qsize(), self.completedURLs.completedQueue.qsize(), sURL)

                if sURL is not None:
                    fetchMetrics = self.pluginObj.fetchDataFromURL(sURL, self.workerID)

                    if fetchMetrics is not None:
                        (uRL, len_raw_data, len_text, publish_date) = fetchMetrics

                        if len_text is not None and len_text > 3:
                            self.completedURLs.completedQueue.put((uRL, len_raw_data, len_text, publish_date, self.pluginName))

                if self.pluginObj.urlQueue.empty():
                    logger.debug('%s: Waiting for input queue to fill up...', self.pluginName)
                    time.sleep(random.randint(1, 5))

            logger.debug('Thread %s finished tasks to retrieve data for plugin %s',
                         self.workerID,
                         self.pluginName)

        except Exception as e:
            logger.error("%s: When trying to retrieve data the exception was: %s",
                         self.pluginName,
                         e)

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
        """ Overridden to enable thread to be called for executing the Plugin jobs """

        if self.taskType == Types.TASK_GET_URL_LIST:
            self.runURLListGatherTasks()

        elif self.taskType == Types.TASK_GET_DATA:
            self.runDataRetrievalTasks()

        elif self.taskType == Types.TASK_PROCESS_DATA:
            self.runDataProcessingTasks()

##########


class histDBWorker(threading.Thread):
    """ Worker object to save completed URL to db asynchronously """

    workerID = -1

    def __init__(self, pluginsList, workCompletedURLs, fetchCycleTime,
                 daemon=None, target=None, name=None):
        """ Initialize the worker object
        """
        self.workerID = name
        self.pluginsDict = pluginsList
        self.completedURLs = workCompletedURLs
        self.fetchCycleTime = max(120, fetchCycleTime)

        logger.debug("Hist database recorder worker %s initialized with the plugins: %s",
                     self.workerID,
                     self.pluginsDict)

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
