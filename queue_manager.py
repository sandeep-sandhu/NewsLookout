#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: queue_manager.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-06-01
 Purpose: Manage worker threads and the job queues of all the scraper plugins for the application
 Copyright 2021, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu, sandeep.sandhu@gmx.com

 Provides:
    queueManager
        config
        initPlugins
        initURLSourcingWorkers
        initContentFetchWorkers
        initDataProcWorkers
        runAllJobs
        startAllModules
        finishAllTasks
        processDataSynchronously


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


# import standard python libraries:
import logging
from datetime import datetime
import time
import multiprocessing
import threading
import queue

# import this project's python libraries:
from data_structs import Types, URLListHelper
from worker import worker, histDBWorker, aggQueueConsumer
from network import NetworkFetcher
from scraper_utils import loadPlugins

# #
# setup logging
logger = logging.getLogger(__name__)


class queueManager:
    """ The Queue manager class runs the main processing of the application
    It launches and manages worker threads to launch the different web scraping processes,
    and saves all results from these threads.
    """

    def __init__(self):

        self.runDate = datetime.now()

        self.available_cores = 1
        self.configData = dict()

        # dict object with contents:
        # { "plugin1name": <plugin1 class instance>,  "plugin2name": <plugin2 class instance>}
        self.mods = dict()

        # dict object with contents:
        # { 1: <worker1 class instance>, 2: <worker2 class instance> }
        self.urlSrcWorkers = dict()
        self.contentFetchWorkers = dict()
        self.dataProcessWorkers = dict()

        # dict object with contents:
        # { "plugin1name": [queue of URLS for plugin1] }
        self.URL_frontier = dict()
        self.newsAggQueue = queue.PriorityQueue()
        self.aggQueueWorker = None

    def config(self, configData):
        """ Read and apply the configuration data passed by the main application
        """
        self.configData = configData
        self.allowedDomainsList = []
        self.histDBWorker = None
        self.fetchCycleTime = 120
        try:
            logger.debug("Configuring the queue manager")
            self.available_cores = multiprocessing.cpu_count()
            self.runDate = configData['rundate']
            self.fetchCycleTime = max(60, (int(self.configData['retry_wait_rand_max_sec']) +
                                      int(self.configData['retry_wait_sec']) +
                                      int(self.configData['connect_timeout']) +
                                      int(self.configData['fetch_timeout'])
                                      ))
        except Exception as e:
            logger.error("Exception when configuring the queue manager: %s", e)
        # Initialize object that reads and writes completed URLs saved in file
        self.dbAccessSemaphore = threading.Semaphore()
        self.workCompletedURLs = URLListHelper(
            self.configData['completed_urls_datafile'],
            self.dbAccessSemaphore
            )
        self.workCompletedURLs.printDBStats()
        # load and initialize all the plugins after everything has been configured.
        self.initPlugins()

    def initPlugins(self):
        """ Load, configure and initialize all plugins
        """
        self.mods = loadPlugins(self.configData)  # load the plugins
        # initialize the plugins:
        for keyitem in self.mods.keys():
            logger.info("Starting web scraping plugin: %s", keyitem)
            self.mods[keyitem].config(self.configData)
            if self.mods[keyitem].pluginType not in [Types.MODULE_NEWS_AGGREGATOR, Types.MODULE_DATA_PROCESSOR]:
                self.mods[keyitem].setNetworkHelper()
                self.URL_frontier[keyitem] = queue.PriorityQueue()
                self.mods[keyitem].setURLQueue(self.URL_frontier[keyitem])
                self.allowedDomainsList = self.allowedDomainsList + self.mods[keyitem].allowedDomains
            elif self.mods[keyitem].pluginType == Types.MODULE_NEWS_AGGREGATOR:
                self.mods[keyitem].setNetworkHelper()
                self.mods[keyitem].setURLQueue(self.newsAggQueue)
            elif self.mods[keyitem].pluginType == Types.MODULE_DATA_PROCESSOR:
                self.mods[keyitem].setConfig(self.configData)
        self.networkHelper = NetworkFetcher(self.configData, self.allowedDomainsList)
        self.workerThreads = len(self.mods.keys())
        # intialize the history DB worker:
        self.histDBWorker = histDBWorker(self.mods,
                                         self.workCompletedURLs,
                                         self.fetchCycleTime,
                                         name=0,
                                         daemon=False)
        # intialize the common queue consumer worker:
        self.aggQueueWorker = aggQueueConsumer(self.mods,
                                               self.workCompletedURLs,
                                               self.fetchCycleTime,
                                               name=0,
                                               daemon=False)

    def initURLSourcingWorkers(self):
        """ Initialize all worker threads to identify URLs
        """
        logger.debug("Initializing the worker threads to identify URLs.")
        workerNumber = 0
        for keyitem in self.mods.keys():

            if self.mods[keyitem].pluginType in [Types.MODULE_NEWS_CONTENT,
                                                 Types.MODULE_NEWS_API,
                                                 Types.MODULE_DATA_CONTENT,
                                                 Types.MODULE_NEWS_AGGREGATOR]:

                workerNumber = workerNumber + 1

                self.urlSrcWorkers[workerNumber] = worker(
                                                          self.mods[keyitem],
                                                          Types.TASK_GET_URL_LIST,
                                                          self.workCompletedURLs,
                                                          name=workerNumber,
                                                          daemon=False)

                self.urlSrcWorkers[workerNumber].setRunDate(self.runDate)
                # after this, the self.urlSrcWorkers dict has the structure: workers[1] = <instantiated worker object>
        logger.info("%s worker threads available to identify URLs to scrape.", len(self.urlSrcWorkers))

    def initContentFetchWorkers(self):
        """ Initialize all worker threads
        """
        logger.debug("Initializing the content fetching worker threads.")
        workerNumber = 0
        for keyitem in self.mods.keys():
            if self.mods[keyitem].pluginType in [Types.MODULE_NEWS_CONTENT,
                                                 Types.MODULE_NEWS_API,
                                                 Types.MODULE_DATA_CONTENT]:
                workerNumber = workerNumber + 1
                self.contentFetchWorkers[workerNumber] = worker(self.mods[keyitem],
                                                                Types.TASK_GET_DATA,
                                                                self.workCompletedURLs,
                                                                # make unique worker names
                                                                name=str(workerNumber + len(self.urlSrcWorkers)),
                                                                daemon=False)
                self.contentFetchWorkers[workerNumber].setRunDate(self.runDate)
                # after this, the self.contentFetchWorkers dict has the structure: workers[1] = <instantiated worker object>
        logger.info("%s worker threads available to fetch content.", len(self.contentFetchWorkers))

    def initDataProcWorkers(self):
        """ Initialize all worker threads
        """
        logger.debug("Initializing the data processing worker threads.")
        workerNumber = 0
        for keyitem in self.mods.keys():
            if self.mods[keyitem].pluginType in [Types.MODULE_DATA_PROCESSOR]:
                workerNumber = workerNumber + 1
                self.dataProcessWorkers[workerNumber] = worker(self.mods[keyitem],
                                                               Types.TASK_PROCESS_DATA,
                                                               self.workCompletedURLs,
                                                               # make unique worker names
                                                               name=str(workerNumber + len(self.urlSrcWorkers)),
                                                               daemon=False)
                self.dataProcessWorkers[workerNumber].setRunDate(self.runDate)
                # after this, the self.dataProcessWorkers dict has the structure: workers[1] = <instantiated worker object>
        logger.info("%s worker threads available to process data.", len(self.dataProcessWorkers))

    def runAllJobs(self):
        """ Process Queue to run all web source (URL) identification jobs
        """
        # To begin with, initialize the URL identifying workers
        self.initURLSourcingWorkers()
        # Next, initialize the URL content-fetching workers
        self.initContentFetchWorkers()
        self.initDataProcWorkers()
        # start all sourcing workers and fetching workers for each URL in URL frontier:
        self.startAllModules()

    def startAllModules(self):
        """ Start All Modules
        """
        try:
            logger.debug("Starting all worker threads.")
            # loop through urlSrcWorkers, and start their threads:
            for keyitem in self.urlSrcWorkers.keys():
                self.urlSrcWorkers[keyitem].start()

            logger.debug("Waiting for %s seconds to start content fetching worker threads.", self.fetchCycleTime)
            time.sleep(self.fetchCycleTime)

            # loop through workers, and start their threads:
            for keyitem in self.contentFetchWorkers.keys():
                self.contentFetchWorkers[keyitem].start()

            # start worker thread that saves history.
            self.histDBWorker.start()

            # start worker to pick up items from common queue and put these into each plugins queue:
            logger.debug("Waiting for %s seconds to start worker to pick up items from common queue", self.fetchCycleTime)
            time.sleep(self.fetchCycleTime)
            # self.aggQueueWorker.start()

            # wait for urlSrcWorkers to finish
            for keyitem in self.urlSrcWorkers.keys():
                self.urlSrcWorkers[keyitem].join()

            # wait for all threads to finish
            for keyitem in self.contentFetchWorkers.keys():
                self.contentFetchWorkers[keyitem].join()
                logger.debug("Completed fetching content for plugin: %s", self.contentFetchWorkers[keyitem].pluginName)

            logger.info('Completed fetching data on all worker threads')

            # TODO: not running the data processing plugin in parallel due to serial nature of the job:
            # for keyitem in self.dataProcessWorkers.keys():
            #     self.dataProcessWorkers[keyitem].start()
            # Instead, synchronously perform any data processing required on fetched data:
            self.processDataSynchronously(self.runDate)

        except KeyboardInterrupt:
            print("Recognized keyboard interrupt, stopping the program now...")

    def finishAllTasks(self):
        """ Finish All Tasks
        """
        # wait for history db worker thread to finish
        self.histDBWorker.join()
        logger.debug("Worker thread that saves history finished running now.")
        # save complete list of URLs retrieved
        self.workCompletedURLs.writeQueueToDB()
        self.workCompletedURLs.printDBStats()

    def processDataSynchronously(self, forDate):
        """ Process Data on Workers:
        loop through data plugins and execute these in serial order
        """
        # fetch and  read data that was processed by each plugin:
        for pluginName in self.mods.keys():
            thisPlugin = self.mods[pluginName]
            if thisPlugin.pluginType == Types.MODULE_DATA_PROCESSOR:
                thisPlugin.processData(forDate)
                # in the end mark completion by changing state:
                thisPlugin.pluginState = Types.STATE_STOPPED
                logger.debug('Completed processing data from plugin: %s', pluginName)


# # end of file ##
