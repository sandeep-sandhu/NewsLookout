#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: queue_manager.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-01-14
 Purpose: Manage worker threads and the job queues of all the scraper plugins for the application
 Copyright 2021, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu, sandeep.sandhu@gmx.com

 Provides:
    queueManager
        config
        initPlugins
        initURLSourcingWorkers
        initContentFetchWorkers
        runAllJobs
        startAllModules
        finishAllTasks
        runDataProcessingJobs
        processDataOnWorkers

 DISCLAIMER: This software is intended for demonstration and educational purposes only.
 Before using it for web scraping any website, always consult that website's terms of use.
 Do not use this software to fetch any data from any website that has forbidden use of web
 scraping or similar mechanisms, or violates its terms of use in any other way. The author is
 not responsible for such kind of inappropriate use of this software.

"""

# #

# import standard python libraries:
import logging
from datetime import datetime
import time
import multiprocessing
import queue
import requests

# import this project's python libraries:
from data_structs import Types, URLListHelper
from worker import worker, histDBWorker
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

        # dict object with contents:
        # { "plugin1name": [queue of URLS for plugin1] }
        self.URL_frontier = dict()

    def config(self, configData):
        """ Read and apply the configuration data passed by the main application """

        self.configData = configData

        self.networkHelper = NetworkFetcher(configData)
        self.histDBWorker = None
        self.fetchCycleTime = 60

        try:
            logger.debug("Configuring the queue manager")

            self.available_cores = multiprocessing.cpu_count()

            self.runDate = configData['rundate']

            self.fetchCycleTime = (int(self.configData['retry_wait_rand_max_sec'])
                                   + int(self.configData['retry_wait_sec'])
                                   + int(self.configData['connect_timeout'])
                                   + int(self.configData['fetch_timeout'])
                                   ) * int(self.configData['retry_count'])

        except Exception as e:
            logger.error("Exception when configuring the queue manager: %s", e)

        # Initialize object that reads and writes completed URLs saved in file
        self.workCompletedURLs = URLListHelper(
            self.configData['completed_urls_datafile']
            )
        self.workCompletedURLs.printDBStats()

        # load and initialize all the plugins after everything has been configured.
        self.initPlugins()

    def initPlugins(self):
        """ Load, configure and initialize all plugins """
        # load the plugins
        self.mods = loadPlugins(self.configData)

        allowedDomainsList = []

        # initialize the plugins:
        for keyitem in self.mods.keys():
            logger.info("Starting web scraping plugin: %s", keyitem)

            self.mods[keyitem].config(self.configData)
            self.mods[keyitem].setNetworkHelper(self.networkHelper)

            self.URL_frontier[keyitem] = queue.PriorityQueue()

            self.mods[keyitem].setURLQueue(self.URL_frontier[keyitem])

            allowedDomainsList = allowedDomainsList + self.mods[keyitem].allowedDomains

        self.workerThreads = len(self.mods.keys())

        # intialize the history DB worker:
        self.histDBWorker = histDBWorker(self.mods,
                                         self.workCompletedURLs,
                                         self.fetchCycleTime,
                                         name=0,
                                         daemon=False)

        cookiePolicy = self.networkHelper.getCookiePolicy(allowedDomainsList)
        self.cookieJar = requests.cookies.RequestsCookieJar(policy=cookiePolicy)
        self.configData['cookieJar'] = self.networkHelper.loadAndSetCookies(self.configData['cookie_file'])

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

        logger.info("%s worker threads available to identify URLs to source.", len(self.urlSrcWorkers))
        if len(self.urlSrcWorkers) != self.workerThreads:
            logger.error("Could not initialize required no of identify URL worker threads.")

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
        if len(self.contentFetchWorkers) != self.workerThreads:
            logger.error("Could not initialize required no of content fetching worker threads.")

    def runAllJobs(self):
        """ Process Queue to run all web source (URL) identification jobs
        """
        # To begin with, initialise the URL sourcing workers
        self.initURLSourcingWorkers()

        # Next, initialise the URL content fetching workers
        self.initContentFetchWorkers()

        self.startAllModules()
        # start sourcing workers and fetching workers for each URL in URL frontier

    def startAllModules(self):
        """ Start All Modules
        """
        logger.debug("Starting all worker threads.")

        # loop through urlSrcWorkers, and start their threads:
        for keyitem in self.urlSrcWorkers.keys():
            self.urlSrcWorkers[keyitem].start()

        logger.debug("Waiting for %s seconds to start content fetching worker threads.", min(self.fetchCycleTime, 60))
        time.sleep(min(self.fetchCycleTime, 60))

        # loop through workers, and start their threads:
        for keyitem in self.contentFetchWorkers.keys():
            self.contentFetchWorkers[keyitem].start()

        logger.debug("Waiting for %s seconds to start worker thread that saves history.", min(self.fetchCycleTime, 120))
        time.sleep(min(self.fetchCycleTime, 120))
        # start saving URLs to history
        self.histDBWorker.start()

        # wait for urlSrcWorkers to finish
        for keyitem in self.urlSrcWorkers.keys():
            self.urlSrcWorkers[keyitem].join()

        # wait for all of them to finish
        for keyitem in self.contentFetchWorkers.keys():
            self.contentFetchWorkers[keyitem].join()
            logger.debug("Completed fetching content for plugin: %s", self.contentFetchWorkers[keyitem].pluginName)

        logger.info('Completed fetching data on all worker threads')

    def finishAllTasks(self):
        """ Finish All Tasks
        """
        # wait for history db worker thread to finish
        self.histDBWorker.join()
        logger.debug("Worker thread that saves history finished running now.")

        # save complete list of URLs retrieved
        self.workCompletedURLs.writeQueueToDB()
        self.workCompletedURLs.printDBStats()

    def runDataProcessingJobs(self):
        """ Process Queue to run Data Processing Jobs
        """
        # not running the data processing plugin in parallel due to heavy nature of the job
        # self.initDataProcWorkers()

        # perform any data processing required on fetched data:
        self.processDataSynchronously()

    def processDataSynchronously(self):
        """ Process Data on Workers:
        loop through data plugins and execute these in serial order """

        # fetch and  read data that was processed by each plugin:
        for pluginName in self.mods.keys():

            thisPlugin = self.mods[pluginName]

            if thisPlugin.pluginType == Types.MODULE_DATA_PROCESSOR:

                thisPlugin.processData()

                logger.debug('Collecting processed data from plugin: %s', pluginName)

# # end of file ##
