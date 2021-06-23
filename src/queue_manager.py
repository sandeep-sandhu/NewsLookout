#!/usr/bin/env python
# -*- coding: utf-8 -*-

# #########################################################################################################
# File name: queue_manager.py                                                                             #
# Application: The NewsLookout Web Scraping Application                                                   #
# Date: 2021-06-23                                                                                        #
# Purpose: Manage worker threads and the job queues of all the scraper plugins for the application        #
# Copyright 2021, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu, sandeep.sandhu@gmx.com  #
#                                                                                                         #
# Provides:                                                                                               #
#    QueueManager                                                                                         #
#        config                                                                                           #
#        initPlugins                                                                                      #
#        initURLSourcingWorkers                                                                           #
#        initContentFetchWorkers                                                                          #
#        initDataProcWorkers                                                                              #
#        runAllJobs                                                                                       #
#        startAllModules                                                                                  #
#        finishAllTasks                                                                                   #
#        processDataSynchronously                                                                         #
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
import importlib
import logging
import os
import sys
from datetime import datetime
import multiprocessing
import threading
import queue

# import this project's python libraries:
from data_structs import Types
from session_hist import SessionHistory
from worker import worker
from worker import ProgressWatcher
from worker import DataProcessor

# #
# setup logging
logger = logging.getLogger(__name__)


class QueueManager:
    """ The Queue manager class runs the main processing of the application
    It launches and manages worker threads to launch the different web scraping processes,
    and saves all results from these threads.
    """
    app_config = None
    runDate = datetime.now()
    available_cores = 1
    fetchCycleTime = 120
    fetchCompletedCount = 0
    totalPluginsURLSrcCount = 0

    # Map object with the following structure:
    # { "plugin1name": <plugin1 class instance>,  "plugin2name": <plugin2 class instance>}
    pluginNameToObjMap = dict()
    dataProcPluginsMap = {}

    allowedDomainsList = []
    # dict with contents: { domainName: pluginName }
    domainToPluginMap = {}

    # dict object with contents:
    # { 1: <worker1 class instance>, 2: <worker2 class instance> }
    urlSrcWorkers = dict()
    contentFetchWorkers = dict()
    dataProcessWorkerList = []
    dataproc_threads = 5
    singleDataProcessor = None

    progressWatchThread = None
    dbAccessSemaphore = None
    sessionHistoryDB = None

    # all the queues:
    fetchCompletedQueue = None
    dataProcQueue = None
    dataProcCompletedQueue = None
    alreadyDataProcList = []

    # Map object with the following structure:
    # { "plugin1name": [queue of URLS for plugin1] }
    URL_frontier = dict()

    def __init__(self):
        self.fetchCompletedQueue = queue.Queue()
        self.dataProcQueue = queue.Queue()
        self.dataProcCompletedQueue = queue.Queue()

    def config(self, app_config):
        """ Read and apply the configuration data passed by the main application
        """
        self.app_config = app_config
        try:
            logger.debug("Configuring the queue manager")
            self.available_cores = multiprocessing.cpu_count()
            self.runDate = self.app_config.rundate
            self.fetchCycleTime = max(60, (int(self.app_config.retry_wait_rand_max_sec) +
                                      int(self.app_config.retry_wait_sec) +
                                      int(self.app_config.connect_timeout) +
                                      int(self.app_config.fetch_timeout)
                                      ))
        except Exception as e:
            logger.error("Exception when configuring the queue manager: %s", e)
        self.dbAccessSemaphore = threading.Semaphore()
        # Initialize object that reads and writes session history of completed URLs into a database
        self.sessionHistoryDB = SessionHistory(
            self.app_config.completed_urls_datafile,
            self.dbAccessSemaphore,
            self
            )
        self.sessionHistoryDB.printDBStats()
        # self.sessionHistoryDB.setDataProcInputQueue(self.dataProcQueue)
        # self.sessionHistoryDB.setDataProcOutputQueue(self.dataProcCompletedQueue)
        # load and initialize all the plugins after everything has been configured.
        self.initPlugins()

    def getFetchResultFromQueue(self, block=True, timeout=30):
        resultObj = self.fetchCompletedQueue.get(block=block,
                                                 timeout=timeout)
        self.fetchCompletedQueue.task_done()
        return(resultObj)

    def isFetchQEmpty(self):
        return(self.fetchCompletedQueue.empty())

    def isDataProcInputQEmpty(self):
        return(self.dataProcQueue.empty())

    def getCompletedQueueSize(self):
        return(self.dataProcQueue.qsize())

    def getDataProcessedQueueSize(self):
        return(self.dataProcCompletedQueue.qsize())

    def addToScrapeCompletedQueue(self, fetchResult):
        self.fetchCompletedCount = self.fetchCompletedCount + 1
        self.fetchCompletedQueue.put(fetchResult)
        self.dataProcQueue.put(fetchResult)

    def fetchFromDataProcInputQ(self, block=True, timeout=30):
        resultObj = self.dataProcQueue.get(block=block, timeout=timeout)
        self.dataProcQueue.task_done()
        return(resultObj)

    def addToDataProcessedQueue(self, fetchResult):
        try:
            self.dataProcCompletedQueue.put(fetchResult)
            logger.debug("Added object to completed data processing queue: %s", fetchResult.savedDataFileName)
        except Exception as e:
            logger.error("When adding item to data processing completed queue, error was: %s", e)

    def getTotalSrcPluginCount(self):
        return(self.totalPluginsURLSrcCount)

    def getQueueStatus(self):
        """
        Reads the URLFrontier, the plugin queues and the data processing queues.
        Returns a tuple with following structure:
          - Count of plugins in URL sourcing state
          - Dictionary map with plugin names as the keys and their current queue sizes as the values.
          - Dictionary map with plugin names as the keys and their total queue sizes as values.
          - Data fetch/scrape completed queue size
          - Total fetch completed size
          - Data process input queue Size
          - Data process completed queue size
          - Total count of all URLs attempted in this session
          - Flag indicating if any plugin is still fetching data over network (source URL list or content)

        :return: A tuple with the relevant contents
        :rtype: tuple
        """
        qsizeMap = {}
        totalQsizeMap = {}
        currentState = {}
        totalURLCount = 0
        countOfPluginsInURLSrcState = 0
        isPluginStillFetchingoverNetwork = False
        for pluginName in self.URL_frontier.keys():
            qsizeMap.update({pluginName: self.URL_frontier[pluginName].qsize()})
            totalQsizeMap[pluginName] = self.pluginNameToObjMap[pluginName].urlQueueTotalSize
            totalURLCount = totalURLCount + self.pluginNameToObjMap[pluginName].urlQueueTotalSize
            isPluginStillFetchingoverNetwork = isPluginStillFetchingoverNetwork or (
                self.pluginNameToObjMap[pluginName].pluginState in [Types.STATE_GET_URL_LIST, Types.STATE_FETCH_CONTENT]
                )
            currentState.update(
                {pluginName: Types.decodeNameFromIntVal(self.pluginNameToObjMap[pluginName].pluginState)}
                )
            if self.pluginNameToObjMap[pluginName].pluginState == Types.STATE_GET_URL_LIST:
                countOfPluginsInURLSrcState = countOfPluginsInURLSrcState + 1
        return((countOfPluginsInURLSrcState,
                qsizeMap,
                totalQsizeMap,
                self.fetchCompletedQueue.qsize(),
                self.fetchCompletedCount,
                self.getCompletedQueueSize(),
                self.getDataProcessedQueueSize(),
                totalURLCount,
                isPluginStillFetchingoverNetwork,
                currentState
                ))

    @staticmethod
    def loadPlugins(app_dir, plugins_dir, contrib_plugins_dir, enabledPluginNames):
        """ Load only enabled plugins from the modules in the plugins directory.
        The class names of plugins are expected to be the same as their module names.
        """
        pluginsDict = dict()
        # add paths to load python files
        sys.path.append(app_dir)
        sys.path.append(plugins_dir)
        sys.path.append(contrib_plugins_dir)
        logger.debug('Read the following plugin names from configuration file: %s', enabledPluginNames)
        modulesPackageName = os.path.basename(plugins_dir)
        for pluginFileName in importlib.resources.contents(modulesPackageName):
            # get full path:
            pluginFullPath = os.path.join(plugins_dir, pluginFileName)
            if os.path.isdir(pluginFullPath):  # skip directories
                continue
            # get only file name without file extension:
            modName = os.path.splitext(pluginFileName)[0]
            # only then load the module if module is enabled in the config file:
            if modName in enabledPluginNames:
                # The class names of plugins are the same as their module names
                className = modName
                try:
                    #logger.debug("Importing web-scraping plugin class: %s", modName)
                    classObj = getattr(importlib.import_module(modName, package=modulesPackageName), className)
                    pluginsDict[modName] = classObj()
                    pluginPriority = enabledPluginNames[modName]
                    pluginsDict[modName].executionPriority = pluginPriority
                    logger.info('Loaded plugin %s with priority = %s', modName, pluginsDict[modName].executionPriority)
                except Exception as e:
                    logger.error("While importing plugin %s got exception: %s", modName, e)
        contribPluginsDict = QueueManager.loadPluginsContrib(contrib_plugins_dir, enabledPluginNames)
        pluginsDict.update(contribPluginsDict)
        return(pluginsDict)

    @staticmethod
    def loadPluginsContrib(contrib_plugins_dir, enabledPluginNames):
        """
        load the contributed plugins for web-scraping
        """
        pluginsDict = dict()
        sys.path.append(contrib_plugins_dir)
        contribPluginsPackageName = os.path.basename(contrib_plugins_dir)
        for pluginFileName in importlib.resources.contents(contribPluginsPackageName):
            # get full path:
            pluginFullPath = os.path.join(contrib_plugins_dir, pluginFileName)
            if os.path.isdir(pluginFullPath):
                continue  # skip directories
            # extract only the file name without its file extension:
            modName = os.path.splitext(pluginFileName)[0]
            # only then load the module if module is enabled in the config file:
            if modName in enabledPluginNames:
                className = modName  # since class names of plugins are same as their module names
                try:
                    logger.debug("Importing contributed web-scraping plugin class: %s", modName)
                    classObj = getattr(importlib.import_module(modName, package=contribPluginsPackageName), className)
                    pluginsDict[modName] = classObj()
                except Exception as e:
                    logger.error("While importing contributed plugin %s got exception: %s", modName, e)
        return(pluginsDict)

    def initPlugins(self):
        """ Load, configure and initialize all plugins
        """
        # load the plugins
        self.pluginNameToObjMap = QueueManager.loadPlugins(self.app_config.install_prefix,
                                                           self.app_config.plugins_dir,
                                                           self.app_config.plugins_contributed_dir,
                                                           self.app_config.enabledPluginNames)
        # initialize the plugins:
        for keyitem in self.pluginNameToObjMap.keys():
            logger.debug("Initializing plugin: %s", keyitem)
            if self.pluginNameToObjMap[keyitem].pluginType not in [Types.MODULE_NEWS_AGGREGATOR,
                                                                   Types.MODULE_DATA_PROCESSOR]:
                self.pluginNameToObjMap[keyitem].config(self.app_config)
                self.pluginNameToObjMap[keyitem].initNetworkHelper()
                self.URL_frontier[keyitem] = queue.Queue()
                self.pluginNameToObjMap[keyitem].setURLQueue(self.URL_frontier[keyitem])
                self.allowedDomainsList = self.allowedDomainsList + self.pluginNameToObjMap[keyitem].allowedDomains
            elif self.pluginNameToObjMap[keyitem].pluginType == Types.MODULE_NEWS_AGGREGATOR:
                self.pluginNameToObjMap[keyitem].config(self.app_config)
                self.pluginNameToObjMap[keyitem].initNetworkHelper()
            elif self.pluginNameToObjMap[keyitem].pluginType == Types.MODULE_DATA_PROCESSOR:
                self.pluginNameToObjMap[keyitem].config(self.app_config)
                self.pluginNameToObjMap[keyitem].additionalConfig(self.sessionHistoryDB)
        # make map with .allowedDomains -> pluginName from plugins:
        # for keyitem in self.pluginNameToObjMap.keys():
            if self.pluginNameToObjMap[keyitem].pluginType in [Types.MODULE_NEWS_CONTENT,
                                                              Types.MODULE_NEWS_AGGREGATOR,
                                                              Types.MODULE_DATA_CONTENT,
                                                              Types.MODULE_NEWS_API]:
                self.totalPluginsURLSrcCount = self.totalPluginsURLSrcCount + 1
                modname = self.pluginNameToObjMap[keyitem].pluginName
                domains = self.pluginNameToObjMap[keyitem].allowedDomains
                for dom in domains:
                    self.domainToPluginMap[dom] = modname
        logger.info("Completed initialising %s plugins.", len(self.pluginNameToObjMap))

    def initURLSourcingWorkers(self):
        """ Initialize all worker threads that run the URL sourcing function of all news/data plugins to identify URLs
        and, also initialise the worker thread that reports progress and saves to history database.
        """
        logger.debug("Initializing the worker threads to identify URLs.")
        workerNumber = 0
        for keyitem in self.pluginNameToObjMap.keys():
            if self.pluginNameToObjMap[keyitem].pluginType in [Types.MODULE_NEWS_CONTENT,
                                                               Types.MODULE_NEWS_API,
                                                               Types.MODULE_DATA_CONTENT,
                                                               Types.MODULE_NEWS_AGGREGATOR]:
                workerNumber = workerNumber + 1
                self.urlSrcWorkers[workerNumber] = worker(
                    self.pluginNameToObjMap[keyitem],
                    Types.TASK_GET_URL_LIST,
                    self.sessionHistoryDB,
                    self,
                    name=workerNumber,
                    daemon=False)
                self.urlSrcWorkers[workerNumber].setRunDate(self.runDate)
            # for news aggregators, set extra parameters: domain name to plugin map, and all Plugins
            if self.pluginNameToObjMap[keyitem].pluginType in [Types.MODULE_NEWS_AGGREGATOR]:
                self.urlSrcWorkers[workerNumber].setDomainMapAndPlugins(self.domainToPluginMap,
                                                                        self.pluginNameToObjMap)
        # after this, the self.urlSrcWorkers dict has the structure: workers[1] = <instantiated worker object>
        logger.info("%s worker threads available to identify URLs to scrape.", len(self.urlSrcWorkers))
        # intialize the progress Watch worker thread:
        self.progressWatchThread = ProgressWatcher(self.pluginNameToObjMap,
                                                   self.sessionHistoryDB,
                                                   self,
                                                   self.app_config.progressRefreshInt,
                                                   name='1',
                                                   daemon=False)

    def initContentFetchWorkers(self):
        """ Initialize all worker threads
        """
        logger.debug("Initializing the content fetching worker threads.")
        workerNumber = 0
        for keyitem in self.pluginNameToObjMap.keys():
            if self.pluginNameToObjMap[keyitem].pluginType in [Types.MODULE_NEWS_CONTENT,
                                                               Types.MODULE_NEWS_API,
                                                               Types.MODULE_DATA_CONTENT]:
                workerNumber = workerNumber + 1
                self.contentFetchWorkers[workerNumber] = worker(self.pluginNameToObjMap[keyitem],
                                                                Types.TASK_GET_DATA,
                                                                self.sessionHistoryDB,
                                                                self,
                                                                name=str(workerNumber + len(self.urlSrcWorkers)),
                                                                daemon=False)
                self.contentFetchWorkers[workerNumber].setRunDate(self.runDate)
                # after this, the self.contentFetchWorkers dict has the structure:
                # workers[1] = <instantiated worker object>
        logger.info("%s worker threads available to fetch content.", len(self.contentFetchWorkers))

    def initDataProcWorkers(self):
        """ Initialize the single data processing worker thread
        This thread will invoke processDate() from all the data processing plugins
         one after another in order of priority.
        """
        logger.debug("Initializing the data processing worker thread with plugins.")
        self.dataProcPluginsMap = {}
        allPriorityValues = []
        try:
            # convert plupginMap to structure: priority -> pluginObj for all data processing plugins
            for keyitem in self.pluginNameToObjMap.keys():
                logger.debug('Checking data proc plugin for: %s, Type = %s',
                            self.pluginNameToObjMap[keyitem],
                            self.pluginNameToObjMap[keyitem].pluginType)
                if self.pluginNameToObjMap[keyitem].pluginType in [Types.MODULE_DATA_PROCESSOR]:
                    priorityVal = self.pluginNameToObjMap[keyitem].executionPriority
                    allPriorityValues.append(priorityVal)
                    self.dataProcPluginsMap[priorityVal] = self.pluginNameToObjMap[keyitem]
            sortedPriorityKeys = sorted(set(allPriorityValues))
            for index in range(self.dataproc_threads):
                self.dataProcessWorkerList.append(DataProcessor(
                    self.dataProcPluginsMap,
                    sortedPriorityKeys,
                    self,
                    name=str(index + 100),  # make unique worker names
                    daemon=False))
            # # intialize the single data processing queue consumer worker:
            # self.singleDataProcessor = DataProcessor(
            #     self.dataProcPluginsMap,
            #     sortedPriorityKeys,
            #     self,
            #     name='100',  # make unique worker names
            #     daemon=False)
        except Exception as e:
            logger.error("Exiting: When Initializing the data processing worker thread with plugins, error was: %s",
                         e)
            sys.exit(2)
        # after this, the self.singleDataProcessor is the thread that contains instantiated data processing plugins
        logger.info("%s worker threads initialised to process data for %s plugins.",
                    len(self.dataProcessWorkerList), len(self.dataProcPluginsMap))

    def runAllJobs(self):
        """ Process Queue to run all web source (URL) identification jobs
        """
        # To begin with, initialize the URL identifying workers
        self.initURLSourcingWorkers()
        # Next, initialize the URL content-fetching workers
        self.initContentFetchWorkers()
        try:
            logger.debug("Starting all worker threads.")
            # loop through urlSrcWorkers, and start their threads:
            for keyitem in self.urlSrcWorkers.keys():
                self.urlSrcWorkers[keyitem].start()

            # logger.debug("Waiting for %s seconds to start content fetching worker threads.", self.fetchCycleTime)
            # time.sleep(self.fetchCycleTime)

            # loop through workers, and start their threads:
            for keyitem in self.contentFetchWorkers.keys():
                self.contentFetchWorkers[keyitem].start()
            # initialise the data processing jobs:
            self.initDataProcWorkers()
            # For each item in completed queue, run each data processing plugins in order of priority:
            # loop through workers, and start their threads:
            for dat_worker in self.dataProcessWorkerList:
                dat_worker.start()
            # start worker thread that saves history.
            self.progressWatchThread.start()

            # wait for urlSrcWorkers to finish
            for keyitem in self.urlSrcWorkers.keys():
                self.urlSrcWorkers[keyitem].join()
            # wait for all threads to finish
            for keyitem in self.contentFetchWorkers.keys():
                self.contentFetchWorkers[keyitem].join()
                logger.info("Worker thread completed fetching content for plugin: %s",
                            self.contentFetchWorkers[keyitem].pluginName)
            logger.info('Completed fetching all data.')
            for dat_worker in self.dataProcessWorkerList:
                dat_worker.join()

            # wait for progress watcher/history db worker thread to finish
            self.progressWatchThread.join()
            logger.debug("Worker thread that saves history finished running now.")
            # save any remaining list of URLs retrieved to the history database
            self.sessionHistoryDB.writeQueueToDB()

            # serially perform any data processing required on entire dataset for given day:
            # self.processDataSerially(self.runDate)
        except KeyboardInterrupt:
            print("Recognized keyboard interrupt, stopping the program now...")

    def finishAllTasks(self):
        """ At the end, perform any clean-ups and print summary to log
        """
        self.sessionHistoryDB.printDBStats()

    def processDataSerially(self, forDate):
        """ Process Data on Workers:
        loop through data plugins and execute these in serial order
        """
        # fetch and  read data that was processed by each plugin:
        for pluginName in self.pluginNameToObjMap.keys():
            thisPlugin = self.pluginNameToObjMap[pluginName]
            if thisPlugin.pluginType == Types.MODULE_DATA_PROCESSOR:
                thisPlugin.processData(forDate)
                # in the end mark completion by changing state:
                thisPlugin.pluginState = Types.STATE_STOPPED
                logger.debug('Completed processing data from plugin: %s', pluginName)


# # end of file ##
