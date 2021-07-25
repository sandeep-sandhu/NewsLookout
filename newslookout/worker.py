#!/usr/bin/env python
# -*- coding: utf-8 -*-

# #########################################################################################################
# File name: worker.py                                                                                    #
# Application: The NewsLookout Web Scraping Application                                                   #
# Date: 2021-06-23                                                                                        #
# Purpose: This object encapsulates the worker thread that                                                #
#  runs all multi-threading functionality to run the                                                      #
#  web scraper plugins loaded by the application.                                                         #
#                                                                                                         #
# Copyright 2021, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu, sandeep.sandhu@gmx.com  #
#                                                                                                         #
# Provides:                                                                                               #
#    PluginWorker                                                                                               #
#        run                                                                                              #
#        setRunDate                                                                                       #
#        runURLListGatherTasks                                                                            #
#        runDataRetrievalTasks                                                                            #
#        runDataProcessingTasks                                                                           #
#                                                                                                         #
#    DataProcessor                                                                                        #
#        run                                                                                              #
#                                                                                                         #
#    ProgressWatcher                                                                                      #
#        run                                                                                              #
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

# #########

# import standard python libraries:
import logging
import threading
import time
from datetime import datetime
import queue
import copy
import enlighten

# import this project's modules
import session_hist
from data_structs import Types
import scraper_utils
from data_structs import QueueStatus

# #########

logger = logging.getLogger(__name__)

# #########


class PluginWorker(threading.Thread):
    """ This worker object runs fetching and data extraction processes within each thread.
    """
    workerID = -1
    taskType = None
    runDate = datetime.now()
    queueFillwaitTime = 1

    def __init__(self,
                 pluginObj,
                 taskType,
                 sessionHistoryDB,
                 queue_manager,
                 daemon=None, target=None, name=None):
        """
         Initialize the worker with plugin object for executing the given task,
          and the history data manager

        :param pluginObj: The plugin object instance to run
        :type pluginObj: base_plugin.basePlugin
        :param taskType: The task is either fetch url list or fetch data.
         Based on the type of task the relevant plugin method will be invoked by this thread
        :type taskType: data_structs.Types
        :param sessionHistoryDB:
        :type sessionHistoryDB: session_hist.SessionHistory
        :param queue_manager: Queue manager instance
        :param daemon: Optional - indicates whether this will be a daemon thread,
         to be passed to the parent object - Thread
        :param target: Optional - alternative method to run instead of run(),
         to be passed to the parent object - Thread
        :param name: Optional - the thread's name, to be passed to the parent object - Thread
        """
        self.workerID = name
        self.pluginObj = pluginObj
        self.pluginName = type(self.pluginObj).__name__
        self.taskType = taskType
        self.queue_manager = queue_manager
        self.sessionHistoryDB = sessionHistoryDB
        self.queueFillwaitTime = 120
        logger.debug(f"PluginWorker {self.workerID} initialized for the plugin: {self.pluginObj}")
        super().__init__(daemon=daemon, target=target, name=name)

    def setRunDate(self, runDate: datetime):
        self.runDate = runDate

    def setDomainMapAndPlugins(self, domainToPluginMap: dict, allPluginObjs: dict):
        self.domainToPluginMap = domainToPluginMap
        self.pluginNameToObjMap = allPluginObjs

    @staticmethod
    def aggregator_url2domain_map(urlList: list, pluginNameToObjMap: dict, domainToPluginMap: dict) -> dict:
        """ Collect URLs in a dictionary mapped to each plugin

        :param urlList: Mixed list of all URLs for various news sites/domains
        :param pluginNameToObjMap: Dictionary/map with keys as plugin name -> plugin object as values
        :param domainToPluginMap: Dictionary/map with keys as domain names -> plugin-name as values
        :return: Dictionary map with keys as plugin-name -> URL list as values
        """

        plugin_to_url_list_map = dict()
        # initialize the dictionary:
        for pluginItem in pluginNameToObjMap.keys():
            # Key -> Value is: Plugin name -> URL List
            plugin_to_url_list_map[pluginItem] = []
        # next, allocate each corresponding URL to the map
        for urlItem in urlList:
            # find the domain from url:
            domainName = scraper_utils.getNetworkLocFromURL(urlItem)
            if domainName in domainToPluginMap:
                # identify the relevant pluginName from the domainName using domainMap:
                thisPlugin = domainToPluginMap[domainName]
                logger.debug(f'For plugin: {thisPlugin}, and domain: {domainName}, adding URL: {urlItem}')
                plugin_to_url_list_map[thisPlugin].append(urlItem)
        return(plugin_to_url_list_map)

    def assign_urls_to_queues(self, plugin_to_url_list_map: dict,
                              pluginName: str,
                              pluginNameToObjMap: dict,
                              sessionHistoryDB: session_hist.SessionHistory):
        """ Assign all collected URLs into the corresponding plugin's queue:

        :param plugin_to_url_list_map: Dictionary map with keys as plugin-name -> URL list as values
        :param pluginName: Name of the news aggregator plugin sourcing the list of news articles
        :param pluginNameToObjMap: Dictionary/map with keys as plugin name -> plugin object as values
        :param sessionHistoryDB: Session history database helper object
        :return:
        """
        for pluginItem in plugin_to_url_list_map.keys():
            # process each plugin-name one by one
            urlCount = len(plugin_to_url_list_map[pluginItem])
            logger.info(f'News aggregator {pluginName} added {urlCount} ' +
                        f'URLs to plugin {pluginItem}')
            # if this plugin-name exists in dictionary of name-objects,
            # and there are URLs available to fetch, then add these to the plugin's queue
            if pluginItem in pluginNameToObjMap and urlCount > 0:
                pluginNameToObjMap[pluginItem].addURLsListToQueue(plugin_to_url_list_map[pluginItem], sessionHistoryDB)
                sessionHistoryDB.addURLsToPendingTable(plugin_to_url_list_map[pluginItem], pluginItem)

    def runURLListGatherTasks(self):
        """ Run Tasks to gather the listing of URLs
        """
        try:
            logger.info(f"Started identifying URLs for plugin: {self.pluginName}")
            # fetch URL list using each plugin's function:
            if self.pluginObj.pluginType in [
                    Types.MODULE_NEWS_CONTENT,
                    Types.MODULE_DATA_CONTENT,
                    Types.MODULE_NEWS_API
                    ]:
                urlList = self.pluginObj.getURLsListForDate(self.runDate, self.sessionHistoryDB)
                self.pluginObj.addURLsListToQueue(urlList, self.sessionHistoryDB)
                self.sessionHistoryDB.addURLsToPendingTable(urlList, self.pluginName)
                # check if both individual data fetcher plugins and news agg are completed,
                #  only then put queue end marker, and change state
                while(self.queue_manager.q_status.any_newsagg_isactive()):
                    self.queue_manager.q_status.updateStatus()
                    logger.info(f'{self.pluginName} waiting for news aggregator to finish, before closing the queue.')
                    time.sleep(self.queueFillwaitTime)
                self.pluginObj.putQueueEndMarker()
            elif self.pluginObj.pluginType == Types.MODULE_NEWS_AGGREGATOR:
                # Recursion is not applicable for news aggregators; simply add url into common queue:
                urlList = self.pluginObj.getURLsListForDate(self.runDate, self.sessionHistoryDB)
                plugin_to_url_list_map = PluginWorker.aggregator_url2domain_map(urlList,
                                                                                self.pluginNameToObjMap,
                                                                                self.domainToPluginMap)
                # put all collected URLs into each plugin queue:
                self.assign_urls_to_queues(plugin_to_url_list_map,
                                                   self.pluginName,
                                                   self.pluginNameToObjMap,
                                                   self.sessionHistoryDB)
                # Mark stop state for this News Aggregator plugin
                self.pluginObj.pluginState = Types.STATE_STOPPED
                self.pluginObj.clearQueue()
            logger.debug(f'Thread {self.workerID} finished getting URL listing for plugin {self.pluginName}')
        except Exception as e:
            logger.error(f'When trying to get URL listing using plugin: {self.pluginName},' +
                         f' Type TASK_GET_URL_LIST, Exception: {e}')

    def runDataRetrievalTasks(self):
        """ Run Data Retrieval Tasks
        """
        logger.info("Started data retrieval job for plugin: %s, queue size: %s",
                    self.pluginName,
                    self.pluginObj.getQueueSize()
                    )
        while (not self.pluginObj.isQueueEmpty()) or (self.pluginObj.pluginState == Types.STATE_GET_URL_LIST):
            sURL = None
            try:
                if self.pluginObj.isQueueEmpty():
                    logger.debug('%s: Waiting %s seconds for input queue to fill up; plugin state = %s',
                                 self.pluginName,
                                 self.queueFillwaitTime,
                                 Types.decodeNameFromIntVal(self.pluginObj.pluginState)
                                 )
                    time.sleep(self.queueFillwaitTime)
                sURL = self.pluginObj.getNextItemFromFetchQueue(timeout=self.queueFillwaitTime)
                # Check if url is valid and whether has already been fetched or not:
                if sURL is not None and self.sessionHistoryDB.url_was_attempted(sURL, self.pluginName) is False:
                    logger.debug(f'{self.pluginName} started fetching URL: {sURL}')
                    fetchResult = self.pluginObj.fetchDataFromURL(sURL, self.workerID)
                    if fetchResult is not None and fetchResult.wasSuccessful is True:
                        self.queue_manager.addToScrapeCompletedQueue(fetchResult)
                        # if additional links have been identified, add these to fetch queue:
                        # filter additionalLinks through pending queue and failed queue to remove duplicates:
                        addl_urls = self.sessionHistoryDB.removeAlreadyFetchedURLs(fetchResult.additionalLinks,
                                                                                   self.pluginName)
                        if len(addl_urls) > 0:
                            logger.debug(f'{self.pluginName} added {len(addl_urls)} additional URLs from {sURL}.')
                            # DISABLED temporarily:
                            # self.pluginObj.addURLsListToQueue(addl_urls, self.sessionHistoryDB)
                            # self.sessionHistoryDB.addURLsToPendingTable(addl_urls, self.pluginName)
                    else:
                        # add url to failed table:
                        self.sessionHistoryDB.addURLToFailedTable(fetchResult, self.pluginName, datetime.now())
                else:
                    logger.info('Got queue end sentinel, stopping data retrieval for plugin %s.',
                                self.pluginName)
                    self.pluginObj.clearQueue()
                    # exit from while loop:
                    break
            except queue.Empty as qempty:
                logger.debug("%s: Queue was empty when trying to retrieve data: %s",
                             self.pluginName,
                             qempty
                             )
            except Exception as e:
                logger.error("%s: When trying to retrieve data the exception was: %s, url = %s",
                             self.pluginName,
                             e,
                             sURL
                             )
        logger.debug('Thread %s finished tasks to retrieve data for plugin %s',
                     self.workerID,
                     self.pluginName)
        # at this point, since all data has been fetched, change the plugin's state to process data:
        self.pluginObj.pluginState = Types.STATE_STOPPED

    def run(self):
        """ Overridden to enable parent thread object to be called for executing the Plugin jobs
        """
        if self.taskType == Types.TASK_GET_URL_LIST:
            self.runURLListGatherTasks()
        elif self.taskType == Types.TASK_GET_DATA:
            self.runDataRetrievalTasks()


##########


class DataProcessor(threading.Thread):
    """  Worker object that asynchronously reads saved articles/data,
     and processes them via the data processing modules
    Extends base class: threading.Thread
    """
    workerID = -1
    waitTimeSec = 5
    queueBlockTimeout = 240
    queue_manager = None
    sortedPriorityKeys = []

    def __init__(self, dataProcPluginsMap, sortedPriorityKeys, queue_manager, queue_status,
                 daemon=None, target=None, name=None):
        """  Initialize the worker thread

        :param dataProcPluginsMap: The dictionary of data processing plugins as priority -> objects
        :param sortedPriorityKeys: The priority wise list of keys for the data processing plugin map
        :param queue_manager: The queue manager instance
        :param daemon: Optional parameter indicating whether this thread should work as daemon?
        :param target: Optional parameter to method that should be run
        :param name: Optional name of the worker thread
        """
        # TODO: pass only the queue status object
        self.workerID = name
        self.dataProcPluginsMap = dataProcPluginsMap
        self.sortedPriorityKeys = sortedPriorityKeys
        self.queue_manager = queue_manager
        self.q_status = queue_status
        logger.debug("Consumer for data processing plugins queue %s initialized with the plugins: %s",
                     self.workerID,
                     self.dataProcPluginsMap)
        super().__init__(daemon=daemon, target=target, name=name)

    @staticmethod
    def processItem(queue_manager, itemInQueue, sortedPriorityKeys, dataProcPluginsMap, workerID):
        item_doc = None
        queue_manager.alreadyDataProcList.append(itemInQueue.URL)
        queue_manager.addToDataProcessedQueue(itemInQueue)
        # fetch and  read data that was processed by each plugin:
        for priorityVal in sortedPriorityKeys:
            thisPlugin = None
            try:
                thisPlugin = dataProcPluginsMap[priorityVal]
                logger.debug(f'Processing data using plugin: {thisPlugin.pluginName}')
                if item_doc is None:
                    item_doc = thisPlugin.loadDocument(itemInQueue.savedDataFileName)
                    logger.debug(f'Loaded document for URL: {item_doc.getURL()}')
                if item_doc is not None:
                    logger.debug(f'Processing document ID: {item_doc.getArticleID()}')
                    thisPlugin.processDataObj(item_doc)
            except Exception as pluginError:
                logger.error(f"Data processing thread {workerID} for plugin {thisPlugin.pluginName} got error" +
                             f": {pluginError}; file = {itemInQueue.savedDataFileName}, URL = {itemInQueue.URL}")

    def run(self):
        """ Runs when the thread is started.
        """
        itemInQueue = None
        self.q_status.updateStatus()
        # logger.debug(f'Data processing: Started thread {self.workerID}')
        while(self.q_status.isPluginStillFetchingoverNetwork or self.q_status.dataInputQsize > 0):
            try:
                # if anything in queue, pick it up and process it:
                itemInQueue = self.queue_manager.fetchFromDataProcInputQ(block=True, timeout=self.queueBlockTimeout)
                if itemInQueue is not None and itemInQueue.URL not in self.queue_manager.alreadyDataProcList:
                    DataProcessor.processItem(self.queue_manager, itemInQueue, self.sortedPriorityKeys,
                                              self.dataProcPluginsMap, self.workerID)
                else:
                    self.queue_manager.addToDataProcessedQueue(itemInQueue)
                    if itemInQueue is not None:
                        logger.debug('Data processing thread %s: Ignoring already processed file for URL %s.',
                                     self.workerID, itemInQueue.URL)
            except Exception as e:
                logger.debug('Data processing thread: Nothing available from queue, size = %s: %s',
                             self.queue_manager.getCompletedQueueSize(), e)
            try:
                time.sleep(self.waitTimeSec)
                self.q_status.updateStatus()
            except Exception as statusCheckError:
                logger.error("Data processing thread: Error when checking plugin state: %s", statusCheckError)


##########


class ProgressWatcher(threading.Thread):
    """ Worker object to save completed URL to history database asynchronously
    """
    workerID = -1
    # refreshIntervalSecs is an estimate of the time in seconds a thread takes to retrieve a url:
    refreshIntervalSecs = 5
    refreshWaitCountForLog = 24
    previousState = dict()
    currentState = dict()
    historyDB = None
    # for URL identification and sourcing progress bar:
    countOfPluginsURLListPending = 0
    # for scraped progress bar:
    totalURLQueueSize = 0
    countOfURLsDownloaded = 0
    # for data processing progress bar:
    countWrittenToDB = 100
    countOfDataProcessed = 0
    q_status = None

    def __init__(self, pluginNameObjMap: dict,
                 sessionHistoryDB,
                 queue_manager: object,
                 queue_status: object,
                 progressRefreshInt: int,
                 daemon=None, target=None, name=None):
        """ Initialize this thread's object

        :param pluginNameObjMap:
        :param sessionHistoryDB:
        :param queue_manager:
        :param queue_status:
        :param progressRefreshInt:
        :param daemon:
        :param target:
        :param name:
        """
        self.workerID = name
        self.pluginNameObjMap = pluginNameObjMap
        self.historyDB = sessionHistoryDB
        self.refreshIntervalSecs = progressRefreshInt
        self.queue_manager = queue_manager
        self.q_status = queue_status
        self.q_status.updateStatus()
        self.enlighten_manager = enlighten.get_manager()
        # create the progress bars:
        self.urlListFtBar = self.enlighten_manager.counter(count=0,
                                                           total=self.q_status.totalPluginsURLSourcing,
                                                           desc='URLs identified:',
                                                           unit='Plugins',
                                                           color='yellow')
        self.urlScrapeBar = self.enlighten_manager.counter(count=0,
                                                           total=1,
                                                           desc='Data downloaded:',
                                                           unit='   URLs',
                                                           color='cyan')
        self.dataProcsBar = self.enlighten_manager.counter(count=0,
                                                           total=1,
                                                           desc=' Data processed:',
                                                           unit='  Files',
                                                           color='green')
        logger.debug("Progress watcher thread %s initialized with the plugins: %s",
                     self.workerID,
                     self.pluginNameObjMap)
        # call base class:
        super().__init__(daemon=daemon, target=target, name=name)

    def run(self):
        """ Main method that runs this progress reporting thread and
         periodically saves it to the history database.
        """
        self.countWrittenToDB = 0
        log_event_update_cnt = 0
        fetchCompletedCount = 0
        logger.info('Progress watcher thread started.')
        try:
            self.q_status.updateStatus()
            # save previous state, use deepcopy for dictionary object:
            previousState = copy.deepcopy(self.q_status.currentState)
            # initial update of the progress bars:
            self.urlListFtBar.update(incr=self.q_status.totalPluginsURLSourcing -
                                     self.q_status.countOfPluginsInURLSrcState)
            self.urlScrapeBar.total = self.q_status.totalURLCount
            self.urlScrapeBar.update(incr=(self.q_status.totalURLCount - self.q_status.fetchPendingCount))
            self.dataProcsBar.total = self.q_status.dataInputQsize + self.q_status.dataOutputQsize
            self.dataProcsBar.update(incr=self.q_status.dataOutputQsize)
            prevCountOfDataProcessed = self.q_status.dataOutputQsize
            print("Web-scraping Progress:")
            # check if any data processing is pending in queue:
            while self.q_status.isPluginStillFetchingoverNetwork is True or self.q_status.dataInputQsize > 0:
                results_from_queue = []
                while not self.queue_manager.isFetchQEmpty():
                    # get all completed urls from queue:
                    results_from_queue.append(self.queue_manager.getFetchResultFromQueue())
                countOfURLsWrittenToDB = self.historyDB.writeQueueToDB(results_from_queue)
                # keep a total count, this will be reported before closing the application
                self.countWrittenToDB = self.countWrittenToDB + countOfURLsWrittenToDB
                prevURLListPluginPending = self.q_status.countOfPluginsInURLSrcState
                # wait for some time before checking again:
                time.sleep(self.refreshIntervalSecs)
                self.q_status.updateStatus()
                if log_event_update_cnt > self.refreshWaitCountForLog:
                    log_event_update_cnt = 0
                    logger.debug(f"Plugin states: {self.q_status.currentState}")
                    logger.debug(f'Count of plugins still sourcing URLs: {self.q_status.countOfPluginsInURLSrcState},' +
                                 f' out of a total of {self.q_status.totalPluginsURLSourcing} plugins.')
                    logger.debug(f"All plugins current queue sizes: {self.q_status.qsizeMap}")
                    logger.debug(f"Current fetch completed count: {self.q_status.fetchCompletQsize}, " +
                                 f"Total fetch completed count: {self.q_status.fetchCompletCount}")
                    # INFO messages:
                    logger.info(f"Total count of all URLs to fetch: {self.q_status.totalURLCount}, " +
                                "Are any plugins still fetching over network? " +
                                f"{self.q_status.isPluginStillFetchingoverNetwork}")
                    logger.info(f"Data items waiting to be processed in Input Queue: {self.q_status.dataInputQsize}, " +
                                f"Data Processed: {self.q_status.dataOutputQsize}")
                    for statusMessage in QueueStatus.getStatusChange(previousState, self.q_status.currentState):
                        logger.info(statusMessage)
                    # save previous state, use deepcopy for dictionary object:
                    previousState = copy.deepcopy(self.q_status.currentState)
                    logger.debug(f'All plugins stopped? {self.q_status.areAllPluginsStopped}')
                log_event_update_cnt = log_event_update_cnt + 1
                # update the progress bars:
                self.urlScrapeBar.total = self.q_status.totalURLCount
                self.dataProcsBar.total = self.q_status.dataInputQsize + self.q_status.dataOutputQsize
                if self.q_status.dataInputQsize + self.q_status.dataOutputQsize == 0:
                    self.dataProcsBar.total = self.q_status.totalURLCount
                self.urlListFtBar.update(incr=prevURLListPluginPending - self.q_status.countOfPluginsInURLSrcState)
                # reset flag before checking all plugins again within loop:
                prevCompletedURLCount = fetchCompletedCount
                fetchCompletedCount = self.q_status.totalURLCount - self.q_status.fetchPendingCount
                self.urlScrapeBar.update(incr=(fetchCompletedCount - prevCompletedURLCount))
                self.dataProcsBar.update(incr=(self.q_status.dataOutputQsize - prevCountOfDataProcessed))
                prevCountOfDataProcessed = self.q_status.dataOutputQsize
            self.urlScrapeBar.close()
            self.urlListFtBar.close()
            self.dataProcsBar.update(incr=(self.q_status.dataOutputQsize - prevCountOfDataProcessed))
            self.dataProcsBar.close()
            logger.info('Progress watcher thread: Finished saving a total of %s URLs in history database.',
                        self.countWrittenToDB)
        except Exception as e:
            logger.error("Progress watcher thread: trying to save history data, the exception was: %s", e)


# # end of file ##
