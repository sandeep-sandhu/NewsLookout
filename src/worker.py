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
#    worker                                                                                               #
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
from data_structs import Types
import scraper_utils
from data_structs import QueueStatus

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
        :type sessionHistoryDB: data_structs.SessionHistory
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
        logger.debug("Worker %s initialized with the plugin: %s", self.workerID, self.pluginObj)
        super().__init__(daemon=daemon, target=target, name=name)

    def setRunDate(self, runDate):
        self.runDate = runDate

    def setDomainMapAndPlugins(self, domainToPluginMap, allPluginObjs):
        self.domainToPluginMap = domainToPluginMap
        self.pluginNameToObjMap = allPluginObjs

    def runURLListGatherTasks(self):
        """ Run Tasks to gather the listing of URLs
        """
        pluginName = type(self.pluginObj).__name__
        try:
            logger.info(f"Started identifying URLs for plugin: {pluginName}")
            # fetch URL list using each plugin's function:
            if self.pluginObj.pluginType in [
                    Types.MODULE_NEWS_CONTENT,
                    Types.MODULE_DATA_CONTENT,
                    Types.MODULE_NEWS_API
                    ]:
                self.pluginObj.getURLsListForDate(self.runDate, self.sessionHistoryDB)
                # TODO: check if both individual data fetcher plugins and news agg are completed,
                #  only then put queue end marker, and change state
                self.pluginObj.putQueueEndMarker()
            if self.pluginObj.pluginType == Types.MODULE_NEWS_AGGREGATOR:
                # Recursion is not applicable for news aggregators; simply add url into common queue:
                urlList = self.pluginObj.getURLsListForDate(self.runDate, self.sessionHistoryDB)
                # collect URLs in a dictionary mapped to each plugin
                pluginToURLListMap = dict()
                # initialize the dictionary:
                for pluginItem in self.pluginNameToObjMap.keys():
                    # Key -> Value is: Plugin name -> URL List
                    pluginToURLListMap[pluginItem] = []
                # logger.debug("pluginToURLListMap: %s", pluginToURLListMap)
                for urlItem in urlList:
                    # find the domain from url:
                    domainName = scraper_utils.getNetworkLocFromURL(urlItem)
                    if domainName in self.domainToPluginMap:
                        # identify the relevant pluginName from the domainName using domainMap:
                        thisPlugin = self.domainToPluginMap[domainName]
                        logger.debug(f'For plugin: {thisPlugin}, and domain: {domainName}, adding URL: {urlItem}')
                        pluginToURLListMap[thisPlugin].append(urlItem)
                # put all collected URLs into each plugin:
                for pluginItem in pluginToURLListMap.keys():
                    urlCount = len(pluginToURLListMap[pluginItem])
                    logger.info(f'News aggregator {pluginName} added {urlCount} ' +
                                f'URLs to plugin {pluginItem}')
                    if pluginItem in pluginToURLListMap and urlCount>0:
                        self.pluginNameToObjMap[pluginItem].addURLsListToQueue(pluginToURLListMap[pluginItem])
                        self.sessionHistoryDB.addURLsToPendingTable(pluginToURLListMap[pluginItem], pluginItem)
                # Mark stop state for this News Aggregator plugin
                self.pluginObj.pluginState = Types.STATE_STOPPED
                self.pluginObj.clearQueue()
            logger.debug('Thread %s finished getting URL listing for plugin %s',
                         self.workerID,
                         pluginName)
        except Exception as e:
            logger.error(
                'When trying to get URL listing using plugin: %s, Type TASK_GET_URL_LIST, Exception: %s',
                pluginName,
                e)

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
                if sURL is not None:
                    fetchResult = self.pluginObj.fetchDataFromURL(sURL, self.workerID)
                    if fetchResult is not None and fetchResult.wasSuccessful is True:
                        self.queue_manager.addToScrapeCompletedQueue(fetchResult)
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

    def __init__(self, dataProcPluginsMap, sortedPriorityKeys, queue_manager,
                 daemon=None, target=None, name=None):
        """  Initialize the worker thread

        :param dataProcPluginsMap: The dictionary of data processing plugins as priority -> objects
        :param sortedPriorityKeys: The priority wise list of keys for the data processing plugin map
        :param queue_manager: The queue manager instance
        :param daemon: Optional parameter indicating whether this thread should work as daemon?
        :param target: Optional parameter to method that should be run
        :param name: Optional name of the worker thread
        """
        self.workerID = name
        self.dataProcPluginsMap = dataProcPluginsMap
        self.sortedPriorityKeys = sortedPriorityKeys
        self.queue_manager = queue_manager
        logger.debug("Consumer for data processing plugins queue %s initialized with the plugins: %s",
                     self.workerID,
                     self.dataProcPluginsMap)
        super().__init__(daemon=daemon, target=target, name=name)

    def arePluginsFetchingData(self):
        """ Checks whether any plugin is still sourcing URLs for given date.

        :return: True/False result of status check
         or fetching data from URLs
         :rtype: bool
        """
        # check status of all plugins again:
        (srcnt, curQMp, totQMp, fetchQ, allFetchQ, dPrcIn, dPrcOu, cn, bnet, st) = self.queue_manager.getQueueStatus()
        return(bnet or dPrcIn > 0)

    def run(self):
        """ Runs when the thread is started.
        """
        itemInQueue = None
        arePluginsStillSourcingData = self.arePluginsFetchingData()
        logger.info(f'{self.workerID} Data processing thread: Started.')
        while(arePluginsStillSourcingData and len(self.dataProcPluginsMap) > 0):
            try:
                # if anything in queue, pick it up and process it:
                itemInQueue = self.queue_manager.fetchFromDataProcInputQ(
                    block=True,
                    timeout=self.queueBlockTimeout)
                if itemInQueue is not None and itemInQueue.URL not in self.queue_manager.alreadyDataProcList:
                    item_doc = None
                    self.queue_manager.alreadyDataProcList.append(itemInQueue.URL)
                    self.queue_manager.addToDataProcessedQueue(itemInQueue)
                    # fetch and  read data that was processed by each plugin:
                    for priorityVal in self.sortedPriorityKeys:
                        thisPlugin = None
                        try:
                            thisPlugin = self.dataProcPluginsMap[priorityVal]
                            if item_doc is None:
                                item_doc = thisPlugin.loadDocument(itemInQueue.savedDataFileName)
                            thisPlugin.processDataObj(item_doc)
                        except Exception as pluginError:
                            logger.error("%s: Data processing thread: %s Plugin: %s, file = %s, URL = %s",
                                         self.workerID,
                                         thisPlugin.pluginName,
                                         pluginError,
                                         itemInQueue.savedDataFileName,
                                         itemInQueue.URL)
                    logger.debug('%s: Data processing thread: processed: %s, input queue size=%s, output queue size=%s',
                                 self.workerID,
                                 itemInQueue.savedDataFileName,
                                 self.queue_manager.getCompletedQueueSize(),
                                 self.queue_manager.getDataProcessedQueueSize()
                                 )
                else:
                    self.queue_manager.addToDataProcessedQueue(itemInQueue)
                    if itemInQueue is not None:
                        logger.debug('%s: Discarding already processed item for URL %s.',
                                     self.workerID, itemInQueue.URL)
            except Exception as e:
                logger.info('Data processing thread: Nothing available from queue, size = %s: %s, itemInQueue = %s',
                            self.queue_manager.getCompletedQueueSize(),
                            e,
                            itemInQueue)
            try:
                time.sleep(self.waitTimeSec)
                arePluginsStillSourcingData = self.arePluginsFetchingData()
                logger.debug('Data processing thread: Continue thread? %s', arePluginsStillSourcingData)
            except Exception as statusCheckError:
                logger.error("Data processing thread: Error when checking plugin state: %s", statusCheckError)
        logger.debug('%s: Data processing thread: Finished data processing on all data.', self.workerID)


##########


class ProgressWatcher(threading.Thread):
    """ Worker object to save completed URL to history database asynchronously
    """
    workerID = -1
    # refreshIntervalSecs is an estimate of the time in seconds a thread takes to retrieve a url:
    refreshIntervalSecs = 5
    refreshWaitCountForLog = 10
    previousState = dict()
    currentState = dict()
    q_mgr = None
    historyDB = None
    # for URL identification and sourcing progress bar:
    totalPluginsURLSourcing = 0
    countOfPluginsURLListPending = 0
    # for scraped progress bar:
    totalURLQueueSize = 0
    countOfURLsDownloaded = 0
    # for data processing progress bar:
    countWrittenToDB = 100
    countOfDataProcessed = 0
    q_status = None

    def __init__(self, pluginNameObjMap, sessionHistoryDB, queue_manager, progressRefreshInt,
                 daemon=None, target=None, name=None):
        """ Initialize the thread object
        """
        self.workerID = name
        self.pluginNameObjMap = pluginNameObjMap
        self.historyDB = sessionHistoryDB
        self.q_mgr = queue_manager
        self.refreshIntervalSecs = progressRefreshInt
        self.q_status = QueueStatus(queue_manager)
        self.q_status.updateStatus()
        self.enlighten_manager = enlighten.get_manager()
        self.totalPluginsURLSourcing = self.q_mgr.getTotalSrcPluginCount()
        # create the progress bars:
        self.urlListFtBar = self.enlighten_manager.counter(count=0,
                                                           total=self.totalPluginsURLSourcing,
                                                           desc='URLs identified:',
                                                           unit='Plugins',
                                                           color='yellow')
        self.urlScrapeBar = self.enlighten_manager.counter(count=0,
                                                           total=0,
                                                           desc='Data downloaded:',
                                                           unit='   URLs',
                                                           color='cyan')
        self.dataProcsBar = self.enlighten_manager.counter(count=0,
                                                           total=0,
                                                           desc=' Data processed:',
                                                           unit='  Files',
                                                           color='green')
        logger.debug("Progress watcher thread %s initialized with the plugins: %s",
                     self.workerID,
                     self.pluginNameObjMap)
        # call base class:
        super().__init__(daemon=daemon, target=target, name=name)

    @staticmethod
    def getStatusChange(previousState, currentState):
        """ Get Status Change
        """
        statusMessages = []
        # current status is: self.currentState, previous status is: self.previousState
        for pluginName in currentState.keys():
            try:
                # for each key in current status, check and compare value in previous state
                currentState = currentState[pluginName]
                if len(previousState) > 0 and currentState != previousState[pluginName]:
                    # print For plugin z, status changed from x to y
                    statusMessages.append(pluginName +
                                          ' changed state to -> ' +
                                          currentState.replace('STATE_', '').replace('_', ' '))
            except Exception as e:
                logger.debug("Progress watcher thread: Error comparing previous state of plugin: %s", e)
        return(statusMessages)

    def getAllPluginsState(self):
        """ Get the state of all plugins. Return a tuple with the following structure:
          - Dictionary of current state
          - Flag indicating whether any plugin is still in state of identifying URLs or fetching data

        :return: Tuple -> (currentStateMap, isPluginStillFetchingData)
        """
        isPluginStillFetchingData = False
        for pluginID in self.pluginNameObjMap.keys():
            isPluginStillFetchingData = isPluginStillFetchingData or (
                self.pluginNameObjMap[pluginID].pluginState == Types.STATE_FETCH_CONTENT
                ) or (self.pluginNameObjMap[pluginID].pluginState == Types.STATE_GET_URL_LIST)
            self.totalURLQueueSize = self.totalURLQueueSize + self.pluginNameObjMap[pluginID].urlQueueTotalSize

    def run(self):
        """ Main method that runs this progress reporting thread and
         periodically saves it to the history database.
        """
        self.countWrittenToDB = 0
        log_event_update_cnt = 0
        fetchCompletedCount = 0
        totalTimeElapsed = self.refreshIntervalSecs
        logger.info('Progress watcher thread: Started.')
        try:
            (srcnt, curQMp, totQMp, fetchQ, allFetchQ, dPrcIn, dPrcOu, cn, bnet, st) = self.q_mgr.getQueueStatus()
            self.urlListFtBar.update(incr=self.totalPluginsURLSourcing - srcnt)
            self.q_status.updateStatus()
            self.urlScrapeBar.total = cn
            fetchPendingCount = 0
            for thiskey in curQMp.keys():
                fetchPendingCount = fetchPendingCount + curQMp[thiskey]
            self.urlScrapeBar.update(incr=(cn - fetchPendingCount))
            print("Web-scraping Progress:")
            # TODO: also check if any data processing is pending in queue:
            while (not self.q_mgr.isFetchQEmpty()) or bnet is True or dPrcIn <= 1:
                countOfURLsWrittenToDB = self.historyDB.writeQueueToDB()
                # keep a total count, this will be reported before closing the application
                self.countWrittenToDB = self.countWrittenToDB + countOfURLsWrittenToDB

                # reset flag before checking all plugins again within loop:
                prevCompletedURLCount = fetchCompletedCount
                prevURLListPluginPending = srcnt
                prevCountOfDataProcessed = dPrcOu
                # save previous state, use deepcopy for dictionary object:
                previousState = copy.deepcopy(st)

                # wait for some time before checking again:
                time.sleep(self.refreshIntervalSecs)
                totalTimeElapsed = totalTimeElapsed + self.refreshIntervalSecs

                self.q_status.updateStatus()
                # TODO: change over to the q_status object instance:
                (srcnt, curQMp, totQMp, fetchQ, allFetchQ, dPrcIn, dPrcOu, cn, bnet, st) = self.q_mgr.getQueueStatus()
                if log_event_update_cnt > self.refreshWaitCountForLog:
                    log_event_update_cnt = 0
                    logger.info(f"Plugin states: {st}")
                    logger.info(f'Count of plugins still sourcing URLs = {srcnt}, ' +
                                f'out of a total of {self.totalPluginsURLSourcing} plugins.')
                    logger.info(f"All plugins current queue sizes: {curQMp}")
                    logger.info(f"All plugins total queue sizes: {totQMp}")
                    logger.info(f"Count of all URLs to fetch: {cn}, " +
                                f"Are any plugins still fetching over network? {bnet}")
                    logger.info(f"Current fetch completed count: {fetchQ}, " +
                                f"Total fetch completed count: {allFetchQ}")
                    logger.info(f"Data Processing Input Queue size: {dPrcIn}, " +
                                f"Data Processing Output Queue size: {dPrcOu}")
                log_event_update_cnt = log_event_update_cnt + 1
                self.urlScrapeBar.total = cn
                self.dataProcsBar.total = dPrcIn + dPrcOu
                if dPrcIn + dPrcOu == 0:
                    self.dataProcsBar.total = cn
                self.urlListFtBar.update(incr=prevURLListPluginPending - srcnt)
                # TODO: urlscrapebar - the count of url in queue is more than fetched by plugins,
                #  so total isnt reaching 100%, increment should factor in difference in queues:
                fetchPendingCount = 0
                for thiskey in curQMp.keys():
                    fetchPendingCount = fetchPendingCount + curQMp[thiskey]
                fetchCompletedCount = cn - fetchPendingCount
                self.urlScrapeBar.update(incr=(fetchCompletedCount - prevCompletedURLCount))
                self.dataProcsBar.update(incr=(dPrcOu - prevCountOfDataProcessed))
                for statusMessage in ProgressWatcher.getStatusChange(previousState, st):
                    logger.info(statusMessage)
                if bnet is False:
                    break
            self.urlScrapeBar.close()
            self.urlListFtBar.close()
            self.dataProcsBar.close()
            logger.info('Progress watcher thread: Finished saving a total of %s URLs in history database.',
                        self.countWrittenToDB)
        except Exception as e:
            logger.error("Progress watcher thread: trying to save history data, the exception was: %s", e)


# # end of file ##
