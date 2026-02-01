#!/usr/bin/env python
# -*- coding: utf-8 -*-

# #########################################################################################################
# File name: worker.py                                                                                    #
# Application: The NewsLookout Web Scraping Application                                                   #
# Date: 2021-06-23                                                                                        #
# Purpose: Worker Module - it encapsulates the worker thread that                                         #
#  runs all multi-threading functionality to run the web scraper plugins loaded by the application.       #
#  This module contains worker thread classes for URL discovery, content fetching,                        #
#  and data processing.                                                                                   #
#                                                                                                         #
# Copyright 2021, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu, sandeep.sandhu@gmx.com  #
#                                                                                                         #
# Provides:                                                                                               #
#    PluginWorker                                                                                         #
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


import copy
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from pathlib import Path
import logging
import threading
import time
import queue
from datetime import datetime
from typing import Optional

import enlighten
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.responses import HTMLResponse
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from data_structs import PluginTypes, QueueStatus
import scraper_utils


logger = logging.getLogger(__name__)


class PluginWorker(threading.Thread):
    """
    Worker thread that executes plugin operations.

    This class runs fetching and data extraction processes within each thread.
    It supports both URL discovery and content fetching tasks.

    Attributes:
        workerID: Unique identifier for this worker
        pluginObj: The plugin instance this worker executes
        taskType: Type of task (URL_LIST or GET_DATA)
        url_gathering_timeout: Maximum time to spend gathering URLs (seconds)
    """

    def __init__(self, pluginObj, taskType, sessionHistoryDB, queue_manager,
                 daemon=False, target=None, name=None):
        """
        Initialize the plugin worker.

        Args:
            pluginObj: Plugin instance to execute
            taskType: Type of task (TASK_GET_URL_LIST or TASK_GET_DATA)
            sessionHistoryDB: Database interface for session history
            queue_manager: Queue manager instance
            daemon (bool, optional): Whether this is a daemon thread
            target (callable, optional): Alternative method to run
            name (str, optional): Thread name
        """
        self.workerID = name
        self.pluginObj = pluginObj
        self.pluginName = type(self.pluginObj).__name__
        self.taskType = taskType
        self.queue_manager = queue_manager
        self.sessionHistoryDB = sessionHistoryDB
        self.queueFillwaitTime = 120
        self.url_gathering_timeout = 600  # Default 10 minutes
        self.url_gather_start_time = None

        logger.debug(f"PluginWorker {self.workerID} initialized for plugin: {self.pluginObj}")
        super().__init__(daemon=daemon, target=target, name=name)

    def setRunDate(self, runDate: datetime):
        """Set the run date for this worker."""
        self.runDate = runDate

    def setDomainMapAndPlugins(self, domainToPluginMap: dict, allPluginObjs: dict):
        """Set domain mapping for news aggregator plugins."""
        self.domainToPluginMap = domainToPluginMap
        self.pluginNameToObjMap = allPluginObjs

    def _checkShutdown(self) -> bool:
        """
        Check if shutdown has been requested.

        Returns:
            bool: True if shutdown requested, False otherwise
        """
        if self.queue_manager.shutdown_event.is_set():
            logger.info(f"Worker {self.workerID} ({self.pluginName}) received shutdown signal")
            return True
        if hasattr(self.pluginObj, 'is_stopped') and self.pluginObj.is_stopped:
            logger.info(f"Worker {self.workerID} ({self.pluginName}) plugin stopped flag set")
            return True
        return False

    def _checkURLGatheringTimeout(self) -> bool:
        """
        Check if URL gathering has exceeded timeout.

        Returns:
            bool: True if timeout exceeded, False otherwise
        """
        if self.url_gather_start_time is None:
            return False

        elapsed = time.time() - self.url_gather_start_time
        if elapsed > self.url_gathering_timeout:
            logger.warning(f"{self.pluginName}: URL gathering timeout ({self.url_gathering_timeout}s) exceeded")
            return True
        return False

    @staticmethod
    def aggregator_url2domain_map(urlList: list, pluginNameToObjMap: dict,
                                  domainToPluginMap: dict) -> dict:
        """
        Map URLs to appropriate plugins based on domain.

        Args:
            urlList (list): List of URLs to map
            pluginNameToObjMap (dict): Map of plugin names to objects
            domainToPluginMap (dict): Map of domains to plugin names

        Returns:
            dict: Map of plugin names to URL lists
        """
        plugin_to_url_list_map = dict()

        # Initialize dictionary
        for pluginItem in pluginNameToObjMap.keys():
            plugin_to_url_list_map[pluginItem] = []

        # Allocate URLs to plugins
        for urlItem in urlList:
            try:
                domainName = scraper_utils.getNetworkLocFromURL(urlItem)
                if domainName in domainToPluginMap:
                    thisPlugin = domainToPluginMap[domainName]
                    logger.debug(f'For plugin: {thisPlugin}, domain: {domainName}, adding URL: {urlItem}')
                    plugin_to_url_list_map[thisPlugin].append(urlItem)
            except Exception as e:
                logger.debug(f"Error mapping URL {urlItem}: {e}")

        return plugin_to_url_list_map

    def assign_urls_to_queues(self, plugin_to_url_list_map: dict, pluginName: str,
                              pluginNameToObjMap: dict, sessionHistoryDB):
        """
        Assign collected URLs to appropriate plugin queues.

        Args:
            plugin_to_url_list_map (dict): Map of plugin names to URL lists
            pluginName (str): Name of aggregator plugin
            pluginNameToObjMap (dict): Map of plugin names to objects
            sessionHistoryDB: Database interface
        """
        for pluginItem in plugin_to_url_list_map.keys():
            urlCount = len(plugin_to_url_list_map[pluginItem])

            if urlCount > 0:
                logger.info(f'News aggregator {pluginName} added {urlCount} URLs to plugin {pluginItem}')

                if pluginItem in pluginNameToObjMap:
                    # Queue DB operation instead of direct call
                    self.queue_manager.queueDBOperation(
                        'add_pending',
                        (plugin_to_url_list_map[pluginItem], pluginItem),
                        wait_for_result=False
                    )

                    # Add URLs to plugin queue
                    pluginNameToObjMap[pluginItem].addURLsListToQueue(
                        plugin_to_url_list_map[pluginItem],
                        sessionHistoryDB
                    )

    def runURLListGatherTasks(self):
        """
        Execute URL gathering tasks.

        This method:
        1. Starts a timer for timeout monitoring
        2. Gathers URLs from the plugin
        3. Streams URLs to queues as they're discovered
        4. Respects shutdown signals and timeouts
        """
        if self._checkShutdown():
            return

        self.url_gather_start_time = time.time()

        try:
            logger.info(f"Started identifying URLs for plugin: {self.pluginName}")

            if self.pluginObj.pluginType in [
                PluginTypes.MODULE_NEWS_CONTENT,
                PluginTypes.MODULE_DATA_CONTENT,
                PluginTypes.MODULE_NEWS_API
            ]:
                # Get URLs with periodic shutdown checks
                urlList = self._getURLsWithTimeoutCheck()

                if urlList and not self._checkShutdown():
                    # Queue DB operation
                    self.queue_manager.queueDBOperation(
                        'add_pending',
                        (urlList, self.pluginName),
                        wait_for_result=False
                    )

                    self.pluginObj.addURLsListToQueue(urlList, self.sessionHistoryDB)

                # Wait for news aggregators to finish
                while self.queue_manager.q_status.any_newsagg_isactive():
                    if self._checkShutdown() or self._checkURLGatheringTimeout():
                        break

                    self.queue_manager.q_status.updateStatus()
                    logger.debug(f'{self.pluginName} waiting for news aggregator to finish')
                    time.sleep(min(self.queueFillwaitTime, 10))

                if not self._checkShutdown():
                    self.pluginObj.putQueueEndMarker()

            elif self.pluginObj.pluginType == PluginTypes.MODULE_NEWS_AGGREGATOR:
                urlList = self._getURLsWithTimeoutCheck()

                if urlList and not self._checkShutdown():
                    plugin_to_url_list_map = PluginWorker.aggregator_url2domain_map(
                        urlList,
                        self.pluginNameToObjMap,
                        self.domainToPluginMap
                    )

                    self.assign_urls_to_queues(
                        plugin_to_url_list_map,
                        self.pluginName,
                        self.pluginNameToObjMap,
                        self.sessionHistoryDB
                    )

                self.pluginObj.pluginState = PluginTypes.STATE_STOPPED
                self.pluginObj.clearQueue()

            logger.info(f'Thread {self.workerID} finished getting URL listing for plugin {self.pluginName}')

        except Exception as e:
            logger.error(f'Error getting URL listing for plugin {self.pluginName}: {e}')

    def _getURLsWithTimeoutCheck(self) -> list:
        """
        Get URLs from plugin with periodic timeout and shutdown checks.

        Returns:
            list: List of discovered URLs
        """
        urlList = []

        try:
            # Check if method supports timeout/shutdown
            if hasattr(self.pluginObj, 'getURLsListForDate'):
                # Wrap the call to check for shutdown periodically
                # For now, call directly but plugin should check is_stopped internally
                urlList = self.pluginObj.getURLsListForDate(self.runDate, self.sessionHistoryDB)

        except Exception as e:
            logger.error(f"Error getting URLs for {self.pluginName}: {e}")

        return urlList

    def runDataRetrievalTasks(self):
        """
        Execute data retrieval tasks.

        This method:
        1. Fetches URLs from the plugin queue
        2. Downloads and parses content
        3. Adds results to processing queue
        4. Handles shutdown signals gracefully
        """
        logger.info("Started data retrieval job for plugin: %s, queue size: %s",
                    self.pluginName, self.pluginObj.getQueueSize())

        check_interval = 5  # Check shutdown every 5 seconds during wait
        last_check = time.time()

        while (not self._checkShutdown()) or (not self.pluginObj.isQueueEmpty()) or \
                (self.pluginObj.pluginState == PluginTypes.STATE_GET_URL_LIST):
            # Check shutdown frequently
            if self._checkShutdown():
                logger.info(f"Worker {self.workerID} stopping due to shutdown")
                break

            # Periodic shutdown check
            if time.time() - last_check > check_interval:
                if self._checkShutdown():
                    logger.info(f"Worker {self.workerID} stopping data retrieval due to shutdown")
                    break
                last_check = time.time()

            sURL = None
            try:
                if self.pluginObj.isQueueEmpty():
                    logger.debug('%s: Waiting for input queue to fill up; plugin state = %s',
                                 self.pluginName,
                                 PluginTypes.decodeNameFromIntVal(self.pluginObj.pluginState))

                    # Wait in short intervals to check shutdown
                    wait_time = min(self.queueFillwaitTime, 10)
                    for _ in range(int(self.queueFillwaitTime / wait_time)):
                        if self._checkShutdown():
                            break
                        time.sleep(wait_time)

                    continue

                sURL = self.pluginObj.getNextItemFromFetchQueue(timeout=10)

                # Check for sentinel
                if sURL is None:
                    logger.info('Got queue end sentinel, stopping data retrieval for plugin %s',
                                self.pluginName)
                    self.pluginObj.clearQueue()
                    break

                # URLs are pre-filtered when loaded from database, no need to check again
                if True:  # Process all URLs from queue
                    logger.debug(f'{self.pluginName} started fetching URL: {sURL}')
                    fetchResult = self.pluginObj.fetchDataFromURL(sURL, self.workerID)

                    # Check for HTTP errors first
                    if fetchResult and hasattr(fetchResult, 'http_error') and fetchResult.http_error:
                        # Permanent HTTP error - save to database
                        if fetchResult.http_error.is_permanent:
                            self.sessionHistoryDB.addHTTPError(
                                sURL,
                                self.pluginName,
                                fetchResult.http_error.status_code,
                                fetchResult.http_error.message
                            )
                            logger.info(f'{self.pluginName}: Saved HTTP {fetchResult.http_error.status_code} error: {sURL}')
                    elif fetchResult is not None and fetchResult.wasSuccessful:
                        self.queue_manager.addToScrapeCompletedQueue(fetchResult)

                        # Handle additional links - only save to DB, don't add to current queue
                        if fetchResult.additionalLinks:
                            # Limit additional links to prevent overwhelming the system
                            max_additional_links = 100
                            limited_links = fetchResult.additionalLinks[:max_additional_links]

                            if len(fetchResult.additionalLinks) > max_additional_links:
                                logger.warning(f"{self.name}: Truncated {len(fetchResult.additionalLinks)} additional URLs to {max_additional_links}")

                            filtered_urls = self.session_history.removeAlreadyFetchedURLs(
                                limited_links,
                                self.plugin_name
                            )

                            if filtered_urls:
                                logger.info(f"{self.name}: Saving {len(filtered_urls)} additional URLs to pending (will process in next run)")
                                # Only save to DB for future processing, don't add to current queue
                                self.queue_manager.queueDBOperation(
                                    'add_pending',
                                    (filtered_urls, self.plugin_name),
                                    wait_for_result=False
                                )
                                # DO NOT add to plugin queue during current run
                    else:
                        # Queue failed URL to DB (async, no wait)
                        self.queue_manager.queueDBOperation(
                            'add_failed',
                            (sURL, self.pluginName, datetime.now()),
                            wait_for_result=False
                        )

            except queue.Empty:
                logger.debug("%s: Queue empty when trying to retrieve data", self.pluginName)

            except Exception as e:
                logger.error("%s: Error retrieving data: %s, url = %s",
                             self.pluginName, e, sURL)

        logger.info('Thread %s finished data retrieval for plugin %s',
                    self.workerID, self.pluginName)
        self.pluginObj.pluginState = PluginTypes.STATE_STOPPED

    def run(self):
        """
        Main thread execution method.

        Executes the appropriate task based on taskType.
        """
        try:
            if self.taskType == PluginTypes.TASK_GET_URL_LIST:
                self.runURLListGatherTasks()
            elif self.taskType == PluginTypes.TASK_GET_DATA:
                self.runDataRetrievalTasks()
        except Exception as e:
            logger.error(f"Worker {self.workerID} ({self.pluginName}) error: {e}")
        finally:
            logger.info(f"Worker {self.workerID} ({self.pluginName}) finished")


class DataProcessor(threading.Thread):
    """
    Worker thread for asynchronous data processing.

    Reads saved articles/data and processes them via data processing plugins.

    Attributes:
        dataProcPluginsMap (dict): Map of priorities to plugin objects
        sortedPriorityKeys (list): Sorted list of priorities
        waitTimeSec (int): Time to wait between queue checks
    """

    def __init__(self, dataProcPluginsMap: dict, sortedPriorityKeys: list,
                 queue_manager, queue_status, daemon=False, target=None, name=None):
        """
        Initialize the data processor.

        Args:
            dataProcPluginsMap (dict): Map of priorities to plugin objects
            sortedPriorityKeys (list): Sorted priorities
            queue_manager: Queue manager instance
            queue_status: Queue status tracker
            daemon (bool, optional): Whether this is a daemon thread
            target (callable, optional): Alternative method to run
            name (str, optional): Thread name
        """
        self.workerID = name
        self.dataProcPluginsMap = dataProcPluginsMap
        self.sortedPriorityKeys = sortedPriorityKeys
        self.queue_manager = queue_manager
        self.q_status = queue_status
        self.waitTimeSec = 5
        self.queueBlockTimeout = 2

        logger.debug("Data processor %s initialized with plugins: %s",
                     self.workerID, self.dataProcPluginsMap)
        super().__init__(daemon=daemon, target=target, name=name)

    @staticmethod
    def processItem(queue_manager, itemInQueue, sortedPriorityKeys: list,
                    dataProcPluginsMap: dict, workerID: str):
        """
        Process a single item through all data processing plugins.

        Args:
            queue_manager: Queue manager instance
            itemInQueue: Item to process
            sortedPriorityKeys (list): Sorted priorities
            dataProcPluginsMap (dict): Map of priorities to plugins
            workerID (str): Worker identifier
        """
        item_doc = None
        queue_manager.alreadyDataProcList.append(itemInQueue.URL)
        queue_manager.addToDataProcessedQueue(itemInQueue)

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
                logger.error(f"Data processor {workerID} plugin {thisPlugin.pluginName} error: " +
                             f"{pluginError}; file = {itemInQueue.savedDataFileName}, " +
                             f"URL = {itemInQueue.URL}")

    def run(self):
        """Main thread execution method for data processing."""
        itemInQueue = None
        self.q_status.updateStatus()
        check_interval = 5
        last_check = time.time()

        logger.info(f'Data processing thread {self.workerID} started')

        while True:
            # Exit if all content-fetching worker pairs are dead AND input queue is drained
            all_pairs_dead = not any(
                p.is_alive() for p in self.queue_manager.worker_pairs.values()
            )
            if all_pairs_dead and self.queue_manager.dataProcQueue.empty():
                logger.info(f"Data processor {self.workerID}: All worker pairs complete and queue empty - exiting")
                break

            # Legacy condition kept as secondary guard
            if not (self.q_status.isPluginStillFetchingoverNetwork or
                    self.q_status.dataInputQsize > 0):
                logger.info(f"Data processor {self.workerID}: isPluginStillFetching=False and queue empty - exiting")
                break

            # Periodic shutdown check
            if time.time() - last_check > check_interval:
                if self.queue_manager.shutdown_event.is_set():
                    logger.info(f"Data processor {self.workerID} stopping due to shutdown")
                    break
                last_check = time.time()

            try:
                itemInQueue = self.queue_manager.fetchFromDataProcInputQ(
                    block=True,
                    timeout=self.queueBlockTimeout
                )

                if itemInQueue is not None and \
                        itemInQueue.URL not in self.queue_manager.alreadyDataProcList:
                    DataProcessor.processItem(
                        self.queue_manager,
                        itemInQueue,
                        self.sortedPriorityKeys,
                        self.dataProcPluginsMap,
                        self.workerID
                    )
                else:
                    self.queue_manager.addToDataProcessedQueue(itemInQueue)
                    if itemInQueue is not None:
                        logger.debug('Data processor %s: Ignoring already processed file for URL %s',
                                     self.workerID, itemInQueue.URL)

            except queue.Empty:
                logger.debug('Data processor %s: Queue empty, size = %s',
                             self.workerID, self.queue_manager.getCompletedQueueSize())

            except Exception as e:
                logger.error(f"Data processor {self.workerID} error: {e}")

            try:
                time.sleep(self.waitTimeSec)
                self.q_status.updateStatus()
            except Exception as statusCheckError:
                logger.error("Data processor: Error checking plugin state: %s", statusCheckError)

        logger.info(f'Data processing thread {self.workerID} finished')





class WorkerPair:
    """
    Coordinates a pair of workers: URL discovery + content fetching.

    This class ensures proper lifecycle management:
    - URL worker starts immediately, times out after configured seconds
    - Content worker monitors URL worker status
    - Content worker terminates when queue is empty AND URL worker is done
    """

    def __init__(self, plugin, session_history, queue_manager, worker_id: str):
        """
        Initialize a coordinated worker pair.

        Args:
            plugin: Plugin instance to execute
            session_history: Database interface
            queue_manager: Queue manager instance
            worker_id: Unique identifier for this worker pair
        """
        self.plugin = plugin
        self.session_history = session_history
        self.queue_manager = queue_manager
        self.worker_id = worker_id
        self.plugin_name = type(plugin).__name__

        # Worker threads
        self.url_worker: Optional[URLDiscoveryWorker] = None
        self.content_worker: Optional[ContentFetchWorker] = None

        # Coordination flags
        self.url_discovery_complete = threading.Event()
        self.url_discovery_timeout = 600  # 10 minutes default

        # Statistics
        self.urls_discovered = 0
        self.urls_processed = 0

        logger.info(f"WorkerPair {worker_id} created for plugin {self.plugin_name}")

    def initialize(self, run_date: datetime, url_timeout: int = 600):
        """
        Initialize both workers in the pair.

        Args:
            run_date: Date for scraping
            url_timeout: Maximum seconds for URL discovery
        """
        self.url_discovery_timeout = url_timeout

        # Create URL discovery worker
        self.url_worker = URLDiscoveryWorker(
            self.plugin,
            self.session_history,
            self.queue_manager,
            self.url_discovery_complete,
            run_date,
            url_timeout,
            name=f"URL-{self.worker_id}"
        )

        # Create content fetch worker
        self.content_worker = ContentFetchWorker(
            self.plugin,
            self.session_history,
            self.queue_manager,
            self.url_discovery_complete,
            name=f"Fetch-{self.worker_id}"
        )

        logger.info(f"WorkerPair {self.worker_id} initialized with {url_timeout}s URL timeout")

    def start(self):
        """Start both workers in the pair."""
        if not self.url_worker or not self.content_worker:
            raise RuntimeError("Workers not initialized. Call initialize() first.")

        logger.info(f"Starting WorkerPair {self.worker_id}")

        # Start URL worker first
        self.url_worker.start()

        # Start content worker (it will wait for URLs)
        self.content_worker.start()

    def join(self, timeout: Optional[float] = None):
        """
        Wait for both workers to complete.

        Args:
            timeout: Maximum time to wait (None = indefinite)
        """
        if self.url_worker:
            self.url_worker.join(timeout=timeout)

        if self.content_worker:
            self.content_worker.join(timeout=timeout)

    def is_alive(self) -> bool:
        """Check if any worker in the pair is still running."""
        url_alive = self.url_worker.is_alive() if self.url_worker else False
        content_alive = self.content_worker.is_alive() if self.content_worker else False
        return url_alive or content_alive

    def get_status(self) -> dict:
        """Get status of both workers."""
        return {
            'worker_id': self.worker_id,
            'plugin': self.plugin_name,
            'url_worker_alive': self.url_worker.is_alive() if self.url_worker else False,
            'content_worker_alive': self.content_worker.is_alive() if self.content_worker else False,
            'url_discovery_complete': self.url_discovery_complete.is_set(),
            'queue_size': self.plugin.getQueueSize(),
            'total_urls': self.plugin.urlQueueTotalSize,
            'processed_urls': self.plugin.urlProcessedCount if hasattr(self.plugin, 'urlProcessedCount') else 0
        }


class URLDiscoveryWorker(threading.Thread):
    """
    Worker thread that discovers URLs from news sources.

    Features:
    - Starts immediately
    - Times out after configured seconds
    - Signals completion to content worker
    """

    def __init__(self, plugin, session_history, queue_manager,
                 completion_event: threading.Event, run_date: datetime,
                 timeout: int, name: str):
        """
        Initialize URL discovery worker.

        Args:
            plugin: Plugin instance
            session_history: Database interface
            queue_manager: Queue manager
            completion_event: Event to signal when discovery is complete
            run_date: Date for scraping
            timeout: Maximum seconds for URL discovery
            name: Thread name
        """
        self.plugin = plugin
        self.plugin_name = type(plugin).__name__
        super().__init__(name=self.plugin_name, daemon=False)
        self.session_history = session_history
        self.queue_manager = queue_manager
        self.completion_event = completion_event
        self.run_date = run_date
        self.timeout = timeout
        self.start_time = None

        logger.debug(f"URLDiscoveryWorker {name} initialized")

    def run(self):
        """Main execution method."""
        self.start_time = time.time()

        logger.info(f"{self.name}: Starting URL discovery (timeout: {self.timeout}s)")

        try:
            # Check shutdown before starting
            if self.queue_manager.shutdown_event.is_set():
                logger.info(f"{self.name}: Shutdown detected before start")
                return

            # STEP 1: Retrieve pending URLs from database
            logger.info(f"{self.name}: Retrieving pending URLs from database...")
            pending_urls = []
            try:
                pending_urls = self.session_history.retrieveTodoURLList(self.plugin_name)
                if pending_urls:
                    logger.info(f"{self.name}: Retrieved {len(pending_urls)} pending URLs from database")
                    # CRITICAL: Don't add pending URLs to queue during discovery
                    # They should only be processed in dedicated content-only runs
                    # For now, log and skip to focus on new URL discovery
                    logger.warning(f"{self.name}: Skipping {len(pending_urls)} pending URLs - they will be processed in next run without URL discovery")
                    # DO NOT ADD: for url in pending_urls: self.plugin.urlQueue.put(url)
            except Exception as e:
                logger.error(f"{self.name}: Error retrieving pending URLs: {e}")

            # STEP 2: Discover new URLs with timeout monitoring
            urls = self._discover_urls_with_timeout()

            if urls and not self.queue_manager.shutdown_event.is_set():
                # Add URLs to plugin queue
                logger.info(f"{self.name}: Adding {len(urls)} newly discovered URLs to queue")

                # Queue DB operation for pending URLs
                self.queue_manager.queueDBOperation(
                    'add_pending',
                    (urls, self.plugin_name),
                    wait_for_result=False
                )

                # Add to plugin's fetch queue
                self.plugin.addURLsListToQueue(urls, self.session_history)

            # Signal completion and add end marker
            if not self.queue_manager.shutdown_event.is_set():
                try:
                    self.plugin.putQueueEndMarker()
                except Exception as e:
                    logger.error(f"{self.name}: Error putting queue end marker: {e}")

        except Exception as e:
            logger.error(f"{self.name}: Error during URL discovery: {e}")
            # Ensure end marker is placed even on error
            try:
                self.plugin.putQueueEndMarker()
            except:
                pass

        finally:
            # Always signal completion
            elapsed = time.time() - self.start_time
            logger.info(f"{self.name}: URL discovery complete (elapsed: {elapsed:.1f}s)")
            self.completion_event.set()

    def _discover_urls_with_timeout(self) -> list:
        """
        Discover URLs with timeout checking.

        Returns:
            list: Discovered URLs
        """
        urls = []

        try:
            # Check if plugin supports URL discovery
            if not hasattr(self.plugin, 'getURLsListForDate'):
                logger.warning(f"{self.name}: Plugin does not support URL discovery")
                return urls

            # Periodic timeout check wrapper
            check_interval = 5  # Check every 5 seconds

            # Start discovery in a separate thread to allow timeout
            discovery_complete = threading.Event()
            discovered_urls = []
            discovery_error = [None]  # List to allow modification in nested function

            def discover():
                try:
                    logger.info(f"{self.name}: Starting URL discovery call...")
                    start_time = time.time()
                    result = self.plugin.getURLsListForDate(
                        self.run_date,
                        self.session_history
                    )
                    elapsed = time.time() - start_time
                    logger.info(f"{self.name}: URL discovery call completed in {elapsed:.1f}s, found {len(result) if result else 0} URLs")

                    discovered_urls.extend(result if result else [])
                except Exception as e:
                    discovery_error[0] = e
                finally:
                    discovery_complete.set()

            discovery_thread = threading.Thread(target=discover, daemon=False)
            discovery_thread.start()

            # Wait with timeout checking
            elapsed = 0
            while elapsed < self.timeout:
                # Check if discovery completed
                if discovery_complete.wait(timeout=check_interval):
                    # Discovery completed normally
                    logger.info(f"{self.name}: URL discovery completed successfully")
                    break

                elapsed = time.time() - self.start_time

                # Check for shutdown
                if self.queue_manager.shutdown_event.is_set():
                    logger.warning(f"{self.name}: Shutdown during URL discovery")
                    self.plugin.is_stopped = True
                    break

                # Check for timeout
                if elapsed >= self.timeout:
                    logger.warning(f"{self.name}: URL discovery timeout reached ({self.timeout}s)")
                    logger.warning(f"{self.name}: Forcing discovery complete with {len(discovered_urls)} URLs found")
                    self.plugin.is_stopped = True
                    break

            # CRITICAL: If thread is still running, give it 2 more seconds then force-stop
            if discovery_thread.is_alive():
                logger.warning(f"{self.name}: Discovery thread still running, waiting 2 seconds...")
                discovery_thread.join(timeout=2)
                if discovery_thread.is_alive():
                    logger.error(f"{self.name}: Discovery thread did not stop, but continuing anyway")

            # Wait a bit for thread to finish
            discovery_thread.join(timeout=1)

            if discovery_error[0]:
                logger.error(f"{self.name}: Discovery error: {discovery_error[0]}")

            urls = discovered_urls

        except Exception as e:
            logger.error(f"{self.name}: Error in URL discovery: {e}")

        return urls


class ContentFetchWorker(threading.Thread):
    """
    Worker thread that fetches content from discovered URLs.

    Features:
    - Monitors URL discovery worker status
    - Processes URLs as they arrive
    - Terminates when queue empty AND URL discovery complete
    """

    def __init__(self, plugin, session_history, queue_manager,
                 url_discovery_complete: threading.Event, name: str):
        """
        Initialize content fetch worker.

        Args:
            plugin: Plugin instance
            session_history: Database interface
            queue_manager: Queue manager
            url_discovery_complete: Event indicating URL discovery is done
            name: Thread name
        """
        self.plugin = plugin
        self.plugin_name = type(plugin).__name__
        super().__init__(name=self.plugin_name, daemon=False)

        self.session_history = session_history
        self.queue_manager = queue_manager
        self.url_discovery_complete = url_discovery_complete

        self.queue_check_interval = 2  # Check queue every 2 seconds
        self.shutdown_check_interval = 1  # Check shutdown every second

        logger.debug(f"ContentFetchWorker {name} initialized")

    def run(self):
        """Main execution method."""
        logger.info(f"{self.name}: Starting content fetching")

        consecutive_empty_checks = 0
        max_empty_checks = 5  # Exit after 5 consecutive empty queue checks when discovery is done

        try:
            while True:
                # Check shutdown
                if self.queue_manager.shutdown_event.is_set():
                    logger.info(f"{self.name}: Shutdown signal received")
                    break

                # Try to get URL from queue
                try:
                    url = self.plugin.getNextItemFromFetchQueue(timeout=self.queue_check_interval)

                    # Check for sentinel
                    if url is None:
                        logger.info(f"{self.name}: Received queue end marker")
                        break

                    # Reset empty counter - we got a URL
                    consecutive_empty_checks = 0

                    # Process URL
                    self._process_url(url)

                except queue.Empty:
                    # Queue is empty - check conditions
                    if self.url_discovery_complete.is_set():
                        consecutive_empty_checks += 1
                        logger.debug(f"{self.name}: Queue empty, discovery complete, check {consecutive_empty_checks}/{max_empty_checks}")

                        if consecutive_empty_checks >= max_empty_checks:
                            logger.info(f"{self.name}: Queue empty and URL discovery complete after {max_empty_checks} checks - stopping")
                            break
                    else:
                        # URL discovery still running, reset counter and wait
                        consecutive_empty_checks = 0
                        logger.debug(f"{self.name}: Queue empty but URL discovery still running, waiting...")
                        continue

                except Exception as e:
                    logger.error(f"{self.name}: Error getting URL from queue: {e}")
                    time.sleep(1)

        except Exception as e:
            logger.error(f"{self.name}: Error during content fetching: {e}")

        finally:
            # Update plugin state
            self.plugin.pluginState = PluginTypes.STATE_STOPPED
            queue_size = self.plugin.urlQueue.qsize()
            logger.info(f"{self.name}: Content fetching complete. Final queue size: {queue_size}")

    def _should_stop(self) -> bool:
        """
        Determine if worker should stop.

        Returns:
            bool: True if should stop, False otherwise
        """
        # This method is no longer used - logic moved to run()
        # Keeping for backward compatibility
        return (
                self.url_discovery_complete.is_set() and
                self.plugin.isQueueEmpty()
        )

    def _process_url(self, url: str):
        """
        Process a single URL.

        Args:
            url: URL to fetch and process
        """
        try:
            # skip Check if already attempted
            # if self.session_history.url_was_attempted(url, self.plugin_name):
            #     logger.debug(f"{self.name}: URL already attempted: {url}")
            #     return

            # Fetch content
            logger.debug(f"{self.name}: Fetching URL: {url}")
            # Diagnostic logging every 50 URLs
            self.plugin.urlProcessedCount = getattr(self.plugin, 'urlProcessedCount', 0) + 1
            if self.plugin.urlProcessedCount % 50 == 0:
                queue_remaining = self.plugin.urlQueue.qsize()
                logger.info(f"{self.name}: Processed {self.plugin.urlProcessedCount} URLs, "
                            f"{queue_remaining} remaining in queue, "
                            f"Discovery complete: {self.url_discovery_complete.is_set()}")
            fetch_result = self.plugin.fetchDataFromURL(url, self.name)

            if fetch_result:
                # Handle HTTP errors
                if hasattr(fetch_result, 'http_error') and fetch_result.http_error:
                    if fetch_result.http_error.is_permanent:
                        self.session_history.addHTTPError(
                            url,
                            self.plugin_name,
                            fetch_result.http_error.status_code,
                            fetch_result.http_error.message
                        )
                        logger.info(f"{self.name}: HTTP {fetch_result.http_error.status_code}: {url}")
                    return

                # Handle successful fetch
                if fetch_result.wasSuccessful:
                    self.queue_manager.addToScrapeCompletedQueue(fetch_result)

                    # Handle additional links
                    if fetch_result.additionalLinks:
                        filtered_urls = self.session_history.removeAlreadyFetchedURLs(
                            fetch_result.additionalLinks,
                            self.plugin_name
                        )

                        if filtered_urls:
                            logger.debug(f"{self.name}: Adding {len(filtered_urls)} additional URLs")
                            self.queue_manager.queueDBOperation(
                                'add_pending',
                                (filtered_urls, self.plugin_name),
                                wait_for_result=False
                            )
                else:
                    # Failed fetch
                    self.queue_manager.queueDBOperation(
                        'add_failed',
                        (url, self.plugin_name, datetime.now()),
                        wait_for_result=False
                    )

        except Exception as e:
            logger.error(f"{self.name}: Error processing URL {url}: {e}")



class ProgressWatcher(threading.Thread):
    """
    Worker thread to monitor progress and save to database asynchronously.

    This thread:
    - Monitors scraping progress
    - Updates progress bars
    - Saves completed URLs to database
    - Can optionally provide REST API status endpoint

    Attributes:
        refreshIntervalSecs (int): How often to update progress
        countWrittenToDB (int): Count of URLs saved to database
    """

    def __init__(self, pluginNameObjMap: dict, sessionHistoryDB,
                 queue_manager, queue_status, app_config,
                 enlighten_manager=None,
                 daemon=None, target=None, name=None):
        """
        Initialize the progress watcher.

        Args:
            pluginNameObjMap (dict): Map of plugin names to objects
            sessionHistoryDB: Database interface
            queue_manager: Queue manager instance
            queue_status: Queue status tracker
            app_config: Application configuration
            daemon (bool, optional): Whether this is a daemon thread
            target (callable, optional): Alternative method to run
            name (str, optional): Thread name
        """
        self.workerID = name
        self.pluginNameObjMap = pluginNameObjMap
        self.historyDB = sessionHistoryDB
        self.queue_manager = queue_manager
        self.q_status = queue_status
        self.refreshIntervalSecs = app_config.progressRefreshInt
        self.refreshWaitCountForLog = 24
        self.previousState = dict()
        self.currentState = dict()
        self.countWrittenToDB = 0

        # Progress bars are set externally (from main thread)
        self.urlListFtBar = None
        self.urlScrapeBar = None
        self.dataProcsBar = None

        logger.debug("Progress watcher thread initialized")
        super().__init__(daemon=daemon, target=target, name=name)

    def set_progress_bars(self, urlListBar, urlScrapeBar, dataProcsBar):
        """Set progress bars (must be called from main thread before starting)."""
        self.urlListFtBar = urlListBar
        self.urlScrapeBar = urlScrapeBar
        self.dataProcsBar = dataProcsBar

    def run(self):
        """Main execution method for progress monitoring."""
        self.countWrittenToDB = 0
        log_event_update_cnt = 0
        fetchCompletedCount = 0

        logger.info('Progress watcher thread started.')

        # Verify bars were set
        if not all([self.urlListFtBar, self.urlScrapeBar, self.dataProcsBar]):
            logger.error("Progress bars not set! Cannot monitor progress.")
            return

        try:
            self.q_status.updateStatus()
            previousState = copy.deepcopy(self.q_status.currentState)

            # Initial progress bar update
            self.urlListFtBar.update(
                incr=self.q_status.totalPluginsURLSourcing - self.q_status.countOfPluginsInURLSrcState
            )
            self.urlScrapeBar.total = max(self.q_status.totalURLCount, 1)
            self.urlScrapeBar.update(incr=(self.q_status.totalURLCount - self.q_status.fetchPendingCount))

            total_data = max(self.q_status.dataInputQsize + self.q_status.dataOutputQsize, 1)
            self.dataProcsBar.total = total_data
            self.dataProcsBar.update(incr=self.q_status.dataOutputQsize)
            prevCountOfDataProcessed = self.q_status.dataOutputQsize

            # Main monitoring loop
            while ((self.q_status.isPluginStillFetchingoverNetwork or self.q_status.dataInputQsize > 0)
                   and not self.queue_manager.shutdown_event.is_set()):

                # Check for shutdown
                if self.queue_manager.shutdown_event.wait(timeout=0.5):
                    logger.info("Progress watcher stopping due to shutdown")
                    break

                # Process completed URLs from queue
                results_from_queue = []
                try:
                    while not self.queue_manager.isFetchQEmpty():
                        results_from_queue.append(
                            self.queue_manager.getFetchResultFromQueue(timeout=0.5)
                        )
                except queue.Empty:
                    pass

                # Queue DB write operation
                if results_from_queue:
                    count_written = self.queue_manager.queueDBOperation(
                        'write_queue',
                        results_from_queue,
                        wait_for_result=True
                    )
                    if count_written:
                        self.countWrittenToDB += count_written

                prevURLListPluginPending = self.q_status.countOfPluginsInURLSrcState

                # Wait before next check
                for _ in range(self.refreshIntervalSecs):
                    if self.queue_manager.shutdown_event.wait(timeout=1):
                        break

                self.q_status.updateStatus()

                # Update progress bars
                self.urlScrapeBar.total = max(self.q_status.totalURLCount, 1)

                current_total = max(self.q_status.dataInputQsize + self.q_status.dataOutputQsize, 1)
                if current_total != self.dataProcsBar.total:
                    self.dataProcsBar.total = current_total

                # Update URL list progress
                plugin_progress = prevURLListPluginPending - self.q_status.countOfPluginsInURLSrcState
                if plugin_progress > 0:
                    self.urlListFtBar.update(incr=plugin_progress)

                # Update scrape progress
                prevCompletedURLCount = fetchCompletedCount
                fetchCompletedCount = (self.q_status.totalURLCount - self.q_status.fetchPendingCount)
                scrape_increment = fetchCompletedCount - prevCompletedURLCount
                if scrape_increment > 0:
                    self.urlScrapeBar.update(incr=scrape_increment)

                # Update data processing progress
                current_processed = self.q_status.dataOutputQsize
                increment = current_processed - prevCountOfDataProcessed
                if increment > 0:
                    self.dataProcsBar.update(incr=increment)
                prevCountOfDataProcessed = current_processed

                # Periodic logging
                if log_event_update_cnt > self.refreshWaitCountForLog:
                    log_event_update_cnt = 0
                    logger.info(f"URLs: {self.q_status.totalURLCount}, " +
                                f"Completed: {self.q_status.fetchCompletCount}, " +
                                f"Processed: {self.q_status.dataOutputQsize}")

                    for statusMessage in QueueStatus.getStatusChange(previousState, self.q_status.currentState):
                        logger.info(statusMessage)

                    previousState = copy.deepcopy(self.q_status.currentState)

                log_event_update_cnt += 1

            logger.info('Progress watcher: Saved %s URLs to history database', self.countWrittenToDB)

        except Exception as e:
            logger.error("Progress watcher error: %s", e, exc_info=True)

        finally:
            logger.info("Progress watcher thread finished")


class StatusAPIServer:
    """
    FastAPI-based status server for NewsLookout.
    REST API Status Endpoint using FastAPI
    Provides real-time status information via HTTP GET /status endpoint.
    Returns JSON with detailed statistics about all workers, queues, and progress.
    """

    def __init__(self, queue_manager, host: str = "0.0.0.0", port: int = 8080):
        """
        Initialize the status API server.

        Args:
            queue_manager: QueueManager instance
            host (str): Host to bind to
            port (int): Port to bind to
        """

        self.queue_manager = queue_manager
        self.host = host
        self.port = port
        self.app = FastAPI(title="NewsLookout Status API", version="3.0.0")
        self.server_thread = None
        self.server = None

        # Setup routes
        self._setup_routes()

    def _setup_routes(self):
        """Setup FastAPI routes."""

        @self.app.get("/")
        async def root():
            """Root endpoint with API information."""
            return {
                "service": "NewsLookout Status API",
                "version": "3.0.0",
                "endpoints": {
                    "/status": "Get detailed application status",
                    "/status/summary": "Get summary statistics",
                    "/health": "Health check endpoint"
                }
            }

        @self.app.get("/dashboard.html", response_class=HTMLResponse)
        async def get_dashboard():
            # Assuming dashboard.html is in the same directory as your main.py
            html_file = Path(__file__).parent / "dashboard.html"
            html_content = html_file.read_text()
            logger.info(f"Serving the dashboard with content of length: {len(html_content)}")
            return HTMLResponse(content=html_content)

        @self.app.get("/health")
        async def health():
            """Health check endpoint."""
            return {"status": "healthy", "timestamp": datetime.now().isoformat()}

        @self.app.get("/status")
        async def get_status():
            """Get comprehensive status."""
            return JSONResponse(content=self._get_comprehensive_status())

        @self.app.get("/status/summary")
        async def get_summary():
            """Get summary statistics."""
            return JSONResponse(content=self._get_summary_status())

    def _get_comprehensive_status(self) -> Dict[str, Any]:
        """
        Generate comprehensive status report.

        Returns:
            dict: Complete status information
        """
        status = {
            "timestamp": datetime.now().isoformat(),
            "application": {
                "name": "NewsLookout",
                "version": "3.0.0",
                "is_running": not self.queue_manager.shutdown_event.is_set()
            },
            "plugins": self._get_plugins_status(),
            "queues": self._get_queues_status(),
            "workers": self._get_workers_status(),
            "database": self._get_database_status(),
            "performance": self._get_performance_metrics()
        }

        return status

    def _get_summary_status(self) -> Dict[str, Any]:
        """Generate summary status."""
        q_status = self.queue_manager.q_status
        q_status.updateStatus()

        return {
            "timestamp": datetime.now().isoformat(),
            "summary": {
                "total_urls_discovered": q_status.totalURLCount,
                "urls_completed": q_status.fetchCompletCount,
                "urls_pending": q_status.fetchPendingCount,
                "data_processed": q_status.dataOutputQsize,
                "plugins_url_sourcing": q_status.countOfPluginsInURLSrcState,
                "total_plugins": len(self.queue_manager.pluginNameToObjMap),
                "is_running": not self.queue_manager.shutdown_event.is_set()
            }
        }

    def _get_plugins_status(self) -> dict:
        """Get status of all plugins - Updated for WorkerPair architecture."""
        from data_structs import PluginTypes

        plugins_status = {
            "content_plugins": [],
            "data_processing": []
        }

        for plugin_name, plugin in self.queue_manager.pluginNameToObjMap.items():
            plugin_info = {
                "name": plugin_name,
                "state": PluginTypes.decodeNameFromIntVal(plugin.pluginState) if hasattr(plugin, 'pluginState') else 'UNKNOWN',
                "priority": plugin.executionPriority
            }

            # Add URL/queue info for content plugins
            if plugin.pluginType in [PluginTypes.MODULE_NEWS_CONTENT,
                                     PluginTypes.MODULE_NEWS_AGGREGATOR,
                                     PluginTypes.MODULE_DATA_CONTENT,
                                     PluginTypes.MODULE_NEWS_API]:
                plugin_info.update({
                    "total_urls": plugin.urlQueueTotalSize if hasattr(plugin, 'urlQueueTotalSize') else 0,
                    "pending_urls": plugin.urlQueue.qsize() if hasattr(plugin, 'urlQueue') else 0,
                    "processed_urls": plugin.urlProcessedCount if hasattr(plugin, 'urlProcessedCount') else 0,
                    "discovered_urls": plugin.urlQueueTotalSize if hasattr(plugin, 'urlQueueTotalSize') else 0,
                })

                # Add worker pair status if available
                if hasattr(self.queue_manager, 'worker_pairs') and plugin_name in self.queue_manager.worker_pairs:
                    pair = self.queue_manager.worker_pairs[plugin_name]
                    plugin_info['worker_pair'] = {
                        'url_discovery_complete': pair.url_discovery_complete.is_set(),
                        'url_worker_alive': pair.url_worker.is_alive() if pair.url_worker else False,
                        'content_worker_alive': pair.content_worker.is_alive() if pair.content_worker else False
                    }

                plugins_status["content_plugins"].append(plugin_info)

            elif plugin.pluginType == PluginTypes.MODULE_DATA_PROCESSOR:
                plugins_status["data_processing"].append(plugin_info)

        return plugins_status

    def _get_queues_status(self) -> Dict[str, Any]:
        """Get status of all queues."""
        return {
            "fetch_completed": {
                "size": self.queue_manager.fetchCompletedQueue.qsize(),
                "total_processed": self.queue_manager.fetchCompletedCount
            },
            "data_processing_input": {
                "size": self.queue_manager.dataProcQueue.qsize()
            },
            "data_processing_output": {
                "size": self.queue_manager.dataProcCompletedQueue.qsize()
            },
            "database_operations": {
                "size": self.queue_manager.dbCommandQueue.qsize()
            }
        }

    def _get_workers_status(self) -> dict:
        """Get status of all workers - Updated for WorkerPair architecture."""
        workers_status = {
            "worker_pairs": [],
            "data_processing_workers": []
        }

        # Worker pairs (replaces separate URL and content workers)
        if hasattr(self.queue_manager, 'worker_pairs'):
            for plugin_name, pair in self.queue_manager.worker_pairs.items():
                try:
                    pair_status = pair.get_status()
                    workers_status["worker_pairs"].append(pair_status)
                except Exception as e:
                    workers_status["worker_pairs"].append({
                        "plugin": plugin_name,
                        "error": str(e)
                    })

        # Data processing workers (unchanged)
        if hasattr(self.queue_manager, 'dataProcessWorkerList'):
            for worker in self.queue_manager.dataProcessWorkerList:
                workers_status["data_processing_workers"].append({
                    "id": getattr(worker, 'workerID', 'unknown'),
                    "is_alive": worker.is_alive() if hasattr(worker, 'is_alive') else False
                })

        return workers_status

    def _get_database_status(self) -> Dict[str, Any]:
        """Get database statistics."""
        try:
            stats = self.queue_manager.sessionHistoryDB.getHTTPErrorStats()
            return {
                "connection_status": "connected",
                "http_errors": stats
            }
        except Exception as e:
            return {
                "connection_status": "error",
                "error": str(e)
            }

    def _get_performance_metrics(self) -> Dict[str, Any]:
        """Calculate performance metrics."""
        q_status = self.queue_manager.q_status
        q_status.updateStatus()

        # Calculate speeds and estimates
        metrics = {
            "url_discovery": {
                "plugins_completed": q_status.totalPluginsURLSourcing - q_status.countOfPluginsInURLSrcState,
                "plugins_total": q_status.totalPluginsURLSourcing,
                "progress_percent": 0
            },
            "content_fetching": {
                "completed": q_status.fetchCompletCount,
                "total": q_status.totalURLCount,
                "pending": q_status.fetchPendingCount,
                "progress_percent": 0,
                "speed_urls_per_hour": 0,
                "estimated_completion": None
            },
            "data_processing": {
                "completed": q_status.dataOutputQsize,
                "total": q_status.dataInputQsize + q_status.dataOutputQsize,
                "progress_percent": 0,
                "speed_items_per_hour": 0
            }
        }

        # Calculate progress percentages
        if q_status.totalPluginsURLSourcing > 0:
            metrics["url_discovery"]["progress_percent"] = round(
                ((q_status.totalPluginsURLSourcing - q_status.countOfPluginsInURLSrcState) /
                 q_status.totalPluginsURLSourcing) * 100, 2
            )

        if q_status.totalURLCount > 0:
            metrics["content_fetching"]["progress_percent"] = round(
                (q_status.fetchCompletCount / q_status.totalURLCount) * 100, 2
            )

        total_data = q_status.dataInputQsize + q_status.dataOutputQsize
        if total_data > 0:
            metrics["data_processing"]["progress_percent"] = round(
                (q_status.dataOutputQsize / total_data) * 100, 2
            )

        # Estimate completion time
        # This is a simplified estimate - you might want to track actual start time
        if q_status.fetchCompletCount > 0 and q_status.fetchPendingCount > 0:
            # Assume we've been running for some time
            # You should track actual start_time for more accurate estimates
            avg_speed = 100  # Placeholder - calculate from actual metrics
            metrics["content_fetching"]["speed_urls_per_hour"] = avg_speed

            hours_remaining = q_status.fetchPendingCount / max(avg_speed, 1)
            estimated_completion = datetime.now() + timedelta(hours=hours_remaining)
            metrics["content_fetching"]["estimated_completion"] = estimated_completion.isoformat()

        return metrics

    def start(self):
        """Start the API server in a background thread with proper error handling."""
        def run_server():
            try:
                config = uvicorn.Config(
                    self.app,
                    host=self.host,
                    port=self.port,
                    log_level="warning",
                    access_log=False
                )
                self.server = uvicorn.Server(config)
                logger.info(f"Starting FastAPI status server on http://{self.host}:{self.port}")
                self.server.run()
            except Exception as e:
                logger.error(f"FastAPI server failed to start: {e}")
                print(f"\n✗ Status API failed to start: {e}")

        self.server_thread = threading.Thread(target=run_server, daemon=True, name="StatusAPI")
        self.server_thread.start()

        # Wait a bit to ensure server started
        time.sleep(2)

        # Verify server is running
        try:
            import socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex((self.host, self.port))
            sock.close()
            if result == 0:
                logger.info(f"Status API confirmed running at http://{self.host}:{self.port}/status")
            else:
                logger.warning(f"Status API may not be running on port {self.port}")
                print(f"⚠ Status API verification failed")
        except Exception as e:
            logger.error(f"Error verifying Status API: {e}")

    def stop(self):
        """Stop the API server."""
        if self.server:
            logger.info("Stopping FastAPI status server...")
            self.server.should_exit = True
            if self.server_thread:
                self.server_thread.join(timeout=5)

# End of file