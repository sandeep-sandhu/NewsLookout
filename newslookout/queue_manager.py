#!/usr/bin/env python
# -*- coding: utf-8 -*-

# #########################################################################################################
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

"""
 Queue Manager Module
 File name: queue_manager.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-06-23
 Purpose: Manage worker threads and the job queues of all the scraper plugins for the application
 This module manages worker threads and job queues for all scraper plugins.
 Copyright 2026, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu, sandeep.sandhu@gmx.com

 Provides:
    QueueManager
        config
        initPlugins
        initURLSourcingWorkers
        initContentFetchWorkers
        initDataProcWorkers
        runAllJobs
        startAllModules
        finishAllTasks
        processDataSynchronously
"""

import importlib
import os
from datetime import datetime
import multiprocessing
import threading
import queue
import time
import signal
import sys
import logging

from data_structs import PluginTypes, QueueStatus
from session_hist import SessionHistory
from worker import PluginWorker, ProgressWatcher, DataProcessor, StatusAPIServer
from config import ConfigManager

logger = logging.getLogger(__name__)


class QueueManager:
    """
    The Queue manager class runs the main processing of the application.

    It launches and manages worker threads to launch different web scraping processes,
    and saves all results from these threads.

    Attributes:
        app_config (ConfigManager): Application configuration
        runDate (datetime): Date for which data is being scraped
        pluginNameToObjMap (dict): Map of plugin names to plugin objects
        urlDiscoveryQueue (queue.Queue): Queue for newly discovered URLs
        dbCommandQueue (queue.Queue): Queue for database operations
        shutdown_event (threading.Event): Event to signal shutdown to all threads
    """

    def __init__(self):
        """Initialize the QueueManager with queues and default values."""
        self.app_config = None
        self.runDate = datetime.now()
        self.available_cores = 1
        self.fetchCycleTime = 120
        self.fetchCompletedCount = 0
        self.totalPluginsURLSrcCount = 0
        self.q_status = None
        self.status_api = None
        self.shutdown_handler = None

        # Plugin management
        self.pluginNameToObjMap = dict()
        self.dataProcPluginsMap = {}
        self.allowedDomainsList = []
        self.domainToPluginMap = {}

        # Worker threads
        self.urlSrcWorkers = dict()
        self.contentFetchWorkers = dict()
        self.dataProcessWorkerList = []
        self.dataproc_threads = 5
        self.progressWatchThread = None
        self.dbWorkerThread = None

        # Synchronization
        self.dbAccessSemaphore = None
        self.sessionHistoryDB = None
        self.shutdown_event = threading.Event()

        # Queues
        self.fetchCompletedQueue = queue.Queue()
        self.dataProcQueue = queue.Queue()
        self.dataProcCompletedQueue = queue.Queue()
        self.alreadyDataProcList = []
        self.URL_frontier = dict()

        # NEW: Queue for URL discovery - decouples URL finding from URL fetching
        self.urlDiscoveryQueue = queue.Queue()

        # NEW: Queue for database operations
        self.dbCommandQueue = queue.Queue()

        # NEW: Max wait time for URL gathering (configurable)
        self.url_gathering_timeout = 600  # 10 minutes default

        self.q_status = QueueStatus(self)

    def config(self, app_config: ConfigManager):
        """
        Configure the queue manager with application settings.

        Args:
            app_config (ConfigManager): Application configuration object
        """
        self.app_config = app_config
        try:
            logger.debug("Configuring the queue manager")
            # Install signal handler
            self.shutdown_handler = GracefulShutdownHandler(self)

            if self.app_config.rest_api_enabled:
                self.status_api = StatusAPIServer(
                    self,
                    host=self.app_config.rest_api_host,
                    port=self.app_config.rest_api_port
                )
            self.available_cores = multiprocessing.cpu_count()
            self.runDate = self.app_config.rundate
            self.fetchCycleTime = max(60, (
                    int(self.app_config.retry_wait_rand_max_sec) +
                    int(self.app_config.retry_wait_sec) +
                    int(self.app_config.connect_timeout) +
                    int(self.app_config.fetch_timeout)
            ))

            # Read URL gathering timeout from config (default 10 minutes)
            self.url_gathering_timeout = self.app_config.checkAndSanitizeConfigInt(
                'operation',
                'url_gathering_timeout',
                default=600,
                maxValue=3600,
                minValue=60
            )
            logger.info(f"URL gathering timeout set to {self.url_gathering_timeout} seconds")

        except Exception as e:
            logger.error("Exception when configuring the queue manager: %s", e)

        self.dbAccessSemaphore = threading.Semaphore()

        # Initialize session history
        self.sessionHistoryDB = SessionHistory(
            self.app_config.completed_urls_datafile,
            self.dbAccessSemaphore
        )
        self.sessionHistoryDB.printDBStats()

        # Start the dedicated database worker thread
        self._startDatabaseWorker()

    def _startDatabaseWorker(self):
        """
        Start a dedicated database worker thread.

        This thread handles all database operations to prevent concurrent access issues.
        All DB operations should be queued and handled by this single thread.
        """
        self.dbWorkerThread = threading.Thread(
            target=self._databaseWorkerLoop,
            name="DatabaseWorker",
            daemon=False
        )
        self.dbWorkerThread.start()
        logger.info("Started dedicated database worker thread")

    def _databaseWorkerLoop(self):
        """
        Main loop for the database worker thread.

        Processes database commands from the queue until shutdown is signaled.
        Command format: (operation, args, result_queue)

        NOTE: Only write operations go through this queue for serialization.
        Read operations (url_was_attempted, removeAlreadyFetchedURLs) are called
        directly for performance since SQLite WAL mode handles concurrent reads.
        """
        logger.info("Database worker thread started")

        while not self.shutdown_event.is_set():
            try:
                # Wait for DB commands with timeout to check shutdown periodically
                cmd = self.dbCommandQueue.get(timeout=1)

                if cmd is None:  # Poison pill to stop the thread
                    break

                operation, args, result_queue = cmd

                try:
                    if operation == 'write_queue':
                        result = self.sessionHistoryDB.writeQueueToDB(args)
                        if result_queue:
                            result_queue.put(result)

                    elif operation == 'add_pending':
                        url_list, plugin_name = args
                        self.sessionHistoryDB.addURLsToPendingTable(url_list, plugin_name)
                        if result_queue:
                            result_queue.put(True)

                    elif operation == 'add_failed':
                        fetch_result, plugin_name, fail_time = args
                        self.sessionHistoryDB.addURLToFailedTable(fetch_result, plugin_name, fail_time)
                        if result_queue:
                            result_queue.put(True)

                    elif operation == 'retrieve_pending':
                        plugin_name = args
                        result = self.sessionHistoryDB.retrieveTodoURLList(plugin_name)
                        if result_queue:
                            result_queue.put(result)

                    else:
                        logger.error(f"Unknown database operation: {operation}")

                except Exception as e:
                    logger.error(f"Error executing database operation '{operation}': {e}")
                    if result_queue:
                        result_queue.put(None)

                finally:
                    self.dbCommandQueue.task_done()

            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Database worker error: {e}")

        logger.info("Database worker thread stopped")

    def queueDBOperation(self, operation: str, args, wait_for_result=False):
        """
        Queue a database operation to be executed by the DB worker thread.

        Args:
            operation (str): Operation type ('write_queue', 'add_pending', etc.)
            args: Arguments for the operation
            wait_for_result (bool): Whether to wait for and return the result

        Returns:
            Result of the operation if wait_for_result=True, else None
        """
        result_queue = queue.Queue() if wait_for_result else None
        self.dbCommandQueue.put((operation, args, result_queue))

        if wait_for_result:
            try:
                return result_queue.get(timeout=60)
            except queue.Empty:
                logger.error(f"Timeout waiting for DB operation: {operation}")
                return None
        return None

    def getFetchResultFromQueue(self, block: bool = True, timeout: int = 30):
        """Get a fetch result from the completed queue."""
        resultObj = self.fetchCompletedQueue.get(block=block, timeout=timeout)
        self.fetchCompletedQueue.task_done()
        return resultObj

    def isFetchQEmpty(self) -> bool:
        """Check if fetch queue is empty."""
        return self.fetchCompletedQueue.empty()

    def isDataProcInputQEmpty(self) -> bool:
        """Check if data processing input queue is empty."""
        return self.dataProcQueue.empty()

    def getCompletedQueueSize(self) -> int:
        """Get size of data processing input queue."""
        return self.dataProcQueue.qsize()

    def getDataProcessedQueueSize(self) -> int:
        """Get size of data processing output queue."""
        return self.dataProcCompletedQueue.qsize()

    def addToScrapeCompletedQueue(self, fetchResult):
        """Add a fetch result to the completed queue."""
        self.fetchCompletedCount = self.fetchCompletedCount + 1
        self.fetchCompletedQueue.put(fetchResult)
        self.dataProcQueue.put(fetchResult)

    def fetchFromDataProcInputQ(self, block: bool = True, timeout: int = 30):
        """Fetch from data processing input queue."""
        resultObj = self.dataProcQueue.get(block=block, timeout=timeout)
        self.dataProcQueue.task_done()
        return resultObj

    def addToDataProcessedQueue(self, fetchResult):
        """Add result to data processing completed queue."""
        try:
            self.dataProcCompletedQueue.put(fetchResult)
            logger.debug("Added object to completed data processing queue: %s",
                         fetchResult.savedDataFileName)
        except Exception as e:
            logger.error("When adding item to data processing completed queue, error was: %s", e)

    def getTotalSrcPluginCount(self) -> int:
        """Get total count of URL sourcing plugins."""
        return self.totalPluginsURLSrcCount

    @staticmethod
    def loadPlugins(app_dir: str, plugins_dir: str, contrib_plugins_dir: str,
                    enabledPluginNames: dict) -> dict:
        """
        Load enabled plugins from the plugins directories.

        Args:
            app_dir (str): Application root directory
            plugins_dir (str): Main plugins directory
            contrib_plugins_dir (str): Contributed plugins directory
            enabledPluginNames (dict): Map of enabled plugin names to priorities

        Returns:
            dict: Map of plugin names to instantiated plugin objects
        """
        pluginsDict = dict()
        sys.path.append(app_dir)
        sys.path.append(plugins_dir)
        sys.path.append(contrib_plugins_dir)

        logger.debug('Loading enabled plugins: %s', enabledPluginNames)
        modulesPackageName = os.path.basename(plugins_dir)

        for pluginFileName in importlib.resources.contents(modulesPackageName):
            pluginFullPath = os.path.join(plugins_dir, pluginFileName)
            if os.path.isdir(pluginFullPath):
                continue

            modName = os.path.splitext(pluginFileName)[0]

            if modName in enabledPluginNames:
                className = modName
                try:
                    classObj = getattr(
                        importlib.import_module(modName, package=modulesPackageName),
                        className
                    )
                    pluginsDict[modName] = classObj()
                    pluginPriority = enabledPluginNames[modName]
                    pluginsDict[modName].executionPriority = pluginPriority
                    logger.info('Loaded plugin %s with priority = %s',
                                modName, pluginsDict[modName].executionPriority)
                except Exception as e:
                    logger.error("While importing plugin %s got exception: %s", modName, e)

        contribPluginsDict = QueueManager.loadPluginsContrib(contrib_plugins_dir, enabledPluginNames)
        pluginsDict.update(contribPluginsDict)
        return pluginsDict

    @staticmethod
    def loadPluginsContrib(contrib_plugins_dir: str, enabledPluginNames: list) -> dict:
        """
        Load contributed plugins.

        Args:
            contrib_plugins_dir (str): Contributed plugins directory
            enabledPluginNames (list): List of enabled plugin names

        Returns:
            dict: Map of plugin names to instantiated plugin objects
        """
        pluginsDict = dict()
        sys.path.append(contrib_plugins_dir)
        contribPluginsPackageName = os.path.basename(contrib_plugins_dir)

        for pluginFileName in importlib.resources.contents(contribPluginsPackageName):
            pluginFullPath = os.path.join(contrib_plugins_dir, pluginFileName)
            if os.path.isdir(pluginFullPath):
                continue

            modName = os.path.splitext(pluginFileName)[0]

            if modName in enabledPluginNames:
                className = modName
                try:
                    logger.debug("Importing contributed plugin: %s", modName)
                    classObj = getattr(
                        importlib.import_module(modName, package=contribPluginsPackageName),
                        className
                    )
                    pluginsDict[modName] = classObj()
                except Exception as e:
                    logger.error("While importing contributed plugin %s got exception: %s",
                                 modName, e)
        return pluginsDict

    def initPlugins(self):
        """
         Load, configure and initialize all plugins with parallel pending URL retrieval.

         This method:
         1. Loads plugin modules from configured directories
         2. Initializes each plugin with configuration
         3. Sets up URL queues for content plugins
         4. Builds domain-to-plugin mappings
         5. Retrieves pending URLs from database (on startup only, not during execution)
         """
        import concurrent.futures

        # Load plugins
        self.pluginNameToObjMap = QueueManager.loadPlugins(
            self.app_config.install_prefix,
            self.app_config.plugins_dir,
            self.app_config.plugins_contributed_dir,
            self.app_config.enabledPluginNames
        )

        # Initialize plugins first (configuration)
        for keyitem in self.pluginNameToObjMap.keys():
            logger.debug("Initializing plugin: %s", keyitem)
            plugin = self.pluginNameToObjMap[keyitem]

            # SET SHUTDOWN EVENT ON PLUGIN
            plugin.shutdown_event = self.shutdown_event

            if plugin.pluginType not in [PluginTypes.MODULE_NEWS_AGGREGATOR,
                                         PluginTypes.MODULE_DATA_PROCESSOR]:
                plugin.config(self.app_config)
                plugin.initNetworkHelper()
                self.URL_frontier[keyitem] = queue.Queue()
                plugin.setURLQueue(self.URL_frontier[keyitem])
                self.allowedDomainsList = self.allowedDomainsList + plugin.allowedDomains

            elif plugin.pluginType == PluginTypes.MODULE_NEWS_AGGREGATOR:
                plugin.config(self.app_config)
                plugin.initNetworkHelper()

            elif plugin.pluginType == PluginTypes.MODULE_DATA_PROCESSOR:
                plugin.config(self.app_config)
                plugin.additionalConfig(self.sessionHistoryDB)

            # Build domain-to-plugin map
            if plugin.pluginType in [PluginTypes.MODULE_NEWS_CONTENT,
                                     PluginTypes.MODULE_NEWS_AGGREGATOR,
                                     PluginTypes.MODULE_DATA_CONTENT,
                                     PluginTypes.MODULE_NEWS_API]:
                self.totalPluginsURLSrcCount += 1
                modname = plugin.pluginName
                domains = plugin.allowedDomains
                for dom in domains:
                    self.domainToPluginMap[dom] = modname

        # PARALLEL PENDING URL RETRIEVAL - optimized
        logger.info("Retrieving pending URLs from database in parallel...")
        start_time = time.time()

        def retrieve_pending_for_plugin(plugin_name, plugin):
            """Retrieve pending URLs for a single plugin."""
            if plugin.pluginType not in [PluginTypes.MODULE_NEWS_AGGREGATOR,
                                         PluginTypes.MODULE_DATA_PROCESSOR]:
                try:
                    pending_urls = self.sessionHistoryDB.retrieveTodoURLList(plugin_name)
                    if pending_urls:
                        logger.info(f"{plugin_name}: Retrieved {len(pending_urls)} pending URLs from database")
                        return plugin_name, pending_urls
                except Exception as e:
                    logger.error(f"Error retrieving pending URLs for {plugin_name}: {e}")
            return plugin_name, []

        # Use ThreadPoolExecutor with more workers for I/O-bound tasks
        plugins_to_process = [
            (name, plugin) for name, plugin in self.pluginNameToObjMap.items()
            if plugin.pluginType not in [PluginTypes.MODULE_NEWS_AGGREGATOR,
                                         PluginTypes.MODULE_DATA_PROCESSOR]
        ]

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(plugins_to_process)) as executor:
            futures = [
                executor.submit(retrieve_pending_for_plugin, name, plugin)
                for name, plugin in plugins_to_process
            ]

            for future in concurrent.futures.as_completed(futures):
                plugin_name, pending_urls = future.result()
                if pending_urls:
                    plugin = self.pluginNameToObjMap[plugin_name]
                    for url in pending_urls:
                        plugin.urlQueue.put(url)
                        plugin.urlQueueTotalSize += 1

        elapsed = time.time() - start_time
        logger.info(f"Parallel URL retrieval completed in {elapsed:.1f} seconds")

        logger.info("Completed initialising %s plugins.", len(self.pluginNameToObjMap))
        self.q_status.updateStatus()

    def initURLSourcingWorkers(self):
        """
        Initialize worker threads for URL sourcing.

        Creates worker threads that identify URLs to scrape from news sources.
        """
        logger.debug("Initializing URL sourcing worker threads.")
        workerNumber = 0

        for keyitem in self.pluginNameToObjMap.keys():
            plugin = self.pluginNameToObjMap[keyitem]

            if plugin.pluginType in [PluginTypes.MODULE_NEWS_CONTENT,
                                     PluginTypes.MODULE_NEWS_API,
                                     PluginTypes.MODULE_DATA_CONTENT,
                                     PluginTypes.MODULE_NEWS_AGGREGATOR]:
                workerNumber += 1
                self.urlSrcWorkers[workerNumber] = PluginWorker(
                    plugin,
                    PluginTypes.TASK_GET_URL_LIST,
                    self.sessionHistoryDB,
                    self,
                    name=str(workerNumber),
                    daemon=False
                )
                self.urlSrcWorkers[workerNumber].setRunDate(self.runDate)

                # Set timeout for URL gathering
                self.urlSrcWorkers[workerNumber].url_gathering_timeout = self.url_gathering_timeout

            if plugin.pluginType == PluginTypes.MODULE_NEWS_AGGREGATOR:
                self.urlSrcWorkers[workerNumber].setDomainMapAndPlugins(
                    self.domainToPluginMap,
                    self.pluginNameToObjMap
                )

        logger.info(f"{len(self.urlSrcWorkers)} worker threads available to identify URLs to scrape.")

    def initContentFetchWorkers(self):
        """
        Initialize worker threads for content fetching.

        Creates worker threads that fetch and parse content from discovered URLs.
        """
        logger.debug("Initializing content fetching worker threads.")
        workerNumber = 0

        for keyitem in self.pluginNameToObjMap.keys():
            plugin = self.pluginNameToObjMap[keyitem]

            if plugin.pluginType in [PluginTypes.MODULE_NEWS_CONTENT,
                                     PluginTypes.MODULE_NEWS_API,
                                     PluginTypes.MODULE_DATA_CONTENT]:
                workerNumber += 1
                self.contentFetchWorkers[workerNumber] = PluginWorker(
                    plugin,
                    PluginTypes.TASK_GET_DATA,
                    self.sessionHistoryDB,
                    self,
                    name=str(workerNumber + len(self.urlSrcWorkers)),
                    daemon=False
                )
                self.contentFetchWorkers[workerNumber].setRunDate(self.runDate)

        logger.info("%s worker threads available to fetch content.", len(self.contentFetchWorkers))

    def initDataProcWorkers(self):
        """
        Initialize worker threads for data processing.

        Creates worker threads that execute data processing plugins on scraped content.
        """
        logger.debug("Initializing data processing worker threads.")
        self.dataProcPluginsMap = {}
        allPriorityValues = []

        try:
            for keyitem in self.pluginNameToObjMap.keys():
                plugin = self.pluginNameToObjMap[keyitem]
                logger.debug(f'Checking data proc plugin: {plugin}, Type = {plugin.pluginType}')

                if plugin.pluginType == PluginTypes.MODULE_DATA_PROCESSOR:
                    priorityVal = plugin.executionPriority
                    allPriorityValues.append(priorityVal)
                    self.dataProcPluginsMap[priorityVal] = plugin

            sortedPriorityKeys = sorted(set(allPriorityValues))

            for index in range(self.dataproc_threads):
                self.dataProcessWorkerList.append(DataProcessor(
                    self.dataProcPluginsMap,
                    sortedPriorityKeys,
                    self,
                    self.q_status,
                    name=str(index + 100),
                    daemon=False
                ))
        except Exception as e:
            logger.error("Error initializing data processing worker threads: %s", e)
            sys.exit(2)

        logger.info(f"{len(self.dataProcessWorkerList)} worker threads initialized for " +
                    f"{len(self.dataProcPluginsMap)} data processing plugins.")

    def runAllJobs(self):
        """
        Execute all scraping jobs with progress bars in main thread with proper thread lifecycle management.

        This is the main execution method that:
        1. Starts progress monitoring
        2. Launches URL sourcing workers
        3. Launches content fetching workers
        4. Launches data processing workers
        5. Waits for all workers to complete
        6. Handles keyboard interrupts gracefully
        """

        # Initialize all workers FIRST
        self.initURLSourcingWorkers()
        self.initContentFetchWorkers()
        self.initDataProcWorkers()

        # Initialize enlighten manager in MAIN thread
        import enlighten
        enlighten_manager = enlighten.get_manager()

        # Initialize progress watcher WITHOUT enlighten (it will use our manager)
        self.progressWatchThread = ProgressWatcher(
            self.pluginNameToObjMap,
            self.sessionHistoryDB,
            self,
            self.q_status,
            self.app_config,
            enlighten_manager=enlighten_manager,
            name='ProgressWatcher',
            daemon=False
        )

        try:
            logger.info("Starting all worker threads.")

            # Start status API FIRST
            if self.status_api:
                self.status_api.start()

            # Create progress bars in MAIN thread
            self.q_status.updateStatus()

            urlListFtBar = enlighten_manager.counter(
                count=0,
                total=max(self.q_status.totalPluginsURLSourcing, 1),
                desc='URLs identified:',
                unit='Plugins',
                color='yellow',
                leave=False
            )

            urlScrapeBar = enlighten_manager.counter(
                count=0,
                total=1,
                desc='Data downloaded:',
                unit='   URLs',
                color='cyan',
                leave=False
            )

            dataProcsBar = enlighten_manager.counter(
                count=0,
                total=1,
                desc=' Data processed:',
                unit='  Files',
                color='green',
                leave=False
            )

            # Pass bars to progress watcher
            self.progressWatchThread.set_progress_bars(urlListFtBar, urlScrapeBar, dataProcsBar)

            print("\nWeb-scraping Progress:\n")

            # Start progress watcher
            self.progressWatchThread.start()

            # Small delay to let bars render
            import time
            time.sleep(0.5)

            # Start URL sourcing workers
            for keyitem in self.urlSrcWorkers.keys():
                self.urlSrcWorkers[keyitem].start()

            # Start content fetching workers
            for keyitem in self.contentFetchWorkers.keys():
                self.contentFetchWorkers[keyitem].start()

            # Start data processing workers
            for dat_worker in self.dataProcessWorkerList:
                dat_worker.start()

            # ============================================================
            # WAIT FOR URL SOURCING WORKERS (with timeout)
            # ============================================================
            logger.info(f"Waiting up to {self.url_gathering_timeout} seconds for URL gathering to complete")
            url_gather_start = time.time()

            for keyitem in self.urlSrcWorkers.keys():
                remaining_time = self.url_gathering_timeout - (time.time() - url_gather_start)
                if remaining_time <= 0:
                    logger.warning("URL gathering timeout reached, proceeding with available URLs")
                    for plugin in self.pluginNameToObjMap.values():
                        plugin.is_stopped = True
                    break

                self.urlSrcWorkers[keyitem].join(timeout=max(1, remaining_time))
                if self.urlSrcWorkers[keyitem].is_alive():
                    logger.warning(f"URL sourcing worker {keyitem} did not complete in time")

            logger.info('URL gathering phase complete.')

            # ============================================================
            # WAIT FOR CONTENT FETCHING WORKERS (no timeout - must complete)
            # ============================================================
            logger.info("Waiting for content fetching workers to complete...")

            # Check if workers are still active
            active_fetch_workers = [
                w for w in self.contentFetchWorkers.values() if w.is_alive()
            ]
            logger.info(f"{len(active_fetch_workers)} content fetching workers are active")

            # Wait for each content worker to complete
            for keyitem in self.contentFetchWorkers.keys():
                worker = self.contentFetchWorkers[keyitem]
                if worker.is_alive():
                    logger.info(f"Waiting for content worker {keyitem} ({worker.pluginName})...")
                    worker.join()  # NO TIMEOUT - wait indefinitely
                    logger.info(f"Content worker {keyitem} ({worker.pluginName}) completed")
                else:
                    logger.debug(f"Content worker {keyitem} ({worker.pluginName}) already finished")

            logger.info('All content fetching workers completed.')

            # ============================================================
            # WAIT FOR DATA PROCESSING WORKERS (no timeout - must complete)
            # ============================================================
            logger.info("Waiting for data processing workers to complete...")

            active_data_workers = [
                w for w in self.dataProcessWorkerList if w.is_alive()
            ]
            logger.info(f"{len(active_data_workers)} data processing workers are active")

            # Wait for each data processing worker to complete
            for dat_worker in self.dataProcessWorkerList:
                if dat_worker.is_alive():
                    logger.info(f"Waiting for data processing worker {dat_worker.workerID}...")
                    dat_worker.join()  # NO TIMEOUT - wait indefinitely
                    logger.info(f"Data processing worker {dat_worker.workerID} completed")
                else:
                    logger.debug(f"Data processing worker {dat_worker.workerID} already finished")

            logger.info('All data processing workers completed.')

            # ============================================================
            # WAIT FOR PROGRESS WATCHER
            # ============================================================
            logger.info("Waiting for progress watcher to complete...")
            if self.progressWatchThread.is_alive():
                self.progressWatchThread.join(timeout=30)
                if self.progressWatchThread.is_alive():
                    logger.warning("Progress watcher did not finish in time")

            # Close progress bars in main thread
            try:
                urlListFtBar.close(clear=True)
                urlScrapeBar.close(clear=True)
                dataProcsBar.close(clear=True)
                enlighten_manager.stop()
            except Exception as e:
                logger.error(f"Error closing progress bars: {e}")

            logger.info("All workers completed successfully.")

        except KeyboardInterrupt:
            logger.error("Recognized keyboard interrupt, stopping the program now...")
            print("\nRecognized keyboard interrupt, stopping the program now...")
            self.shutdown()

        except Exception as e:
            logger.error(f"Error while processing all the queues: {e}", exc_info=True)
            print(f"Error while processing all the queues: {e}")
            self.shutdown()
            raise

    def _log_worker_states(self):
        """Log the state of all workers for debugging."""
        logger.debug("="*50)
        logger.debug("WORKER STATES:")
        logger.debug("="*50)

        # URL sourcing workers
        url_alive = sum(1 for w in self.urlSrcWorkers.values() if w.is_alive())
        logger.debug(f"URL Sourcing Workers: {url_alive}/{len(self.urlSrcWorkers)} alive")

        # Content fetching workers
        fetch_alive = sum(1 for w in self.contentFetchWorkers.values() if w.is_alive())
        logger.debug(f"Content Fetching Workers: {fetch_alive}/{len(self.contentFetchWorkers)} alive")

        # Data processing workers
        data_alive = sum(1 for w in self.dataProcessWorkerList if w.is_alive())
        logger.debug(f"Data Processing Workers: {data_alive}/{len(self.dataProcessWorkerList)} alive")

        # Progress watcher
        logger.debug(f"Progress Watcher: {'alive' if self.progressWatchThread.is_alive() else 'finished'}")

        logger.debug("="*50)

    def shutdown(self):
        logger.info("Shutting down the queue manager.")
        # TODO: kill all worker threads.
        # stop each content worker:
        for keyitem in self.contentFetchWorkers.keys():
            worker = self.contentFetchWorkers[keyitem]
            if worker.is_alive():
                worker.stop()
                logger.info(f"Content worker {keyitem} ({worker.pluginName}) stopped")
            else:
                logger.debug(f"Content worker {keyitem} ({worker.pluginName}) already finished")


class GracefulShutdownHandler:
    """
    Handles graceful shutdown on SIGINT (Ctrl+C) and SIGTERM.

    Features:
    - First Ctrl+C: Initiates graceful shutdown
    - Second Ctrl+C: Forces immediate exit
    - Propagates shutdown signal to all components
    """

    def __init__(self, queue_manager):
        """
        Initialize shutdown handler.

        Args:
            queue_manager: QueueManager instance to shutdown
        """
        self.queue_manager = queue_manager
        self.shutdown_count = 0
        self.original_sigint = None
        self.original_sigterm = None

        # Install signal handlers
        self._install_handlers()

    def _install_handlers(self):
        """Install signal handlers for SIGINT and SIGTERM."""
        self.original_sigint = signal.signal(signal.SIGINT, self._signal_handler)
        self.original_sigterm = signal.signal(signal.SIGTERM, self._signal_handler)
        logging.info("Installed graceful shutdown handlers for SIGINT and SIGTERM")

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        self.shutdown_count += 1

        if self.shutdown_count == 1:
            # First interrupt - graceful shutdown
            print("\n" + "="*50)
            print("*** Interrupt received. Shutting down gracefully... ***")
            print("*** Press Ctrl+C again to force immediate exit ***")
            print("="*50)
            logging.warning("Shutdown signal received, initiating graceful shutdown...")

            # Signal all components to stop
            if self.queue_manager:
                self.queue_manager.shutdown()

        elif self.shutdown_count >= 2:
            # Second interrupt - force exit
            print("\n" + "="*50)
            print("*** Force shutdown requested. Exiting immediately... ***")
            print("="*50)
            logging.error("Force shutdown signal received, exiting immediately")

            # Force exit cleanly
            import os
            os._exit(1)

    def restore_handlers(self):
        """Restore original signal handlers."""
        if self.original_sigint:
            signal.signal(signal.SIGINT, self.original_sigint)
        if self.original_sigterm:
            signal.signal(signal.SIGTERM, self.original_sigterm)
        logging.info("Restored original signal handlers")


# End of file