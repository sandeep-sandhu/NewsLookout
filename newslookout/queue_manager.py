#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Queue Manager with Coordinated Worker Pairs
- Runs progress monitoring in main thread
- Uses WorkerPair for coordinated URL discovery and content fetching
- thread lifecycle management
"""

import importlib
import os
from datetime import datetime
import multiprocessing
import threading
import queue
import time
import logging
import signal
import sys
import traceback

from data_structs import PluginTypes, QueueStatus
from session_hist import SessionHistory
from worker import WorkerPair, DataProcessor, StatusAPIServer
from config import ConfigManager


logger = logging.getLogger(__name__)


class QueueManager:
    """
    Queue Manager with coordinated worker pairs.

    Key features:
    - WorkerPair coordinates URL discovery and content fetching
    - Progress monitoring runs in main thread
    - Proper thread lifecycle with timeouts

    """

    def __init__(self):
        """Initialize the QueueManager."""
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

        # Coordinated worker pairs
        self.worker_pairs = dict()  # plugin_name -> WorkerPair

        # Data processing workers
        self.dataProcessWorkerList = []
        self.dataproc_threads = 5

        # Progress monitoring
        self.progress_monitor = None

        # Synchronization
        self.dbAccessSemaphore = None
        self.sessionHistoryDB = None
        self.shutdown_event = threading.Event()

        # Queues
        self.fetchCompletedQueue = queue.Queue()
        self.dataProcQueue = queue.Queue()
        self.dataProcCompletedQueue = queue.Queue()
        self.alreadyDataProcList = []

        # Database operations queue
        self.dbCommandQueue = queue.Queue()
        self.dbWorkerThread = None

        # URL gathering timeout
        self.url_gathering_timeout = 600  # 10 minutes default

        self.q_status = QueueStatus(self)

    def config(self, app_config: ConfigManager):
        """Configure the queue manager."""
        self.app_config = app_config

        try:
            logger.debug("Configuring the queue manager")

            # Install signal handler
            self.shutdown_handler = GracefulShutdownHandler(self)

            # Start status API if enabled
            if self.app_config.rest_api_enabled:
                self.status_api = StatusAPIServer(
                    self,
                    host=self.app_config.rest_api_host,
                    port=self.app_config.rest_api_port
                )

            self.available_cores = multiprocessing.cpu_count()
            self.runDate = self.app_config.rundate

            # Calculate fetch cycle time
            self.fetchCycleTime = max(60, (
                    int(self.app_config.retry_wait_rand_max_sec) +
                    int(self.app_config.retry_wait_sec) +
                    int(self.app_config.connect_timeout) +
                    int(self.app_config.fetch_timeout)
            ))

            # Read URL gathering timeout
            self.url_gathering_timeout = self.app_config.checkAndSanitizeConfigInt(
                'operation',
                'url_gathering_timeout',
                default=600,
                maxValue=3600,
                minValue=60
            )
            logger.info(f"URL gathering timeout: {self.url_gathering_timeout}s")

        except Exception as e:
            logger.error(f"Error configuring queue manager: {e}")

        # Initialize database
        self.dbAccessSemaphore = threading.Semaphore()
        self.sessionHistoryDB = SessionHistory(
            self.app_config.completed_urls_datafile,
            self.dbAccessSemaphore
        )
        self.sessionHistoryDB.printDBStats()

        # Start database worker
        self._startDatabaseWorker()

    def _startDatabaseWorker(self):
        """Start dedicated database worker thread."""
        self.dbWorkerThread = threading.Thread(
            target=self._databaseWorkerLoop,
            name="DatabaseWorker",
            daemon=False
        )
        self.dbWorkerThread.start()
        logger.info("Database worker thread started")

    def _databaseWorkerLoop(self):
        """Main loop for database worker with batching."""
        logger.info("Database worker loop started")

        # Batching configuration
        batch_size = 1000
        batch_timeout = 2.0  # seconds
        pending_operations = []
        last_flush = time.time()

        while not self.shutdown_event.is_set():
            try:
                # Try to get command with short timeout for batching
                try:
                    cmd = self.dbCommandQueue.get(timeout=0.5)
                except queue.Empty:
                    cmd = None

                if cmd is None:
                    # Check if we need to flush pending batch
                    if pending_operations and (time.time() - last_flush) > batch_timeout:
                        self._flush_pending_batch(pending_operations)
                        pending_operations = []
                        last_flush = time.time()

                    if self.shutdown_event.is_set():
                        break
                    continue

                if cmd is None:  # Poison pill
                    break

                operation, args, result_queue = cmd

                # Batch 'add_pending' operations
                if operation == 'add_pending' and not result_queue:
                    pending_operations.append((operation, args, result_queue))
                    self.dbCommandQueue.task_done()

                    # Flush if batch is full
                    if len(pending_operations) >= batch_size:
                        self._flush_pending_batch(pending_operations)
                        pending_operations = []
                        last_flush = time.time()
                    continue

                # Execute other operations immediately
                try:
                    if operation == 'write_queue':
                        # Process immediately without batching
                        result = self.sessionHistoryDB.writeQueueToDB(args)
                        if result_queue:
                            result_queue.put(result)
                        else:
                            # Log if write succeeded but no result queue
                            logger.debug(f"Wrote {len(args) if isinstance(args, list) else 1} URLs to DB (no result queue)")

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

                    else:
                        logger.error(f"Unknown DB operation: {operation}")

                except Exception as e:
                    logger.error(f"Error in DB operation '{operation}': {e}")
                    if result_queue:
                        result_queue.put(None)

                finally:
                    self.dbCommandQueue.task_done()

            except Exception as e:
                logger.error(f"Database worker error: {e}")

        # Flush any remaining operations
        if pending_operations:
            self._flush_pending_batch(pending_operations)

        logger.info("Database worker loop stopped")

    def _flush_pending_batch(self, pending_operations):
        """Flush a batch of pending URL operations."""
        if not pending_operations:
            return

        try:
            # Group by plugin
            plugin_urls = {}
            for operation, args, result_queue in pending_operations:
                url_list, plugin_name = args
                if plugin_name not in plugin_urls:
                    plugin_urls[plugin_name] = []
                plugin_urls[plugin_name].extend(url_list)

            # Write in batches per plugin
            total_urls = 0
            for plugin_name, urls in plugin_urls.items():
                self.sessionHistoryDB.addURLsToPendingTable(urls, plugin_name)
                total_urls += len(urls)

            logger.info(f"Flushed batch: {total_urls} URLs across {len(plugin_urls)} plugins")

        except Exception as e:
            logger.error(f"Error flushing pending batch: {e}")

    def queueDBOperation(self, operation: str, args, wait_for_result=False):
        """Queue a database operation."""
        result_queue = queue.Queue() if wait_for_result else None
        self.dbCommandQueue.put((operation, args, result_queue))

        if wait_for_result:
            try:
                return result_queue.get(timeout=8)
            except queue.Empty:
                logger.error(f"Timeout waiting for DB operation: {operation}")
                logger.error(f"DB queue size: {self.dbCommandQueue.qsize()}, args sample: {str(args)[:100]}")
                return None
        return None

    def initPlugins(self):
        """Load and initialize all plugins."""
        # Load plugins
        self.pluginNameToObjMap = self.loadPlugins(
            self.app_config.install_prefix,
            self.app_config.plugins_dir,
            self.app_config.plugins_contributed_dir,
            self.app_config.enabledPluginNames
        )

        # Initialize plugins
        for plugin_name, plugin in self.pluginNameToObjMap.items():
            logger.debug(f"Initializing plugin: {plugin_name}")

            # Set shutdown event
            plugin.shutdown_event = self.shutdown_event
            # Set queue manager reference for database operations
            plugin.queue_manager = self

            if plugin.pluginType not in [PluginTypes.MODULE_NEWS_AGGREGATOR,
                                         PluginTypes.MODULE_DATA_PROCESSOR]:
                plugin.config(self.app_config)
                plugin.initNetworkHelper()
                plugin.setURLQueue(queue.Queue())
                self.allowedDomainsList.extend(plugin.allowedDomains)

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
                for domain in plugin.allowedDomains:
                    self.domainToPluginMap[domain] = plugin_name

        # NOTE: Pending URL retrieval moved to URLDiscoveryWorker.run()
        # This allows progress bars to be displayed immediately when workers start

        logger.info(f"Initialized {len(self.pluginNameToObjMap)} plugins")
        self.q_status.updateStatus()

    def _retrieve_pending_for_plugin(self, plugin_name: str, plugin) -> tuple:
        """Retrieve pending URLs for a single plugin."""
        if plugin.pluginType not in [PluginTypes.MODULE_NEWS_AGGREGATOR,
                                     PluginTypes.MODULE_DATA_PROCESSOR]:
            try:
                pending_urls = self.sessionHistoryDB.retrieveTodoURLList(plugin_name)
                if pending_urls:
                    logger.info(f"{plugin_name}: Retrieved {len(pending_urls)} pending URLs")
                    return plugin_name, pending_urls
            except Exception as e:
                logger.error(f"Error retrieving pending URLs for {plugin_name}: {e}")
        return plugin_name, []

    def initWorkerPairs(self):
        """
        Initialize coordinated worker pairs for each plugin.

        Each plugin gets a WorkerPair that coordinates:
        - URL discovery worker (with timeout)
        - Content fetching worker (monitors URL worker)
        """
        logger.info("Initializing coordinated worker pairs...")

        worker_id = 0

        for plugin_name, plugin in self.pluginNameToObjMap.items():
            # Only create pairs for content plugins
            if plugin.pluginType in [PluginTypes.MODULE_NEWS_CONTENT,
                                     PluginTypes.MODULE_DATA_CONTENT,
                                     PluginTypes.MODULE_NEWS_API]:
                worker_id += 1

                # Create worker pair
                pair = WorkerPair(
                    plugin,
                    self.sessionHistoryDB,
                    self,
                    worker_id="[" + str(worker_id) + "] " + plugin_name
                )

                # Initialize with timeout
                pair.initialize(
                    run_date=self.runDate,
                    url_timeout=self.url_gathering_timeout
                )

                self.worker_pairs[plugin_name] = pair

        logger.info(f"Created {len(self.worker_pairs)} worker pairs")

    def initDataProcWorkers(self):
        """Initialize data processing workers."""
        logger.debug("Initializing data processing workers...")
        self.dataProcPluginsMap = {}
        allPriorityValues = []

        try:
            for plugin_name, plugin in self.pluginNameToObjMap.items():
                if plugin.pluginType == PluginTypes.MODULE_DATA_PROCESSOR:
                    priority = plugin.executionPriority
                    allPriorityValues.append(priority)
                    self.dataProcPluginsMap[priority] = plugin

            sortedPriorityKeys = sorted(set(allPriorityValues))

            for key in self.dataProcPluginsMap.keys():
                self.dataProcessWorkerList.append(DataProcessor(
                    self.dataProcPluginsMap,
                    sortedPriorityKeys,
                    self,
                    self.q_status,
                    name=self.dataProcPluginsMap[key].pluginName,
                    daemon=False
                ))

        except Exception as e:
            logger.error(f"Error initializing data processing workers: {e}")
            import sys
            sys.exit(2)

        logger.info(f"{len(self.dataProcessWorkerList)} data processing workers initialized")

    def runAllJobs(self):
        """
        Execute all jobs with progress monitoring in main thread.

        This method:
        1. Starts worker pairs
        2. Starts data processing workers
        3. Monitors progress in main thread
        4. Waits for all workers to complete
        """
        # Initialize all components
        self.initWorkerPairs()
        self.initDataProcWorkers()

        # Start status API
        if self.status_api:
            self.status_api.start()

        try:
            logger.info("Starting all workers...")

            # Start worker pairs
            for plugin_name, pair in self.worker_pairs.items():
                pair.start()
                logger.info(f"Started worker pair for {plugin_name}")

            # Start data processing workers
            for worker in self.dataProcessWorkerList:
                worker.start()

            # Run progress monitoring in MAIN THREAD
            self._monitor_progress_main_thread()

            logger.info("All workers completed successfully")

        except KeyboardInterrupt:
            logger.error("Keyboard interrupt received")
            print("\nStopping workers gracefully...")
            self.shutdown()

        except Exception as e:
            logger.error(f"Error during execution: {e}", exc_info=True)
            self.shutdown()
            raise

    def _monitor_progress_main_thread(self):
        """
        Monitor progress in the main thread.

        This replaces the separate ProgressWatcher thread and ensures
        we can track all workers properly.
        """
        import enlighten

        # Initialize progress bars
        manager = enlighten.get_manager()

        urlListBar = manager.counter(
            count=0,
            total=max(len(self.worker_pairs), 1),
            desc='URLs discovered:',
            unit='Plugins',
            color='yellow',
            leave=False
        )

        urlFetchBar = manager.counter(
            count=0,
            total=1,
            desc='Data downloaded:',
            unit='  URLs',
            color='cyan',
            leave=False
        )

        dataProcsBar = manager.counter(
            count=0,
            total=1,
            desc='Data processed:',
            unit=' Files',
            color='green',
            leave=False
        )

        print("\nWeb-scraping Progress:\n")

        # Monitoring loop
        refresh_interval = self.app_config.progressRefreshInt
        log_counter = 0
        urls_completed_last = 0
        data_processed_last = 0

        try:
            # Check shutdown
            while not self.shutdown_event.is_set() and (
                    any(p.is_alive() for p in self.worker_pairs.values()) or
                    any(w.is_alive() for w in self.dataProcessWorkerList)
            ):
                # Update status
                self.q_status.updateStatus()

                # Check if all workers are done
                pairs_active = sum(1 for p in self.worker_pairs.values() if p.is_alive())
                data_workers_active = sum(1 for w in self.dataProcessWorkerList if w.is_alive())

                if pairs_active == 0 and data_workers_active == 0:
                    logger.info("All workers have completed - stopping progress monitor")
                    break

                # Update URL discovery progress
                pairs_complete = sum(
                    1 for p in self.worker_pairs.values()
                    if p.url_discovery_complete.is_set()
                )
                urlListBar.update(incr=pairs_complete - urlListBar.count)

                # Update URL fetch progress
                urls_completed = self.q_status.fetchCompletCount
                # Once all worker pairs are dead, snap the total to actual completed
                # so the bar reaches 100% instead of being stuck at a fraction
                if pairs_active == 0:
                    urlFetchBar.total = max(urls_completed, 1)
                else:
                    urlFetchBar.total = max(self.q_status.totalURLCount, 1)
                urlFetchBar.update(incr=urls_completed - urls_completed_last)
                urls_completed_last = urls_completed

                # Update data processing progress
                data_total = self.q_status.dataInputQsize + self.q_status.dataOutputQsize
                dataProcsBar.total = max(data_total, 1)
                data_processed = self.q_status.dataOutputQsize
                dataProcsBar.update(incr=data_processed - data_processed_last)
                data_processed_last = data_processed

                # Process completed URLs from queue
                self._process_completed_urls()

                # Periodic logging
                log_counter += 1
                if log_counter > 24:  # Every ~2 minutes with 5s refresh
                    log_counter = 0
                    logger.info(
                        f"Status - Pairs: {pairs_active}, URLs: {self.q_status.totalURLCount}, "
                        f"Completed: {urls_completed}, Processed: {data_processed}"
                    )

                # Wait before next update
                for _ in range(refresh_interval):
                    if self.shutdown_event.wait(timeout=1):
                        break

        finally:
            # Close progress bars
            urlListBar.close(clear=True)
            urlFetchBar.close(clear=True)
            dataProcsBar.close(clear=True)
            manager.stop()

            logger.info("Progress monitoring complete")

    def _process_completed_urls(self):
        """Process completed URLs from queue and save to database."""
        results = []

        try:
            while not self.fetchCompletedQueue.empty():
                results.append(self.fetchCompletedQueue.get_nowait())
        except queue.Empty:
            pass

        if results:
            count = self.queueDBOperation(
                'write_queue',
                results,
                wait_for_result=True
            )
            if count:
                logger.debug(f"Saved {count} URLs to database")

    def shutdown(self):
        """Shutdown all workers gracefully."""
        logger.info("Initiating shutdown...")

        # Signal shutdown
        self.shutdown_event.set()

        # Wait for worker pairs
        logger.info("Waiting for worker pairs to complete...")
        for plugin_name, pair in self.worker_pairs.items():
            pair.join(timeout=10)
            if pair.is_alive():
                logger.warning(f"Worker pair {plugin_name} did not finish in time")

        # Wait for data processing workers
        logger.info("Waiting for data processing workers...")
        for worker in self.dataProcessWorkerList:
            worker.join(timeout=10)
            if worker.is_alive():
                logger.warning(f"Data worker {worker.workerID} did not finish in time")

        # Stop database worker
        logger.info("Stopping database worker...")
        self.dbCommandQueue.put(None)  # Poison pill
        if self.dbWorkerThread:
            self.dbWorkerThread.join(timeout=5)

        logger.info("Shutdown complete")

    # Keep existing helper methods for compatibility
    def addToScrapeCompletedQueue(self, fetchResult):
        """Add fetch result to completed queue."""
        self.fetchCompletedCount += 1
        self.fetchCompletedQueue.put(fetchResult)
        self.dataProcQueue.put(fetchResult)

    def fetchFromDataProcInputQ(self, block=True, timeout=30):
        """Fetch from data processing input queue."""
        result = self.dataProcQueue.get(block=block, timeout=timeout)
        self.dataProcQueue.task_done()
        return result

    def addToDataProcessedQueue(self, fetchResult):
        """Add to data processing output queue."""
        self.dataProcCompletedQueue.put(fetchResult)

    def getCompletedQueueSize(self):
        """Get data processing input queue size."""
        return self.dataProcQueue.qsize()

    def getDataProcessedQueueSize(self):
        """Get data processing output queue size."""
        return self.dataProcCompletedQueue.qsize()

    @staticmethod
    def loadPlugins(app_dir, plugins_dir, contrib_dir, enabled_names):
        """Load plugins from directories."""
        # Implementation from original code
        import sys

        plugins = {}
        sys.path.append(app_dir)
        sys.path.append(plugins_dir)
        sys.path.append(contrib_dir)

        logger.info(f"Loading plugins: {', '.join(enabled_names.keys())}")

        package_name = os.path.basename(plugins_dir)

        for plugin_file in importlib.resources.contents(package_name):
            plugin_path = os.path.join(plugins_dir, plugin_file)
            if os.path.isdir(plugin_path):
                continue

            mod_name = os.path.splitext(plugin_file)[0]

            if mod_name in enabled_names:
                try:
                    class_obj = getattr(
                        importlib.import_module(mod_name, package=package_name),
                        mod_name
                    )
                    plugins[mod_name] = class_obj()
                    plugins[mod_name].executionPriority = enabled_names[mod_name]
                    logger.info(f"Loaded plugin {mod_name}")
                except Exception as e:
                    logger.error(f"Error loading plugin {mod_name}: {e}")

        return plugins


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
        logger.info("Installed graceful shutdown handlers for SIGINT and SIGTERM")

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        self.shutdown_count += 1

        if self.shutdown_count == 1:
            # First interrupt - graceful shutdown
            print("\n" + "="*50)
            print("*** Interrupt received. Shutting down gracefully... ***")
            print("*** Press Ctrl+C again to force immediate exit ***")
            print("="*50)
            logger.warning("Shutdown signal received, initiating graceful shutdown...")

            # Signal all components to stop
            if self.queue_manager:
                self.queue_manager.shutdown()

        elif self.shutdown_count >= 2:
            # Second interrupt - force exit
            print("\n" + "="*50)
            print("*** Force shutdown requested. Exiting immediately... ***")
            print("="*50)
            logger.error("Force shutdown signal received, exiting immediately")

            # Force exit cleanly
            import os
            os._exit(1)

    def restore_handlers(self):
        """Restore original signal handlers."""
        if self.original_sigint:
            signal.signal(signal.SIGINT, self.original_sigint)
        if self.original_sigterm:
            signal.signal(signal.SIGTERM, self.original_sigterm)
        logger.info("Restored original signal handlers")


def join_with_logging(thread, timeout, name):
    thread.join(timeout)
    if thread.is_alive():
        logger.error(f"Thread {name} ({thread.name}) timed out after {timeout}s")


# for thread_id, frame in sys._current_frames().items():
#     logger.error(f"Thread {thread_id} stuck at:")
#     traceback.print_stack(frame)


# End of file