#!/usr/bin/env python
# -*- coding: utf-8 -*-

# #########################################################################################################
# File name: scraper_app.py                                                                               #
# Application: The NewsLookout Web Scraping Application                                                   #
# Date: 2021-06-23                                                                                        #
# Purpose: Main class for the web scraping and news text processing application                           #
# Copyright 2021, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu, sandeep.sandhu@gmx.com  #
#                                                                                                         #
# Usage:                                                                                                  #
# python scraper_app.py -c <configfile> -d <rundate>                                                      #
#                                                                                                         #
# The default location of configuration file is: conf/newslookout.conf                                    #
# Java SimpleDate Format for rundate argument, e.g. 2019-12-31 is: ${current_date:yyyy-MM-dd}             #
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


__version__ = "3.0.0"
__author__ = "Sandeep Singh Sandhu"
__copyright__ = "Copyright 2026, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu"
__credits__ = ["Sandeep Singh Sandhu"]
__license__ = "GPL"
__maintainer__ = "Sandeep Singh Sandhu"
__email__ = "sandeep.sandhu@gmx.com"
__status__ = "Production"

##########

#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
NewsLookout Library Interface

This module provides a simple, Pythonic interface for using NewsLookout as a library
in your own Python applications.

Example usage:
    
    from newslookout import NewsLookoutApp
    
    # Create and configure the app
    app = NewsLookoutApp(config_file='path/to/config.conf')
    
    # Run for a specific date
    app.run(run_date='2026-01-21', max_runtime=3600)
    
    # Or run in the background
    app.start()
    # ... do other work ...
    app.stop()
    
    # Get statistics
    stats = app.get_statistics()
    print(f"URLs processed: {stats['urls_processed']}")
"""

import logging
import os
from pathlib import Path
import sys
import threading
import time
from datetime import datetime
from typing import Optional, Dict, Any
from logging.handlers import RotatingFileHandler

# Import core application components
from queue_manager import QueueManager
from config import ConfigManager
from scraper_utils import checkAndGetNLTKData


class NewsLookoutApp:
    """
    Main application class for NewsLookout web scraping.

    This class provides a clean API for using NewsLookout as a library,
    allowing easy integration into other Python applications.

    Attributes:
        config_file (str): Path to configuration file
        app_config (ConfigManager): Configuration manager instance
        queue_manager (QueueManager): Queue and worker manager
        is_running (bool): Whether the app is currently running
        run_thread (threading.Thread): Background execution thread

    Example:
        >>> app = NewsLookoutApp('config.conf')
        >>> app.run(run_date='2026-01-21')
        >>> stats = app.get_statistics()
    """

    def __init__(self, config_file: str, run_date: Optional[str] = None):
        """
        Initialize the NewsLookout application.

        Args:
            config_file (str): Path to the configuration file
            run_date (str, optional): Date to scrape in 'YYYY-MM-DD' format.
                                     Defaults to today.

        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If config file is invalid
        """
        self.config_file = config_file
        self.run_date = run_date or datetime.now().strftime('%Y-%m-%d')
        self.app_config = None
        self.queue_manager = None
        self.is_running = False
        self.run_thread = None
        self._stats = {
            'urls_discovered': 0,
            'urls_processed': 0,
            'urls_failed': 0,
            'data_processed': 0,
            'start_time': None,
            'end_time': None
        }

        # Validate config file exists
        if not os.path.isfile(config_file):
            raise FileNotFoundError(f"Configuration file not found: {config_file}")

        # Initialize configuration
        self._initialize_config()

        print(f"NewsLookout v{__version__} initialized")

    def _initialize_config(self):
        """
        Initialize configuration and setup logging.

        Raises:
            ValueError: If configuration is invalid
        """
        try:
            # Read configuration
            self.app_config = ConfigManager(self.config_file, self.run_date)
            self.app_config.app_version = __version__

            # Setup logging if not already configured
            if not logging.getLogger().handlers:
                self._setup_logging()

            # Set PID file:
            NewsLookoutApp.set_pid_file(self.app_config.pid_file)

            logging.info(f"======== Started application NewsLookout v{__version__} ========")

            # check plugins directory, if incorrect then set to plugins subdirectory of script path:
            script_path = os.path.abspath(__file__)
            plugins_path = os.path.join(script_path, 'plugins')
            if not Path(self.app_config.plugins_dir).is_dir():
                self.app_config.plugins_dir = plugins_path
                logging.info(f"Changing plugins directory to: {plugins_path}")

            # Check and download NLTK data if needed
            checkAndGetNLTKData()

            # Initialize queue manager
            self.queue_manager = QueueManager()
            self.queue_manager.config(self.app_config)

            logging.info(f"Configuration loaded from: {self.config_file}")
            logging.info(f"Scraping data for date: {self.run_date}")

        except Exception as e:
            logging.error(f"Failed to initialize configuration: {e}")
            raise ValueError(f"Invalid configuration: {e}")

    def _setup_logging(self):
        """Setup logging configuration - FILE ONLY, no console."""
        from logging.handlers import RotatingFileHandler

        log_level = getattr(logging, self.app_config.logLevelStr.upper(), logging.INFO)

        # Remove all existing handlers to prevent duplicates
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        # Set log level
        root_logger.setLevel(log_level)

        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s:[%(levelname)s]:%(name)s:%(thread)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # Setup file handler ONLY
        if self.app_config.logfile:
            file_handler = RotatingFileHandler(
                filename=self.app_config.logfile,
                mode='a',
                maxBytes=self.app_config.max_logfile_size,
                backupCount=self.app_config.logfile_backup_count,
                encoding='utf-8'
            )
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)

        # Suppress third-party library logging
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('requests').setLevel(logging.WARNING)
        logging.getLogger('newspaper').setLevel(logging.WARNING)

        # Single console message to confirm logging setup
        print(f"Logging to: {self.app_config.logfile}")


    @staticmethod
    def set_pid_file(pid_file: str):
        """ Creates a new text file containing the process identifier.

        This serves as a kind of locking mechanism to prevent
         multiple instances of the application running at the same time.

        :param pid_file: File name to be used as the PID file.
        """
        # create PID file before starting the application:
        if os.path.isfile(pid_file):
            print(f'ERROR! Cannot start the application since the PID file already exists: {pid_file}')
            sys.exit(1)
        else:
            fp = None
            try:
                pidValue = os.getpid()
                with open(pid_file, 'wt', encoding='utf-8') as fp:  # create empty file
                    fp.write(str(pidValue))
                print(f'Using PID file: {pid_file}')
            except Exception as e:
                print(f'Error creating PID file: {e}, File: {pid_file}')
                sys.exit(1)
            finally:
                fp.close()

    def remove_pid_file(self):
        """ After completion of application run(), close down everything, and remove the pid file.
        """
        if os.path.isfile(self.app_config.pid_file):
            try:
                os.remove(self.app_config.pid_file)
            except Exception as e:
                logging.error("Error deleting PID file %s: %s", self.app_config.pid_file, e)
        else:
            logging.info(f"PID file {self.app_config.pid_file} does not exist, so unable to delete it.")


    def run(self, run_date: Optional[str] = None, max_runtime: Optional[int] = None,
            blocking: bool = True) -> Dict[str, Any]:
        """
        Run the web scraping process.

        Args:
            run_date (str, optional): Date to scrape in 'YYYY-MM-DD' format
            max_runtime (int, optional): Maximum runtime in seconds
            blocking (bool): If True, wait for completion. If False, run in background.

        Returns:
            dict: Statistics about the scraping run

        Example:
            >>> app = NewsLookoutApp('config.conf')
            >>> stats = app.run(run_date='2026-01-21', max_runtime=3600)
            >>> print(f"Processed {stats['urls_processed']} URLs")
        """
        if self.is_running:
            logging.warning("Application is already running")
            return self.get_statistics()

        # Update run date if provided
        if run_date:
            self.run_date = run_date
            self.app_config.rundate = ConfigManager.checkAndParseDate(run_date)
            self.queue_manager.runDate = self.app_config.rundate

        self._stats['start_time'] = datetime.now()
        self.is_running = True

        if blocking:
            self._execute(max_runtime)
            return self.get_statistics()
        else:
            # Run in background thread
            self.run_thread = threading.Thread(
                target=self._execute,
                args=(max_runtime,),
                daemon=False
            )
            self.run_thread.start()
            logging.info("NewsLookout started in background")
            return {"status": "running", "message": "Application running in background"}

    def _execute(self, max_runtime: Optional[int] = None):
        """
        Execute the scraping process.

        Args:
            max_runtime (int, optional): Maximum runtime in seconds
        """
        try:
            # Initialize plugins
            self.queue_manager.initPlugins()

            # Setup timeout if specified
            if max_runtime:
                timer = threading.Timer(max_runtime, self.stop)
                timer.daemon = True
                timer.start()
                logging.info(f"Set maximum runtime: {max_runtime} seconds")

            # Run all jobs
            self.queue_manager.runAllJobs()

            # Finish up
            self.queue_manager.shutdown()

            self._stats['end_time'] = datetime.now()

            # Update statistics
            self._update_statistics()

            logging.info("NewsLookout execution completed successfully")

        except KeyboardInterrupt:
            logging.info("Execution interrupted by user")
            self.stop()

        except Exception as e:
            logging.error(f"Error during execution: {e}")
            raise

        finally:
            self.is_running = False

    def start(self):
        """
        Start the application in background mode.

        This is equivalent to calling run(blocking=False).

        Example:
            >>> app = NewsLookoutApp('config.conf')
            >>> app.start()
            >>> # Do other work...
            >>> app.stop()
        """
        return self.run(blocking=False)

    def stop(self, timeout: int = 30):
        """
        Stop the running application gracefully.

        Args:
            timeout (int): Maximum seconds to wait for shutdown

        Example:
            >>> app = NewsLookoutApp('config.conf')
            >>> app.start()
            >>> time.sleep(60)
            >>> app.stop()
        """
        if not self.is_running:
            logging.warning("Application is not running")
            return

        logging.info("Stopping NewsLookout...")

        # Signal shutdown
        if self.queue_manager:
            self.queue_manager.shutdown()

        # Wait for background thread if it exists
        if self.run_thread and self.run_thread.is_alive():
            self.run_thread.join(timeout=timeout)

            if self.run_thread.is_alive():
                logging.warning(f"Application did not stop within {timeout} seconds")
            else:
                logging.info("Application stopped successfully")

        self.is_running = False
        self._stats['end_time'] = datetime.now()

    def _update_statistics(self):
        """Update internal statistics from queue manager."""
        if self.queue_manager and self.queue_manager.q_status:
            q_status = self.queue_manager.q_status
            self._stats.update({
                'urls_discovered': q_status.totalURLCount,
                'urls_processed': q_status.fetchCompletCount,
                'data_processed': q_status.dataOutputQsize
            })

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about the current or last run.

        Returns:
            dict: Dictionary containing statistics:
                - urls_discovered: Total URLs found
                - urls_processed: URLs successfully scraped
                - urls_failed: URLs that failed
                - data_processed: Number of items processed
                - start_time: When execution started
                - end_time: When execution ended
                - duration: Runtime in seconds
                - is_running: Current running status

        Example:
            >>> stats = app.get_statistics()
            >>> print(f"Processed {stats['urls_processed']} URLs")
            >>> print(f"Runtime: {stats['duration']} seconds")
        """
        self._update_statistics()

        stats = dict(self._stats)
        stats['is_running'] = self.is_running

        # Calculate duration
        if stats['start_time']:
            end = stats['end_time'] or datetime.now()
            duration = (end - stats['start_time']).total_seconds()
            stats['duration'] = duration
        else:
            stats['duration'] = 0

        return stats

    def get_plugin_status(self) -> Dict[str, str]:
        """
        Get status of all loaded plugins.

        Returns:
            dict: Map of plugin names to their current states

        Example:
            >>> status = app.get_plugin_status()
            >>> for plugin, state in status.items():
            ...     print(f"{plugin}: {state}")
        """
        if not self.queue_manager or not self.queue_manager.q_status:
            return {}

        return dict(self.queue_manager.q_status.currentState)

    def wait_for_completion(self, timeout: Optional[int] = None):
        """
        Wait for the application to complete (if running in background).

        Args:
            timeout (int, optional): Maximum seconds to wait. None = wait indefinitely.

        Returns:
            bool: True if completed, False if timeout reached

        Example:
            >>> app.start()
            >>> if app.wait_for_completion(timeout=3600):
            ...     print("Completed successfully")
            ... else:
            ...     print("Timeout reached")
        """
        if not self.run_thread:
            return True

        self.run_thread.join(timeout=timeout)
        return not self.run_thread.is_alive()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if self.is_running:
            self.stop()

    def __repr__(self):
        """String representation."""
        status = "running" if self.is_running else "stopped"
        return f"<NewsLookoutApp(status={status}, date={self.run_date})>"


# Convenience functions for quick usage
def scrape(config_file: str, run_date: Optional[str] = None,
           max_runtime: Optional[int] = None) -> Dict[str, Any]:
    """Convenience function to run a scraping job.

    Args:
        config_file (str): Path to configuration file
        run_date (str, optional): Date to scrape in 'YYYY-MM-DD' format
        max_runtime (int, optional): Maximum runtime in seconds

    Returns:
        dict: Statistics from the scraping run

    Example:
        >>> from newslookout import scrape
        >>> stats = scrape('config.conf', run_date='2026-01-13')
        >>> print(f"Processed {stats['urls_processed']} URLs")
    """

    run_stats = {'urls_discovered':0, 'urls_processed':0, 'data_processed':0, 'duration':0.0}
    app_inst = NewsLookoutApp(config_file, run_date=run_date)

    try:
        # Run the application - THIS BLOCKS UNTIL COMPLETE
        run_stats = app_inst.run(max_runtime=max_runtime, blocking=True)

    except KeyboardInterrupt:
        logging.info("Keyboard interrupt in main - shutdown already handled")
        print("\nShutdown complete.")
    except Exception as e:
        logging.error(f"Fatal error in main: {e}", exc_info=True)
        print(f"\nFatal error: {e}")
        sys.exit(1)
    finally:
        # Clean up
        app_inst.remove_pid_file()
        print("\nThe program has completed execution.")

    return run_stats


# Example usage when run as script
if __name__ == "__main__":
    import argparse

    try:
        parser = argparse.ArgumentParser(description='NewsLookout Web Scraping Application')
        parser.add_argument('-c', '--config', required=True, help='Configuration file path')
        parser.add_argument('-d', '--date', help='Run date (YYYY-MM-DD)')
        parser.add_argument('-t', '--timeout', type=int, help='Maximum runtime in seconds')

        args = parser.parse_args()

        # Run the application
        stats = scrape(args.config, run_date=args.date, max_runtime=args.timeout)

        # Print summary
        print("\n" + "="*50)
        print("Scraping Summary:")
        print("="*50)
        print(f"URLs Discovered: {stats['urls_discovered']}")
        print(f"URLs Processed:  {stats['urls_processed']}")
        print(f"Data Processed:  {stats['data_processed']}")
        print(f"Duration:        {stats['duration']:.1f} seconds")
        print("="*50)
    except Exception as e:
        logging.error(f"Fatal error in main: {e}", exc_info=True)


# End of file