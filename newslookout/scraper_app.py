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


__version__ = "2.1.0"
__author__ = "Sandeep Singh Sandhu"
__copyright__ = "Copyright 2021, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu"
__credits__ = ["Sandeep Singh Sandhu"]
__license__ = "GPL"
__maintainer__ = "Sandeep Singh Sandhu"
__email__ = "sandeep.sandhu@gmx.com"
__status__ = "Production"

##########

# import standard python libraries:
import platform
import sys
import logging
import logging.handlers
from datetime import datetime
from configparser import ConfigParser
import getopt
import os

# import project's python libraries:
from queue_manager import QueueManager
from scraper_utils import checkAndGetNLTKData
from config import ConfigManager

################


class NewsLookout:
    """ NewsLookout Web Scraping Application
    Main class that runs the entire application.
    """

    config_file = os.path.join('conf', 'newslookout.conf')
    run_date = datetime.now().strftime('%Y-%m-%d')
    app_config = None
    app_queue_manager = None

    def __init__(self):
        """
        Initialize the application class
        by reading the program arguments, validating them
        and setting the configuration data accordingly
        """
        self.print_banner()

    @staticmethod
    def print_usage_and_exit():
        print('Usage: newslookout -c <configuration file> -d <run date as YYYY-MM-dd>')
        sys.exit(1)

    def read_cmdline_args(self, sysargs: list):
        """  Read the command line arguments and parse them.

        :param sysargs: The command line arguments passed from the OS.
        :return: Nothing
        """
        try:
            if len(sysargs) < 3:
                NewsLookout.print_usage_and_exit()
            opts, args = getopt.getopt(sysargs[1:], "h:c:d:", ["configfile = ", "rundate = "])
            for opt, arg in opts:
                if opt in ("-h", "--help"):
                    NewsLookout.print_usage_and_exit()
                elif opt in ("-c", "--configfile"):
                    self.config_file = arg
                elif opt in ("-d", "--rundate"):
                    self.run_date = arg
        except getopt.GetoptError as e:
            print("Error reading command line options:", e)
            NewsLookout.print_usage_and_exit()

    def readConfigFile(self):
        """ Utility function to read the configuration file,
        and parse it into the dictionary structure used by the application.
        """
        self.app_config = ConfigManager(self.config_file, self.run_date)
        self.app_config.app_version = __version__
        config_obj = ConfigParser()
        try:
            config_obj.read_file(open(self.config_file, encoding='utf-8'))
            # self.configData['configReader'] = config_obj
        except Exception as e:
            print(f'ERROR: Configuration file "{self.config_file}" could not be read: {e}')
            sys.exit(1)

    @staticmethod
    def print_banner():
        """ Prints the startup banner. """
        print(f"--- NewsLookout Web Scraping Application, Version {__version__} ---")
        print('Running on: Python version ' +
              f'{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}' +
              f' ({platform.system()})')
        print(f'Started application at: {datetime.now().strftime("%Y-%b-%d %H:%M:%S")}')

    def run(self):
        """Run the application after configuring the queue manager and initialising the plugins.
        """
        logging.info(f'--- NewsLookout Web Scraping Application, (version {__version__}) ---')
        logging.info(f'Retrieving data for run date: {self.run_date}')
        checkAndGetNLTKData()
        self.app_queue_manager.config(self.app_config)
        # load and initialize all the plugins after everything has been configured.
        self.app_queue_manager.initPlugins()
        self.app_queue_manager.runAllJobs()
        self.app_queue_manager.finishAllTasks()

    def config(self, sys_argv: list):
        """ Configure the application using the command line arguments

        :param sys_argv: Command line arguments
        :return:
        """
        self.read_cmdline_args(sys_argv)
        # initialise the queue manager:
        print(f'Retrieving data for run date: {self.run_date}')
        self.app_queue_manager = QueueManager()
        # read and setup the configuration:
        print(f'Reading configuration from: {self.config_file}')
        self.readConfigFile()
        # setup the logging mechanism:
        NewsLookout.setup_logger(self.app_config.logfile,
                                 log_level=self.app_config.logLevelStr,
                                 max_size_byte=self.app_config.max_logfile_size,
                                 backup_count=self.app_config.logfile_backup_count)

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

    @staticmethod
    def setup_logger(logfile: str, log_level: str = 'INFO', max_size_byte: int = 1024000, backup_count: int = 10):
        # setup logger with DEBUG level as default
        logLevel = logging.DEBUG
        if log_level == 'INFO':
            logLevel = logging.INFO
        elif log_level == 'WARN':
            logLevel = logging.WARNING
        elif log_level == 'ERROR':
            logLevel = logging.ERROR
        # Create formatter for the logging:
        fh_formatter = logging.Formatter('%(asctime)s:[%(levelname)s]:%(name)s:%(thread)s: %(message)s',
                                         datefmt='%Y-%m-%d %H:%M:%S')
        # Set up the default root logger to do nothing
        logging.basicConfig(
            handlers=[logging.NullHandler()],
            level=logLevel,
            format='%(asctime)s:%(levelname)s:%(name)s:%(thread)s: %(message)s')
        if logfile is not None:
            print(f'Logging events to file: {logfile}')
            # Create file handler
            scraperLogFileHandler = logging.handlers.RotatingFileHandler(
                filename=logfile, mode='a', maxBytes=max_size_byte,
                backupCount=backup_count, encoding='utf-8')
            scraperLogFileHandler.setFormatter(fh_formatter)
            # add to root logger
            logging.getLogger('').addHandler(scraperLogFileHandler)

    # # end of application class definition ##


def main():
    # global app_inst
    # instantiate the main application class:
    app_inst = NewsLookout()
    # configure the application:
    app_inst.config(sys.argv)
    NewsLookout.set_pid_file(app_inst.app_config.pid_file)
    # run the application:
    app_inst.run()
    # clean-up before exiting:
    app_inst.remove_pid_file()
    print("The program has completed execution successfully.")


# the main application class instance is a global variable:
# global app_inst

if __name__ == "__main__":
    main()

# # end of file ##
