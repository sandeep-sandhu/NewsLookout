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


__version__ = "1.9.9"
__author__ = "Sandeep Singh Sandhu"
__copyright__ = "Copyright 2021, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu"
__credits__ = ["Sandeep Singh Sandhu"]
__license__ = "GPL"
__maintainer__ = "Sandeep Singh Sandhu"
__email__ = "sandeep.sandhu@gmx.com"
__status__ = "Production"

##########

# import standard python libraries:
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

    config_file = os.path.join('conf','newslookout.conf')
    run_date = datetime.now().strftime('%Y-%m-%d')
    app_config = None
    app_queue_manager = None

    def __init__(self):
        """
        Initialize the application class
        by reading the program arguments, validating them
        and setting the configuration data accordingly
        """
        print("NewsLookout Web Scraping Application, Version ", __version__)
        print("Python version: ", sys.version)

    @staticmethod
    def printUsageAndExit():
        print('Usage: newslookout -c <configuration file> -d <run date as YYYY-MM-dd>')
        sys.exit(1)

    def readArgs(self):
        """ Read command line arguments and parse them
        """
        try:
            if len(sys.argv) < 3:
                self.printUsageAndExit()
            opts, args = getopt.getopt(sys.argv[1:], "h:c:d:", ["configfile = ", "rundate = "])
            for opt, arg in opts:
                if opt in ("-h", "--help"):
                    self.printUsageAndExit()
                elif opt in ("-c", "--configfile"):
                    self.config_file = arg
                elif opt in ("-d", "--rundate"):
                    self.run_date = arg
        except getopt.GetoptError as e:
            print("Error reading command line options:", e)
            self.printUsageAndExit()

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

    def run(self):
        """Run the application job after configuring the main queue
        """
        logging.info(f'--- NewsLookout Application (version {__version__}) ---')
        logging.info(f'--- Python version: {sys.version} ---')
        logging.info(f'Started retrieving data for run date: {self.run_date} ---')
        checkAndGetNLTKData()
        self.app_queue_manager.config(self.app_config)
        self.app_queue_manager.runAllJobs()
        self.app_queue_manager.finishAllTasks()

    def config(self):
        self.readArgs()
        # initialise the queue manager:
        print(f'Run date: {app_inst.run_date}')
        self.app_queue_manager = QueueManager()
        # read and setup the configuration:
        print(f'Reading configuration from: {self.config_file}')
        self.readConfigFile()
        # setup the logging mechanism:
        print(f'Logging events to file: {self.app_config.logfile}')
        self.setupLogger()

    def set_pid_file(self):
        # create PID file before starting the application:
        if os.path.isfile(self.app_config.pid_file):
            print(f'ERROR! Cannot start the application since PID file exists: {self.app_config.pid_file}')
            sys.exit(1)
        else:
            fp = None
            try:
                pidValue = os.getpid()
                fp = open(self.app_config.pid_file, 'wt', encoding='utf-8')  # create empty file
                fp.write(str(pidValue))
            except Exception as e:
                print(f'Error creating PID file: {e}, File: {self.app_config.pid_file}')
                sys.exit(1)
            finally:
                fp.close()

    def remove_pid_file(self):
        # After completion of run(), close down everything, remove the pid file:
        if os.path.isfile(app_inst.app_config.pid_file):
            try:
                os.remove(app_inst.app_config.pid_file)
            except Exception as e:
                logging.error("Error deleting PID file %s: %s", app_inst.app_config.pid_file, e)
                sys.exit(1)
        else:
            logging.info("PID file %s does not exist, so unable to delete it when shutting down.",
                         app_inst.app_config.pid_file)

    def setupLogger(self):
        # setup logger with DEBUG level as default
        logLevel = logging.DEBUG
        if self.app_config.logLevelStr == 'INFO':
            logLevel = logging.INFO
        elif self.app_config.logLevelStr == 'WARN':
            logLevel = logging.WARNING
        elif self.app_config.logLevelStr == 'ERROR':
            logLevel = logging.ERROR
        # Create file handler
        scraperLogFileHandler = logging.handlers.RotatingFileHandler(
            filename=self.app_config.logfile, mode='a', maxBytes=self.app_config.max_logfile_size,
            backupCount=self.app_config.logfile_backup_count, encoding='utf-8')
        # Create formatter for the file handler
        fh_formatter = logging.Formatter('%(asctime)s:[%(levelname)s]:%(name)s:%(thread)s: %(message)s',
                                         datefmt='%Y-%m-%d %H:%M:%S')
        scraperLogFileHandler.setFormatter(fh_formatter)
        # Set up the default root logger to do nothing
        logging.basicConfig(
            handlers=[logging.NullHandler()],
            level=logLevel,
            format='%(asctime)s:%(levelname)s:%(name)s:%(thread)s: %(message)s')
        # add to root logger
        logging.getLogger('').addHandler(scraperLogFileHandler)

    # # end of application class definition ##


def main():
    global app_inst
    # instantiate the main application class
    app_inst = NewsLookout()
    app_inst.config()

    app_inst.set_pid_file()
    print(f'Using PID file: {app_inst.app_config.pid_file}')
    # run the application:
    app_inst.run()
    # clean-up before exiting:
    app_inst.remove_pid_file()
    print("The program has completed execution successfully.")


# the main application class instance is a global variable:
global app_inst

if __name__ == "__main__":
    main()

# # end of file ##
