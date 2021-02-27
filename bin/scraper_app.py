#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: scraper_app.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-01-14
 Purpose: Main class for the web scraping and news text processing application
 Copyright 2021, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu, sandeep.sandhu@gmx.com

 Usage:
 python scraper_app.py -c <configfile> -d <rundate>

 The default location of configuration file is: conf/scraper.conf
 Date format for rundate: YYYY-MM-DD, e.g. 2019-12-31

 DISCLAIMER: This software is intended for demonstration and educational purposes only.
 Before using it for web scraping any website, always consult that website's terms of use.
 Do not use this software to fetch any data from any website that has forbidden use of web
 scraping or similar mechanisms, or violates its terms of use in any other way. The author is
 not responsible for such kind of inappropriate use of this software.

"""

##########

__version__ = "1.7"
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
# import web retrieval and text processing python libraries:
import newspaper
import nltk
import os
# import project's python libraries:
from scraper_utils import checkAndParseDate
from queue_manager import queueManager
from scraper_utils import checkAndGetNLTKData
################


class NewsLookout:
    """ NewsLookout Web Scraping Application
    Main class that runs the entire application.
    """

    configData = {
                  'version': __version__, 'logLevelStr': 'INFO', 'rundate': datetime.now(),
                  'logfile': os.path.join('logs', 'scraper.log'), 'configfile': os.path.join('conf', 'scraper.conf'),
                  'pid_file': os.path.join('logs', 'scraper.pid'), 'newspaper_config': None,
                  'data_dir': 'data', 'plugins_dir': 'plugins', 'worker_threads': 2,
                  'user_agent':
                  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3",
                  'fetch_timeout': 60, 'completed_urls_datafile': os.path.join('data', 'completed_urls.db'),
                  'proxies': {"http": None, "https": None}
                  }

    appQueue = None

    def __init__(self):
        """Initialize the application class
        by reading the program arguments, validating them
        and setting the configuration data accordingly """

        print("NewsLookout Web Scraping Application, Version ", self.configData['version'])
        self.readArgs()
        self.appQueue = queueManager()
        self.readConfig()
        self.applyConfig()

        if os.path.isfile(self.configData['pid_file']):
            print("ERROR: Unable to start the application since PID file exists: ", self.configData['pid_file'])
            sys.exit(1)

        else:
            # create empty file
            fp = None

            try:
                pidValue = os.getpid()
                fp = open(self.configData['pid_file'], 'wt', encoding='utf-8')
                fp.write(str(pidValue))

            except Exception as e:
                print("Error creating PID file: ", e, self.configData['pid_file'])
                sys.exit(1)

            finally:
                fp.close()

    def readArgs(self):
        """ read command line arguments and parse them """

        try:
            opts, args = getopt.getopt(sys.argv[1:],
                                       "h:c:d:",
                                       ["configfile = ", "rundate = "]
                                       )

        except getopt.GetoptError as e:
            print("Error reading command line options: %s", e)
            print('Usage: scraper_app.py -c <configuration file> -d <run date>')
            sys.exit(2)

        for opt, arg in opts:

            if opt in ("-h", "--help"):
                print('Usage: scraper_app.py -c <configuration file> -d <run date>')
                sys.exit(0)

            elif opt in ("-c", "--configfile"):
                self.configData['configfile'] = arg

            elif opt in ("-d", "--rundate"):
                self.configData['rundate'] = arg

        try:
            # check if config file exists, exit if it doesnt exist
            if os.path.isfile(self.configData['configfile']):
                print('Reading configuration settings from file:', self.configData['configfile'])
            else:
                print('ERROR: Configuration file (', self.configData['configfile'], ') not found')
                sys.exit(1)

        except Exception as e:
            print('ERROR: Configuration file (', self.configData['configfile'], ') not found:', e)
            sys.exit(1)

    def readConfig(self):
        """Utility function to read the configuration file,
        and parse it into the dictionary structure used by the application."""

        configur = ConfigParser()
        try:
            configur.read_file(open(self.configData['configfile'], encoding='utf-8'))

        except Exception as e:
            print('Error opening configuration file (', self.configData['configfile'], '):', e)
            sys.exit(1)

        try:
            self.configData['configReader'] = configur

            self.configData['logLevelStr'] = configur.get('logging', 'log_level').strip()

            self.configData['max_logfile_size'] = configur.get('logging', 'max_logfile_size').strip()
            try:
                self.configData['max_logfile_size'] = max(1000000, int(self.configData['max_logfile_size']))
            except Exception as e:
                print("Error reading max_logfile_size from configuration file: %s", e)
                self.configData['max_logfile_size'] = 10240000

            self.configData['enabledPlugins'] = configur.get('plugins', 'enabled').strip()

            self.configData['logfile'] = configur.get('installation', 'log_file').strip()
            self.configData['pid_file'] = configur.get('installation', 'pid_file').strip()
            self.configData['data_dir'] = configur.get('installation', 'data_dir').strip()
            self.configData['cookie_file'] = configur.get('installation', 'cookie_file').strip()
            self.configData['plugins_dir'] = configur.get('installation', 'plugins_dir').strip()
            self.configData['install_prefix'] = configur.get('installation', 'prefix').strip()
            self.configData['completed_urls_datafile'] = configur.get('installation', 'completed_urls_datafile').strip()

            self.configData['save_html'] = configur.get('operation', 'save_html').strip()
            self.configData['user_agent'] = configur.get('operation', 'user_agent').strip()
            self.configData['proxy_url_http'] = configur.get('operation', 'proxy_url_http').strip()
            self.configData['proxy_url_https'] = configur.get('operation', 'proxy_url_https').strip()
            self.configData['proxy_user'] = configur.get('operation', 'proxy_user').strip()
            self.configData['proxy_password'] = configur.get('operation', 'proxy_password').strip()
            self.configData['proxy_ca_certfile'] = configur.get('operation', 'proxy_ca_certfile').strip()

            self.configData['worker_threads'] = configur.getint('operation', 'worker_threads')
            self.configData['retry_count'] = configur.getint('operation', 'retry_count')
            self.configData['retry_wait_sec'] = configur.getint('operation', 'retry_wait_sec')
            self.configData['retry_wait_rand_min_sec'] = configur.getint('operation', 'retry_wait_rand_min_sec')
            self.configData['retry_wait_rand_max_sec'] = configur.getint('operation', 'retry_wait_rand_max_sec')
            self.configData['fetch_timeout'] = configur.getint('operation', 'fetch_timeout')
            self.configData['connect_timeout'] = configur.getint('operation', 'connect_timeout')

            self.configData['rundate'] = checkAndParseDate(self.configData['rundate'])

        except Exception as e:
            print("Error reading configuration from file (", self.configData['configfile'], "): ", e)

    def applyConfig(self):
        """ apply configuration """

        os.environ['HTTP_PROXY'] = ''
        os.environ['HTTPS_PROXY'] = ''

        try:
            newspaper_config = newspaper.Config()
            newspaper_config.memoize_articles = True
            newspaper_config.http_success_only = True
            newspaper_config.fetch_images = False
            newspaper_config.number_threads = 2
            newspaper_config.browser_user_agent = self.configData['user_agent']
            newspaper_config.request_timeout = self.configData['fetch_timeout']

            # add this to config data
            self.configData['newspaper_config'] = newspaper_config

            # set OS environment variables for proxy server:
            if len(self.configData['proxy_url_http']) > 3 and len(self.configData['proxy_url_https']) > 3:

                os.environ['HTTP_PROXY'] = self.configData['proxy_url_http']

                os.environ['HTTPS_PROXY'] = self.configData['proxy_url_https']

                self.configData['proxies'] = {"http": self.configData['proxy_url_http'],
                                              "https": self.configData['proxy_url_https']}
            # else:
            #     print("INFO: Not using any proxy servers: "
            #           , self.configData['proxy_url_http']
            #           , " or "
            #           , self.configData['proxy_url_https'])

            nltk.set_proxy(self.configData['proxies'])
            self.configData['newspaper_config'].proxies = self.configData['proxies']
            # print("INFO: For NLTK, using Proxy configuration: ", nltk.getproxies())

        except Exception as e:
            print("ERROR: Unable to set proxy parameters: %s", e)

    def run(self, currVersion):
        """Run the application job after configuring the main queue """

        self.configData['version'] = currVersion

        logging.info('--- NewsLookout Application (version %s) has started retrieving data for run date: %s ---',
                     self.configData['version'],
                     self.configData['rundate'])

        checkAndGetNLTKData()

        self.appQueue.config(self.configData)

        self.appQueue.runAllJobs()

        self.appQueue.runDataProcessingJobs()

        self.appQueue.finishAllTasks()
    # # end of application class definition ##


# the main application class instance is a global variable
global app_inst

if __name__ == "__main__":

    global app_inst

    # instantiate the main application class
    app_inst = NewsLookout()

    # setup logger
    logLevel = logging.DEBUG

    if app_inst.configData['logLevelStr'] == 'INFO':
        logLevel = logging.INFO
    elif app_inst.configData['logLevelStr'] == 'WARN':
        logLevel = logging.WARNING
    elif app_inst.configData['logLevelStr'] == 'ERROR':
        logLevel = logging.ERROR
    else:
        logLevel = logging.DEBUG

    # Create file handler
    scraperLogFileHandler = logging.handlers.RotatingFileHandler(
                                                                 filename=app_inst.configData['logfile'],
                                                                 mode='a',
                                                                 maxBytes=app_inst.configData['max_logfile_size'],
                                                                 backupCount=10,
                                                                 encoding='utf-8')
    # Create formatter for the file handler
    fh_formatter = logging.Formatter(
                                     '%(asctime)s:[%(levelname)s]:%(name)s:%(thread)s: %(message)s',
                                     datefmt='%Y-%m-%d %H:%M:%S'
                                     )
    scraperLogFileHandler.setFormatter(fh_formatter)

    # Set up the default root logger to do nothing
    logging.basicConfig(
                        handlers=[logging.NullHandler()],
                        level=logLevel,
                        format='%(asctime)s:%(levelname)s:%(name)s:%(thread)s: %(message)s'
                        )
    # add to root logger
    logging.getLogger('').addHandler(scraperLogFileHandler)
    print("Logging to file:", app_inst.configData['logfile'])

    # run the application:
    app_inst.run(__version__)

    # close down everything:
    if os.path.isfile(app_inst.configData['pid_file']):
        try:
            # delete: app_inst.configData['pid_file']
            os.remove(app_inst.configData['pid_file'])
        except Exception as e:
            logging.error("Error removing PID file %s: %s", app_inst.configData['pid_file'], e)
            sys.exit(1)
    else:
        logging.info("PID file %s does not exist, so unable to delete it.",
                     app_inst.configData['pid_file'])

# # end of file ##
