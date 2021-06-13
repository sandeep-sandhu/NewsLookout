#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: scraper_app.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-06-10
 Purpose: Main class for the web scraping and news text processing application
 Copyright 2021, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu, sandeep.sandhu@gmx.com

 Usage:
 python scraper_app.py -c <configfile> -d <rundate>

 The default location of configuration file is: conf/newslookout.conf
 Java SimpleDate Format for rundate argument, e.g. 2019-12-31 is: ${current_date:yyyy-MM-dd}

 Notice:
 This software is intended for demonstration and educational purposes only. This software is
 experimental and a work in progress. Under no circumstances should these files be used in
 relation to any critical system(s). Use of these files is at your own risk.

 Before using it for web scraping any website, always consult that website's terms of use.
 Do not use this software to fetch any data from any website that has forbidden use of web
 scraping or similar mechanisms, or violates its terms of use in any other way. The author is
 not liable for such kind of inappropriate use of this software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
 PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
 FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 DEALINGS IN THE SOFTWARE.

"""

##########

__version__ = "1.9.1"
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
import tempfile

# import web retrieval and text processing python libraries:
import newspaper
import nltk
import os

# import project's python libraries:
from scraper_utils import checkAndParseDate
from scraper_utils import checkAndSanitizeConfigInt
from scraper_utils import checkAndSanitizeConfigString
from queue_manager import queueManager
from scraper_utils import checkAndGetNLTKData

################


class NewsLookout:
    """ NewsLookout Web Scraping Application
    Main class that runs the entire application.
    """
    configData = {
                  'version': __version__, 'logLevelStr': 'INFO', 'rundate': datetime.now().strftime('%Y-%m-%d'),
                  'logfile': os.path.join('logs', 'newslookout.log'), 'configfile': os.path.join('conf', 'newslookout.conf'),
                  'pid_file': os.path.join('logs', 'newslookout.pid'), 'newspaper_config': None,
                  'data_dir': 'data', 'plugins_dir': 'plugins', 'plugins_contributed_dir': 'plugins_contrib',
                  'master_data_dir': os.path.join('data', 'master_data'), 'proxy_url_http': '', 'proxy_url_https': '',
                  'recursion_level': 1, 'user_agent':
                  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3",
                  'fetch_timeout': 60, 'completed_urls_datafile': os.path.join('data', 'completed_urls.db'),
                  'proxies': {"http": None, "https": None}, 'logfile_backup_count': 30, 'verify_ca_cert': True
                  }

    appQueue = None

    def __init__(self):
        """
        Initialize the application class
        by reading the program arguments, validating them
        and setting the configuration data accordingly
        """
        print("NewsLookout Web Scraping Application, Version ", self.configData['version'])
        print("Python version: ", sys.version)

    def config(self):
        self.readArgs()
        self.appQueue = queueManager()
        configurObj = self.readConfigFile()
        self.readConfigEnviron(configurObj)
        self.readConfigOperations(configurObj)
        self.applyConfig()

    def printUsageAndExit(self):
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
                    self.configData['configfile'] = arg
                elif opt in ("-d", "--rundate"):
                    self.configData['rundate'] = arg
        except getopt.GetoptError as e:
            print("Error reading command line options:", e)
            self.printUsageAndExit()

    def readConfigFile(self):
        """ Utility function to read the configuration file,
        and parse it into the dictionary structure used by the application.
        """
        configur = ConfigParser()
        try:
            configur.read_file(open(self.configData['configfile'], encoding='utf-8'))
            self.configData['configReader'] = configur
        except Exception as e:
            print(f'ERROR: Configuration file "{self.configData["configfile"]}" could not be read: {e}')
            sys.exit(1)
        return(configur)

    def readConfigEnviron(self, configur):
        """ Utility function to read the configuration file,
        and parse it into the dictionary structure used by the application.
        """
        try:
            self.configData['logLevelStr'] = checkAndSanitizeConfigString(
                configur,
                'logging',
                'log_level',
                default='INFO'
                )
            self.configData['logfile_backup_count'] = checkAndSanitizeConfigInt(
                configur,
                'logging',
                'logfile_backup_count',
                default=30,
                maxValue=100,
                minValue=1
                )
            self.configData['max_logfile_size'] = checkAndSanitizeConfigInt(
                configur,
                'logging',
                'max_logfile_size',
                default=1024 * 1024,
                maxValue=1024 * 1024 * 10,
                minValue=10240
                )
            self.configData['install_prefix'] = checkAndSanitizeConfigString(
                configur,
                'installation',
                'prefix',
                default=os.getcwd())
            self.configData['data_dir'] = checkAndSanitizeConfigString(
                configur,
                'installation',
                'data_dir',
                default=None
                )
            try:
                # first check if data directory self.configData['data_dir'] of given date exists, or not
                if self.configData['data_dir'] is not None and os.path.isdir(self.configData['data_dir']) is False:
                    # data dir does not exist, so try creating it:
                    os.mkdir(self.configData['data_dir'])
                elif self.configData['data_dir'] is None:
                    print('Error identifying data directory (', self.configData['data_dir'], '):')
                    sys.exit(1)
            except Exception as theError:
                print("Exception caught creating data directory",
                      self.configData['data_dir'], ": ", theError)
            self.configData['master_data_dir'] = checkAndSanitizeConfigString(
                configur,
                'installation',
                'master_data_dir',
                default=os.path.join(self.configData['data_dir'], 'master_data')
                )
            try:
                # first check if data directory self.configData['data_dir'] of given date exists, or not
                if os.path.isdir(self.configData['master_data_dir']) is False:
                    # data dir does not exist, so try creating it:
                    os.mkdir(self.configData['master_data_dir'])
            except Exception as theError:
                print("Exception caught creating directory to save Master-data",
                      self.configData['master_data_dir'], ": ", theError)
            self.configData['plugins_dir'] = checkAndSanitizeConfigString(
                configur,
                'installation',
                'plugins_dir',
                default=os.path.join(self.configData['install_prefix'], 'plugins')
                )
            self.configData['plugins_contributed_dir'] = checkAndSanitizeConfigString(
                configur,
                'installation',
                'plugins_contributed_dir',
                default=os.path.join(self.configData['install_prefix'], 'plugins_contrib')
                )
            self.configData['logfile'] = checkAndSanitizeConfigString(
                configur,
                'installation',
                'log_file',
                default=os.path.join(self.configData['data_dir'], 'newslookout.log')
                )
            self.configData['pid_file'] = checkAndSanitizeConfigString(
                configur,
                'installation',
                'pid_file',
                default=os.path.join(tempfile.gettempdir(), 'newslookout.pid')
                )
            self.configData['cookie_file'] = checkAndSanitizeConfigString(
                configur,
                'installation',
                'cookie_file',
                default=os.path.join(self.configData['data_dir'], 'cookies.txt')
                )
            self.configData['completed_urls_datafile'] = checkAndSanitizeConfigString(
                configur,
                'installation',
                'completed_urls_datafile',
                default=os.path.join(self.configData['data_dir'], 'completed_urls.db')
                )
        except Exception as e:
            print("Error reading environment configuration from file (", self.configData['configfile'], "): ", e)

    def readConfigOperations(self, configur):
        """ Utility function to read the configuration file,
        and parse it into the dictionary structure used by the application.
        """
        try:
            self.configData['proxy_ca_certfile'] = checkAndSanitizeConfigString(
                configur,
                'operation',
                'proxy_ca_certfile',
                default=None  # os.path.join(self.configData['data_dir'], 'proxy_ca.crt')
                )
            if len(self.configData['proxy_ca_certfile']) < 2:
                self.configData['proxy_ca_certfile'] = None
            self.configData['verify_ca_cert'] = checkAndSanitizeConfigString(
                configur,
                'operation',
                'verify_ca_cert',
                default='True'
                )
            if self.configData['verify_ca_cert'].upper() == 'FALSE':
                self.configData['verify_ca_cert'] = False
            else:
                self.configData['verify_ca_cert'] = True
            self.configData['save_html'] = configur.get('operation', 'save_html').strip()
            self.configData['user_agent'] = configur.get('operation', 'user_agent').strip()
            self.configData['proxy_url_http'] = configur.get('operation', 'proxy_url_http').strip()
            self.configData['proxy_url_https'] = configur.get('operation', 'proxy_url_https').strip()
            self.configData['proxy_user'] = configur.get('operation', 'proxy_user').strip()
            self.configData['proxy_password'] = configur.get('operation', 'proxy_password').strip()
            self.configData['recursion_level'] = checkAndSanitizeConfigInt(
                configur,
                'operation',
                'recursion_level',
                default=1,
                maxValue=4,
                minValue=1
                )
            self.configData['retry_count'] = checkAndSanitizeConfigInt(
                configur,
                'operation',
                'retry_count',
                default=3,
                maxValue=10,
                minValue=1
                )
            self.configData['retry_wait_sec'] = checkAndSanitizeConfigInt(
                configur,
                'operation',
                'retry_wait_sec',
                default=5,
                maxValue=600,
                minValue=1
                )
            self.configData['retry_wait_rand_min_sec'] = checkAndSanitizeConfigInt(
                configur,
                'operation',
                'retry_wait_rand_min_sec',
                default=3,
                maxValue=600,
                minValue=0
                )
            self.configData['retry_wait_rand_max_sec'] = checkAndSanitizeConfigInt(
                configur,
                'operation',
                'retry_wait_rand_max_sec',
                default=10,
                maxValue=600,
                minValue=1
                )
            self.configData['fetch_timeout'] = checkAndSanitizeConfigInt(
                configur,
                'operation',
                'fetch_timeout',
                default=3,
                maxValue=600,
                minValue=3
                )
            self.configData['connect_timeout'] = checkAndSanitizeConfigInt(
                configur,
                'operation',
                'connect_timeout',
                default=3,
                maxValue=600,
                minValue=3
                )
            self.configData['rundate'] = checkAndParseDate(self.configData['rundate'])
        except Exception as e:
            print("Error reading operational configuration from file (", self.configData['configfile'], "): ", e)

    def applyConfig(self):
        """ apply configuration
        """
        os.environ['HTTP_PROXY'] = ''
        os.environ['HTTPS_PROXY'] = ''
        try:
            newspaper_config = newspaper.Config()
            newspaper_config.memoize_articles = False
            newspaper_config.http_success_only = True
            newspaper_config.fetch_images = False
            newspaper_config.number_threads = 2
            newspaper_config.browser_user_agent = self.configData['user_agent']
            newspaper_config.request_timeout = self.configData['fetch_timeout']
            newspaper_config.use_cached_categories = False
            # add this to config data
            self.configData['newspaper_config'] = newspaper_config
            # set OS environment variables for proxy server:
            if len(self.configData['proxy_url_http']) > 3 and len(self.configData['proxy_url_https']) > 3:
                os.environ['HTTP_PROXY'] = self.configData['proxy_url_http']
                os.environ['HTTPS_PROXY'] = self.configData['proxy_url_https']
                self.configData['proxies'] = {"http": self.configData['proxy_url_http'],
                                              "https": self.configData['proxy_url_https']}
            else:
                os.environ['HTTP_PROXY'] = ''
                os.environ['HTTPS_PROXY'] = ''
                self.configData['proxy_url_http'] = None
                self.configData['proxy_url_https'] = None
                self.configData['proxies'] = {}

            nltk.set_proxy(self.configData['proxies'])
            self.configData['newspaper_config'].proxies = self.configData['proxies']
            # print("INFO: For NLTK, using Proxy configuration: ", nltk.getproxies())
        except Exception as e:
            print("ERROR: Unable to set proxy parameters: %s", e)

    def run(self):
        """Run the application job after configuring the main queue
        """
        logging.info('--- NewsLookout Application (version %s) has started retrieving data for run date: %s ---',
                     self.configData['version'],
                     self.configData['rundate'])
        logging.info('--- Python version: %s ---', sys.version)
        checkAndGetNLTKData()
        self.appQueue.config(self.configData)

        self.appQueue.runAllJobs()

        self.appQueue.finishAllTasks()
    # # end of application class definition ##


def main():
    global app_inst
    # instantiate the main application class
    app_inst = NewsLookout()
    app_inst.config()

    # setup logger with DEBUG level as default
    logLevel = logging.DEBUG
    if app_inst.configData['logLevelStr'] == 'INFO':
        logLevel = logging.INFO
    elif app_inst.configData['logLevelStr'] == 'WARN':
        logLevel = logging.WARNING
    elif app_inst.configData['logLevelStr'] == 'ERROR':
        logLevel = logging.ERROR
    # Create file handler
    scraperLogFileHandler = logging.handlers.RotatingFileHandler(
                                                                 filename=app_inst.configData['logfile'],
                                                                 mode='a',
                                                                 maxBytes=app_inst.configData['max_logfile_size'],
                                                                 backupCount=app_inst.configData['logfile_backup_count'],
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
    print("Saving data to:", app_inst.configData['data_dir'])
    print("Using PID file:", app_inst.configData['pid_file'])
    print("Logging events to file:", app_inst.configData['logfile'])

    # create PID file before starting the application:
    if os.path.isfile(app_inst.configData['pid_file']):
        print("ERROR: Unable to start the application since PID file exists: ", app_inst.configData['pid_file'])
        sys.exit(1)
    else:
        fp = None
        try:
            pidValue = os.getpid()
            fp = open(app_inst.configData['pid_file'], 'wt', encoding='utf-8')  # create empty file
            fp.write(str(pidValue))
        except Exception as e:
            print("Error creating PID file: ", e, app_inst.configData['pid_file'])
            sys.exit(1)
        finally:
            fp.close()
    # run the application:
    app_inst.run()

    # After completion of run(), close down everything, remove the pid file:
    if os.path.isfile(app_inst.configData['pid_file']):
        try:
            os.remove(app_inst.configData['pid_file'])
        except Exception as e:
            logging.error("Error removing PID file %s: %s", app_inst.configData['pid_file'], e)
            sys.exit(1)
    else:
        logging.info("PID file %s does not exist, so unable to delete it.", app_inst.configData['pid_file'])
    print("The program has completed execution successfully.")


# the main application class instance is a global variable
global app_inst

if __name__ == "__main__":
    main()


# # end of file ##
