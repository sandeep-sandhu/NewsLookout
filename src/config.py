#!/usr/bin/env python
# -*- coding: utf-8 -*-

# #########################################################################################################
#                                                                                                         #
# File name: config.py                                                                                    #
# Application: The NewsLookout Web Scraping Application                                                   #
# Date: 2021-06-23                                                                                        #
# Purpose: Configuration helper class that performs configuraition operations for this application         #
# Copyright 2021, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu, sandeep.sandhu@gmx.com  #
#                                                                                                         #
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

# import standard python libraries:
from datetime import datetime, date
import logging
import sys
import os
import tempfile

# import other python libraries:
import configparser
import nltk
import newspaper
# import this project's python libraries:

##########

# setup logging
from scraper_utils import removeStartTrailQuotes

logger = logging.getLogger(__name__)


class ConfigManager:
    """ The configuration manager class performs all the configuration processing for the application
    """

    config_parser = None
    rundate = datetime.now()
    rundate_str = datetime.now().strftime('%Y-%m-%d')
    # default install prefix is parent of 'src' folder:
    install_prefix = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    config_file = os.path.join('conf', 'newslookout.conf')
    app_version = 0
    logfile = os.path.join('logs', 'newslookout.log')
    logLevelStr = 'INFO'
    max_logfile_size = 1024 * 1024
    pid_file = os.path.join('logs', 'newslookout.pid')
    newspaper_config = None
    data_dir = 'data'
    plugins_dir = os.path.join(install_prefix, 'plugins')
    plugins_contributed_dir = os.path.join(install_prefix, 'plugins_contrib')
    master_data_dir = os.path.join(data_dir, 'master_data')
    completed_urls_datafile = os.path.join('data', 'completed_urls.db')
    verify_ca_cert = True
    fetch_timeout = 60
    connect_timeout = 3
    retry_wait_rand_max_sec = 10
    retry_count = 3
    retry_wait_sec = 10
    retry_wait_rand_min_sec = 10
    proxy_url_http = ''
    proxy_url_https = ''
    recursion_level = 1
    user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML like Gecko) Version/7.0.3"
    proxy_user = None
    proxy_password = None
    proxies = {"http": None, "https": None}
    logfile_backup_count = 30
    save_html = 'true'
    enabledPluginNames = []

    def __init__(self, configFileName, rundate):
        """ Read and apply the configuration data passed by the main application
        """
        self.config_file = configFileName
        self.config_parser = configparser.ConfigParser()
        try:
            self.config_parser.read_file(open(configFileName, encoding='utf-8'))
            self.rundate = rundate
        except Exception as e:
            print(f'ERROR: Configuration file "{configFileName}" could not be read: {e}')
            sys.exit(1)
        # apply the configurations:
        self.readEnvironCfg()
        self.readPluginNames()
        self.readOperationsCfg()
        self.applyNetworkConfig()

    def checkAndSanitizeConfigString(self, sectionName, configParamName, default=None):
        """ Check and sanitize config string value """
        configParamValue = default
        try:
            paramStr = self.config_parser.get(sectionName, configParamName).strip()
            configParamValue = paramStr
        except Exception as e:
            print(f"Error reading parameter {configParamName} from configuration file, exception was: {e}")
            if default is None:
                print(f"Error reading parameter {configParamName} from configuration file: default value missing.")
        return(configParamValue)

    def checkAndSanitizeConfigInt(self, sectionName, configParamName, default=None, maxValue=None, minValue=None):
        """ Check and sanitize config integer value """
        configParamValue = default
        try:
            paramVal = self.config_parser.getint(sectionName, configParamName)
            if maxValue is not None:
                paramVal = min(paramVal, maxValue)
            if minValue is not None:
                paramVal = max(paramVal, minValue)
            configParamValue = paramVal
        except Exception as e:
            print("Error reading numeric parameter '",
                  configParamName,
                  "' from configuration file, exception was:",
                  configParamName,
                  e)
            if default is None:
                print(f"Error reading parameter {configParamName} from configuration file: default value missing.")
        return(configParamValue)

    @staticmethod
    def checkAndParseDate(dateStr):
        """ Check and Parse Date String, set it to today's date if its in future
        """
        runDate = datetime.now()
        logger.debug("Checking date string: %s", dateStr)
        try:
            if type(dateStr).__name__ == 'datetime':
                runDate = dateStr
            elif type(dateStr).__name__ == 'str':
                runDate = datetime.strptime(dateStr, '%Y-%m-%d')
        except Exception as e:
            logger.error("Invalid date for retrieval (%s): %s; using todays date instead.",
                         dateStr, e)
        # get the current local date
        today = date.today()
        if runDate.date() > today:
            logger.error("Date for retrieval (%s) cannot be after today's date; using todays date instead.",
                         runDate.date())
            runDate = datetime.now()
        return(runDate)

    def readPluginNames(self):
        """ Read the list of plugins enabled in the configuration file
        """
        self.enabledPluginNames = dict()
        try:
            if 'plugins' in self.config_parser.sections():
                section = self.config_parser['plugins']
                if section.name == 'plugins':
                    for key, item in section.items():
                        # TODO : sort the list it so that priority is used
                        if key.startswith('plugin'):
                            name_priority = removeStartTrailQuotes(item.strip()).split('|')
                            pluginName = name_priority[0].strip()
                            priorityVal = 999
                            if name_priority is not None and len(name_priority)>1:
                                try:
                                    priorityVal = int(name_priority[1].strip())
                                except Exception as e:
                                    logger.error('When reading plugin priority as an integer: %s', e)
                            logger.debug('Adding %s with priority %s to the list of enabled plugins.',
                                         pluginName, priorityVal)
                            try:
                                self.enabledPluginNames[pluginName] = priorityVal
                            except Exception as e:
                                logger.error('When adding plugin name and priority to Map: %s', e)
        except Exception as e:
            logger.error("Error reading names of enabled plugins: %s", e)

    def readEnvironCfg(self):
        """ Utility function to read the configuration file,
        and parse it into the dictionary structure used by the application.
        """
        try:
            self.logLevelStr = self.checkAndSanitizeConfigString(
                'logging',
                'log_level',
                default='INFO'
            )
            self.logfile_backup_count = self.checkAndSanitizeConfigInt(
                'logging',
                'logfile_backup_count',
                default=30,
                maxValue=100,
                minValue=1
            )
            self.max_logfile_size = self.checkAndSanitizeConfigInt(
                'logging',
                'max_logfile_size',
                default=1024 * 1024,
                maxValue=1024 * 1024 * 10,
                minValue=1024 * 10
            )
            self.install_prefix = self.checkAndSanitizeConfigString(
                'installation',
                'prefix',
                default=os.getcwd())
            self.checkAndSetDataDir()
            self.master_data_dir = self.checkAndSanitizeConfigString(
                'installation',
                'master_data_dir',
                default=os.path.join(self.data_dir, 'master_data')
            )
            try:
                # first check if data directory self.data_dir'] of given date exists, or not
                if os.path.isdir(self.master_data_dir) is False:
                    # data dir does not exist, so try creating it:
                    os.mkdir(self.master_data_dir)
            except Exception as e:
                print(f"Exception caught creating directory to save Master-data ({self.master_data_dir}) : {e}")
            self.plugins_dir = self.checkAndSanitizeConfigString(
                'installation',
                'plugins_dir',
                default=os.path.join(self.install_prefix, 'plugins')
            )
            self.plugins_contributed_dir = self.checkAndSanitizeConfigString(
                'installation',
                'plugins_contributed_dir',
                default=os.path.join(self.install_prefix, 'plugins_contrib')
            )
            self.logfile = self.checkAndSanitizeConfigString(
                'installation',
                'log_file',
                default=os.path.join(self.data_dir, 'newslookout.log')
            )
            self.pid_file = self.checkAndSanitizeConfigString(
                'installation',
                'pid_file',
                default=os.path.join(tempfile.gettempdir(), 'newslookout.pid')
            )
            self.cookie_file = self.checkAndSanitizeConfigString(
                'installation',
                'cookie_file',
                default=os.path.join(self.data_dir, 'cookies.txt')
            )
            self.readAndCheckSessionHistDb()
        except Exception as e:
            print(f"Error reading environment configuration from file ({self.config_file}): {e}")

    def readAndCheckSessionHistDb(self):
        """ Read and Check Session History Database file.
        """
        self.completed_urls_datafile = self.checkAndSanitizeConfigString(
            'installation',
            'completed_urls_datafile',
            default=os.path.join(self.data_dir, 'completed_urls.db')
        )
        journalFile = self.completed_urls_datafile + '-journal'
        if os.path.isfile(journalFile):
            print(f'ERROR: Session history database was not properly closed, remove journal file: {journalFile}')
            sys.exit(3)
        self.progressRefreshInt = self.checkAndSanitizeConfigInt(
            'operation',
            'progressbar_refresh_interval',
            default=10,
            maxValue=300,
            minValue=2
            )

    def readOperationsCfg(self):
        """ Utility function to read the configuration file,
        and parse it into the dictionary structure used by the application.
        """
        try:
            self.proxy_ca_certfile = self.checkAndSanitizeConfigString(
                'operation',
                'proxy_ca_certfile',
                default=None  # os.path.join(self.data_dir, 'proxy_ca.crt')
            )
            if len(self.proxy_ca_certfile) < 2:
                self.proxy_ca_certfile = None
            self.verify_ca_cert = self.checkAndSanitizeConfigString(
                'operation',
                'verify_ca_cert',
                default='True'
            )
            if self.verify_ca_cert.upper() == 'FALSE':
                self.verify_ca_cert = False
            else:
                self.verify_ca_cert = True
            self.save_html = self.checkAndSanitizeConfigString('operation', 'save_html')
            self.user_agent = self.checkAndSanitizeConfigString('operation', 'user_agent')
            self.proxy_url_http = self.checkAndSanitizeConfigString('operation', 'proxy_url_http')
            self.proxy_url_https = self.checkAndSanitizeConfigString('operation', 'proxy_url_https')
            self.proxy_user = self.checkAndSanitizeConfigString('operation', 'proxy_user')
            self.proxy_password = self.checkAndSanitizeConfigString('operation', 'proxy_password')
            self.recursion_level = self.checkAndSanitizeConfigInt(
                'operation',
                'recursion_level',
                default=1,
                maxValue=4,
                minValue=1
            )
            self.retry_count = self.checkAndSanitizeConfigInt(
                'operation',
                'retry_count',
                default=3,
                maxValue=10,
                minValue=1
            )
            self.retry_wait_sec = self.checkAndSanitizeConfigInt(
                'operation',
                'retry_wait_sec',
                default=5,
                maxValue=600,
                minValue=1
            )
            self.retry_wait_rand_min_sec = self.checkAndSanitizeConfigInt(
                'operation',
                'retry_wait_rand_min_sec',
                default=3,
                maxValue=600,
                minValue=0
            )
            self.retry_wait_rand_max_sec = self.checkAndSanitizeConfigInt(
                'operation',
                'retry_wait_rand_max_sec',
                default=10,
                maxValue=600,
                minValue=1
            )
            self.fetch_timeout = self.checkAndSanitizeConfigInt(
                'operation',
                'fetch_timeout',
                default=3,
                maxValue=600,
                minValue=3
            )
            self.connect_timeout = self.checkAndSanitizeConfigInt(
                'operation',
                'connect_timeout',
                default=3,
                maxValue=600,
                minValue=3
            )
            self.rundate = ConfigManager.checkAndParseDate(self.rundate)
        except Exception as e:
            print(f"Error reading operational configuration from file ({self.config_file}): {e}")

    def applyNetworkConfig(self):
        """ Apply configuration for networking
        """
        os.environ['HTTP_PROXY'] = ''
        os.environ['HTTPS_PROXY'] = ''
        try:
            newspaper_config = newspaper.Config()
            newspaper_config.memoize_articles = False
            newspaper_config.http_success_only = True
            newspaper_config.fetch_images = False
            newspaper_config.number_threads = 2
            newspaper_config.browser_user_agent = self.user_agent
            newspaper_config.request_timeout = self.fetch_timeout
            newspaper_config.use_cached_categories = False
            # add this to config data
            self.newspaper_config = newspaper_config
            # set OS environment variables for proxy server:
            if len(self.proxy_url_http) > 3 and len(self.proxy_url_https) > 3:
                os.environ['HTTP_PROXY'] = self.proxy_url_http
                os.environ['HTTPS_PROXY'] = self.proxy_url_https
                self.proxies = {"http": self.proxy_url_http,
                                              "https": self.proxy_url_https}
            else:
                os.environ['HTTP_PROXY'] = ''
                os.environ['HTTPS_PROXY'] = ''
                self.proxy_url_http = None
                self.proxy_url_https = None
                self.proxies = {}

            nltk.set_proxy(self.proxies)
            self.newspaper_config.proxies = self.proxies
            # print("INFO: For NLTK, using Proxy configuration: ", nltk.getproxies())
        except Exception as e:
            print("ERROR: Unable to set proxy parameters: %s", e)

    def checkAndSetDataDir(self):
        """ Check and set the data directory form configuraiton file """
        self.data_dir = self.checkAndSanitizeConfigString(
            'installation',
            'data_dir',
            default=None
        )
        try:
            # first check if data directory self.data_dir'] of given date exists, or not
            if self.data_dir is not None and os.path.isdir(self.data_dir) is False:
                # data dir does not exist, so try creating it:
                os.mkdir(self.data_dir)
            elif self.data_dir is None:
                print(f'Error identifying data directory: {self.data_dir}')
                sys.exit(1)
        except Exception as e:
            print(f"Exception caught creating directory to save data ({self.data_dir}) : {e}")


# # end of file ##
