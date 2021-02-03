#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: scraper_app.py
 Application: The NewsLookout Web Scraping Application
 Date: 2020-01-11
 Purpose: Main class for the web scraping and news text processing application
"""

__version__ = "1.6"
__author__ = "Sandeep Singh Sandhu"
__copyright__ = "Copyright 2020, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu"
__credits__ = ["Sandeep Singh Sandhu"]
__license__ = "GPL"
__maintainer__ = "Sandeep Singh Sandhu"
__email__ = "sandeep.sandhu@gmx.com"
__status__ = "Production"

####################################

# Usage:
# python scraper_app.py -c <configfile> -d <rundate>

# Running:
# cd /usr/local/share/scraper_py
# python3 bin/scraper_app.py -c conf/scraper.conf -d 2019-12-31

####################################


# import standard python libraries:
import sys, getopt, os
import time
import logging
import importlib
import importlib.resources
from configparser import ConfigParser 


# import web retrieval and text processing python libraries:
import bs4
import newspaper
import nltk
import spacy

# import project's python libraries:
from scraper_utils import checkAndParseDate, checkAndGetNLTKData, loadAndSetCookies
from queueManager import queueManager



####################################


class NewsLookout:
   """ NewsLookout Web Scraping Application """
   
   configData = { 'version' : 1.6, 'logLevelStr': 'INFO' }
   appQueue = None
   configFileName = 'scraper.conf'
   logfile = 'scraper.log'
   pidFileName = "scraper.pid"
   newspaper_config = None
   
   
   def __init__(self):
      """Initialize the application class
       by reading the program arguments, validating them
       and setting the configuration data accordingly """
      
      print("NewsLookout Web Scraping Application" )
      self.readArgs()
      self.appQueue = queueManager()
      self.readConfig()
      
      if os.path.isfile(self.pidFileName):
          print("ERROR: Unable to start the application since PID file exists: ", self.pidFileName)
          sys.exit(1)
      else:
          # create empty file
          fp = None
          try:
              pidValue = os.getpid()
              fp = open(self.pidFileName, 'wt', encoding='utf-8')
              n = fp.write( str(pidValue) )
          except Exception as e:
              print("Error creating PID file: ", e, self.pidFileName)
              sys.exit(1)
          finally:
              fp.close()



   def readArgs(self):
       
      try:
         opts, args = getopt.getopt( sys.argv[1:]
                                      , "h:c:d:"
                                      ,["configfile=", "rundate="]
                                      )
         
      except getopt.GetoptError as e:
         print( "Error reading command line options: %s", e)
         print( 'Usage: scraper_app.py -c <configuration file> -d <run date>' )
         sys.exit(2)
      
      for opt, arg in opts:
          
         if opt in ("-h", "--help"):
            print( 'Usage: scraper_app.py -c <configuration file> -d <run date>' )
            sys.exit(0)
            
         elif opt in ("-c", "--configfile"):
            self.configData['configfile'] = arg
            
         elif opt in ("-d", "--rundate"):
            self.configData['rundate'] = arg
      
      try:
          # check if config file exists, exit if it doesnt exist
          if os.path.isfile( self.configData['configfile'] ):
             print( 'Reading configuration settings from file:', self.configData['configfile'] )
          else:
             print( 'ERROR: Configuration file (', self.configData['configfile'], ') not found' )
             sys.exit(1)

      except Exception as e:
          print('ERROR: Configuration file (', self.configData['configfile'], ') not found:', e )
          sys.exit(1)




   def readConfig(self):
      """Utility function to read the configuration file,
      and parse it into the dictionary structure used by the application."""

      configur = ConfigParser()
      try:          
          configur.read_file( open( self.configData['configfile'], encoding='utf-8' ) )

      except Exception as e:
          print('ERROR: Configuration file (', self.configData['configfile'], ') could not be read:', e )
          sys.exit(1)
    
      try:
          self.configData['configReader'] = configur
          
          self.configData['logfile'] = configur.get('installation','log_file')
          
          self.configData['pid_file'] = configur.get('installation','pid_file')
          self.pidFileName = configur.get('installation','pid_file')
          
          self.configData['logLevelStr'] = configur.get('logging','log_level')
              
          self.configData['data_dir'] = configur.get('installation','data_dir')
          
          self.configData['cookie_file'] = configur.get('installation','cookie_file')
          
          self.configData['enabledPlugins'] = configur.get('plugins','enabled')
        
          self.configData['plugins_dir'] = configur.get('installation','plugins_dir')
        
          self.configData['install_prefix'] = configur.get('installation','prefix')
          
          self.configData['completed_urls_datafile'] = configur.get('installation','completed_urls_datafile')
          
          self.configData['retry_count'] = configur.getint('operation', 'retry_count')
          
          self.configData['save_html'] = configur.get('operation', 'save_html')
          
          self.configData['retry_wait_sec'] = configur.getint('operation', 'retry_wait_sec')
          
          self.configData['retry_wait_rand_min_sec'] = configur.getint('operation', 'retry_wait_rand_min_sec')
          self.configData['retry_wait_rand_max_sec'] = configur.getint('operation', 'retry_wait_rand_max_sec')
          
          self.configData['fetch_timeout'] = configur.getint('operation', 'fetch_timeout')
          
          self.configData['user_agent'] = configur.get('operation', 'user_agent')
          
          self.configData['proxy_url_http'] = configur.get('operation', 'proxy_url_http')
          self.configData['proxy_url_https'] = configur.get('operation', 'proxy_url_https')
          
          self.configData['proxy_user'] = configur.get('operation', 'proxy_user')
          self.configData['proxy_password'] = configur.get('operation', 'proxy_password')
          
          self.configData['proxies'] = dict()
          
          self.configData['worker_threads'] = configur.getint('operation', 'worker_threads')
          
          self.configData['rundate'] = checkAndParseDate( self.configData['rundate'] )

          
      except Exception as e:
          print("ERROR Reading configuration file - ", self.configData['configfile'], "; exception was: ", e )

      os.environ['HTTP_PROXY'] = ''
      os.environ['HTTPS_PROXY'] = ''
      self.configData[ 'proxies' ] = { "http": None, "https": None }
        
      try:
          self.newspaper_config = newspaper.Config()
          self.newspaper_config.memoize_articles = True
          self.newspaper_config.http_success_only = True
          self.newspaper_config.fetch_images = False
          self.newspaper_config.number_threads = 2
          self.newspaper_config.browser_user_agent = self.configData['user_agent']
          self.newspaper_config.request_timeout = self.configData['fetch_timeout']
          
          # add this to config data
          self.configData['newspaper_config'] = self.newspaper_config
                        
          # set OS environment variables for proxy server: 
          if len( self.configData['proxy_url_http'] )> 3 and len( self.configData['proxy_url_https'] )> 3:
              
              os.environ['HTTP_PROXY']  = self.configData['proxy_url_http']
              
              os.environ['HTTPS_PROXY'] = self.configData['proxy_url_https']
              
              self.configData[ 'proxies' ] = { "http": self.configData['proxy_url_http']
                                             , "https": self.configData['proxy_url_https'] }
              
          else:
              print("INFO: Not using any proxy servers: "
                    , self.configData['proxy_url_http']
                    , " or "
                    , self.configData['proxy_url_https'] )

          nltk.set_proxy( self.configData[ 'proxies' ] )
          
          self.configData['newspaper_config'].proxies = self.configData['proxies']
          
          self.configData['cookieJar'] = loadAndSetCookies( self.configData['cookie_file'] )
                    
          print("INFO: For NLTK, using Proxy configuration: ", nltk.getproxies() )


      except Exception as e:
          print("ERROR: Unable to set proxy parameters: %s", e )




   def run(self, currVersion):
          """Run the application job after configuring the main queue """
          
          self.configData['version'] = currVersion
          
          logging.info('--- Application (version %s) has started retrieving data for date: %s ---'
                       , self.configData['version'], self.configData['rundate'] )
          
          checkAndGetNLTKData()
          
          self.appQueue.config( self.configData )
        
          self.appQueue.runWebSourceIdentificationJobs()
          
          self.appQueue.runWebRetrievalJobs()
          
          self.appQueue.runDataProcessingJobs()
        
   ## end of application class definition ##



# the main application class instance is a global variable
global app_inst



if __name__ == "__main__" :
    
    global app_inst
    
    # instantiate the main application class
    app_inst = NewsLookout()
    
    # setup logger
    logLevel = logging.DEBUG
    
    if app_inst.configData['logLevelStr']=='INFO':
        logLevel = logging.INFO
    elif app_inst.configData['logLevelStr']=='WARN':
        logLevel = logging.WARNING
    elif app_inst.configData['logLevelStr']=='ERROR':
        logLevel = logging.ERROR
    else:
        logLevel = logging.DEBUG
    
    logging.basicConfig( filename=app_inst.configData['logfile']
                        , level=logLevel
                        , format='%(asctime)s:%(levelname)s:%(name)s:%(thread)s: %(message)s' )
    
    print( "Logging to file:"
           , app_inst.configData['logfile']
           , "with log level"
           , logLevel)
    
    # run the application:
    app_inst.run( __version__ )
    
    # close down everything:
    if os.path.isfile(app_inst.pidFileName):
        try:
            # delete: app_inst.pidFileName
            os.remove(app_inst.pidFileName) 
        except Exception as e:
            logging.error("Error removing PID file %s: %s", app_inst.pidFileName, e )
            sys.exit(1)
    else:
        logging.info("PID file %s does not exist, so unable to delete it.", app_inst.pidFileName )



def printAppVersion():
    listOfGlobals = globals()    
    print( "Application version ="
           , listOfGlobals['app_inst'].appQueue.configData['version'] )


## end of file ##