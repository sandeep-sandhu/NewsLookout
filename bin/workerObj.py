#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: workerObj.py
 Application: The NewsLookout Web Scraping Application
 Date: 2020-01-11
 Purpose: This object encapsulates the worker thread that
  runs all multi-threading functionality to run the
  web scraper plugins loaded by the application.
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

# import standard python libraries:
import logging
logger = logging.getLogger(__name__)
import threading
import time
import queue
from datetime import datetime

# import this project's python libraries:
from scraper_utils import Types

####################################


class workerObj(threading.Thread):
    """ Worker object to process fetching and data extraction in each thread. """
    
    workerID = -1
    plugins = dict()

    workToDoQueue = None
    taskType = Types.TASK_GET_URL_LIST
    runDate = datetime.now()
    
    
    def __init__(self, pluginList, daemon=None, target=None, name=None):
        """ Initialize the worker object """
        
        self.workerID = name
        
        self.plugins = pluginList
        
        logger.debug("%s: Got initialized with plugins: %s", self.workerID, self.plugins)
        
        self.workToDoQueue = queue.PriorityQueue()
        
        super().__init__(daemon = daemon, target= target, name=name )



    def setTaskType(self, taskType):
        self.taskType = taskType
        
        
        
    def setRunDate(self, runDate):
        self.runDate = runDate



    def run(self):
        """ Overridden to enable thread to be called for executing the Plugin jobs """
        
        pluginName = None
        queueItem = None
        sURL = None
        
        logger.debug( "Size of work queue = %s, task = %s", self.workToDoQueue.qsize(), self.taskType )
        
        while( self.workToDoQueue.empty()==False ):

            try:
                
                if self.taskType==Types.TASK_GET_DATA:
                    
                    (priority_number, (pluginName, sURL) ) = self.workToDoQueue.get(block=True)
                    
                    logger.debug('Thread %s given task to retrieve URL: %s, for web scraper plugin %s'
                                 , self.workerID, sURL.encode('ascii') , pluginName)
                
                    thisTaskModule = self.plugins[pluginName]
                    
                    if thisTaskModule.pluginType in [ Types.MODULE_NEWS_CONTENT, Types.MODULE_NEWS_API, Types.MODULE_DATA_CONTENT ]:
                        
                        thisTaskModule.fetchDataFromURL( sURL, self.workerID )
                        
                        thisTaskModule.sleepBeforeNextFetch( )
                        
                        logger.debug('Thread %s finished retrieval of URL: %s, for web scraper plugin %s'
                                     , self.workerID, sURL.encode('ascii') , type(thisTaskModule).__name__ )
                        
                        
                elif self.taskType==Types.TASK_GET_URL_LIST:
                    (priority_number, (pluginName) ) = self.workToDoQueue.get(block=True)
                    
                    thisTaskModule = self.plugins[pluginName]
                    
                    logger.debug('Thread %s given task to get URL listing for web scraper plugin %s'
                                 , self.workerID, pluginName)

                    if thisTaskModule.pluginType in [ Types.MODULE_NEWS_CONTENT, Types.MODULE_NEWS_API, Types.MODULE_DATA_CONTENT, Types.MODULE_NEWS_AGGREGATOR ]:
                        
                        # fetch URL list using each plugin's function:
                        thisTaskModule.getURLsListForDate( self.runDate )
                        
                        logger.debug('Thread %s finished getting URL listing for web scraper plugin %s'
                                     , self.workerID, pluginName)
                    
                    
                elif self.taskType==Types.TASK_PROCESS_DATA:
                    
                    (priority_number, (pluginName) ) = self.workToDoQueue.get(block=True)
                    
                    thisTaskModule = self.plugins[pluginName]
                    
                    logger.debug('Thread %s given task to process data for processing plugin %s'
                                 , self.workerID, pluginName)
                    
                    if (thisTaskModule.pluginType == Types.MODULE_DATA_PROCESSOR ):
                        
                        # get data processing initiated for each plugin's function:
                        thisTaskModule.processData( self.runDate )
                        
                        logger.debug('Thread %s finished getting URL listing for web scraper plugin %s'
                                     , self.workerID, pluginName)
                    
            except Exception as e:
                logger.error( "When trying to run task for plugin: %s, Task type: %s, Exception: %s", pluginName, self.taskType, e)
            
        logger.debug("Thread %s finished all type %s tasks assigned.", self.workerID, self.taskType )


## end of file ##