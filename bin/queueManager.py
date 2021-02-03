#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: queueManager.py
 Application: The NewsLookout Web Scraping Application
 Date: 2020-01-11
 Purpose: manage queues of the scraper plugins for the web scraper
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
from datetime import datetime
import sys, os
import threading
import multiprocessing
import queue
import json
from json import JSONEncoder
import traceback
import itertools

# import web retrieval and text processing python libraries:
import nltk
import newspaper

# import this project's python libraries:
from workerObj import workerObj
from scraper_utils import completedURLs, loadPlugins, checkAndGetNLTKData
from scraper_utils import retainValidArticles, removeInValidArticles, deDupeList, getCookiePolicy
from scraper_utils import Types
import scraper_utils

####################################


class queueManager:
	""" The Queue manager class which runs the main process of the application """
	
	# default values to fallback on, in case config file is not available:
	jsonFileName = "completed_urls.json"	
	retryCount = 1
	
	configData = dict()
	mods = dict()
	workers = dict()	
	URL_frontier = dict()


	def __init__(self):
		self.workCompletedURLs = completedURLs()
		self.previouslyCompletedURLs = completedURLs()
		self.runDate = datetime.now()
		self.workerThreads = 2
		self.available_cores = 1



	def initPlugins(self):
		""" Load, configure and initialize all plugins """
		
		# load the plugins		
		self.mods = loadPlugins(self.configData)

		for keyitem in self.mods.keys():
			logger.info("Starting web scraping plugin: %s", keyitem)
			self.mods[keyitem].config( self.configData)



	def initWorkers(self):
		""" Initialize all worker threads """
		logger.debug("Initializing the worker threads.")
		
		for workerNumber in range(self.workerThreads):
			logger.debug("Initializing worker ID %s with plugins: %s", workerNumber, self.mods)
			self.workers[ workerNumber ] = workerObj(self.mods, name=workerNumber, daemon=False)
			# after this, the self.workers dict has the structure: workers[1] = <instantiated worker object>
		
		logger.info("%s worker threads available", len(self.workers) )
		if len(self.workers)!= self.workerThreads:
			logger.error("Could not initialize required no of worker threads.")



	def checkAndSetWorkerCounts(self):
		""" check whether the core count does not exceed 3 times local machine available cores """
		
		if self.workerThreads > self.available_cores * 3:
			logger.error("Core count configured %s is higher than available number of CPU cores %s of the local machine"
						, self.workerThreads, self.available_cores)
			raise Exception("Core count configured is higher than available number of CPU cores of the local machine")
		
		elif self.workerThreads==1 and self.available_cores > 1:
			self.workerThreads = 2



	def config(self, configData):
		""" Read and apply the configuration data passed by the main application """
		
		self.configData = configData
		
		try:
			logger.debug("Configuring the queue manager")
			
			self.retryCount = self.configData['retry_count']
			self.proxies = self.configData['proxies']
			
			self.workerThreads = configData['worker_threads']
			self.available_cores = multiprocessing.cpu_count()
			
			self.runDate = configData['rundate']
			
			self.jsonFileName = self.configData['completed_urls_datafile']
			
			self.fetch_timeout = self.configData['fetch_timeout']
			self.userAgentStr = self.configData['user_agent']
			
			
		except Exception as e:
			logger.error("Exception when configuring the queue manager: %s", e)

		try:
			# Apply the configuration
			self.checkAndSetWorkerCounts()


		except Exception as e:
			logger.error("Exception applying the configuration: %s", e)

		self.initPlugins()



	def readPreviouslyCompeltedQueuefromJSON(self):
		""" Read Previously Completed URLs saved in JSON file """
		
		 # instantiate the class:
		self.previouslyCompletedURLs =  completedURLs()
		
		# read data from the filename
		self.previouslyCompletedURLs.readFromJSON(self.jsonFileName)
		


	def addRetrievedURLasCompleted(self, pluginName, sURL):
		""" Add Retrieved URL as Completed """
		
		#logger.debug("Adding URL = %s as completed by web scraping plugin = %s", pluginName, sURL)
		self.workCompletedURLs.addURL( sURL )



	def dedupeURLs(self):
		""" check with history and remove duplicates """
		
		removalcounter = 0
		totalCounter = 0
		
		for keyitem in self.URL_frontier.keys(): 			
			# loop through all urls of the plugin
			
			for sURL in self.URL_frontier[keyitem]:				
				#logger.debug("De-duplicating url %s of plugin %s", sURL, keyitem)
				totalCounter = totalCounter + 1
				if self.previouslyCompletedURLs.checkURLExists(sURL):					
					# if so, remove this one since it is a duplicate:
					# logger.debug("Removing already retrieved URL: %s", sURL)					
					self.URL_frontier[keyitem].remove(sURL)
					removalcounter = removalcounter + 1

		# at this point, the result should be a de-duplicated self.URL_frontier object
		logger.info("Removed %s duplicate URLs, Final count of URLs to retrieve = %s"
				, removalcounter, totalCounter )



	def retrieveURLListing(self):
		""" Retrieve the Listing of URLs """
		
		# initialize the queue with pre-fetch jobs, i.e. fetch headings and urls for a given date
		
		# fetch and then read URL list data from each plugin:
		for pluginName in self.mods.keys():
			
			# logger.debug("Retrieving the list of URLs for plugin: %s", pluginName)
			thisPlugin = self.mods[pluginName]
			
			# fetch URL list using each plugin's function:
			thisPlugin.getURLsListForDate( thisPlugin, self.runDate )
			
			# add the URLs retrieved by this plugin to the common URL frontier:
			if thisPlugin.pluginType in [Types.MODULE_NEWS_CONTENT, Types.MODULE_DATA_CONTENT, Types.MODULE_NEWS_API]:
				
				self.URL_frontier[pluginName] = thisPlugin.listOfURLS
				
			elif thisPlugin.pluginType == Types.MODULE_NEWS_AGGREGATOR:
				
				logger.info("Now allocating URLs from news aggregator %s to individual plugins.", pluginName)

				# for each url, check domains of each mod and assign if matches:
				for URLFromAggregator in thisPlugin.listOfURLS:
					logger.debug("Now assigning URL %s to plugin.", URLFromAggregator)
			
			# empty the URL listing inside each plugin class
			thisPlugin.listOfURLS = []



	def assignScrapingModulesToQueue(self):
		""" Assign URL to workers by adding URL name as the key in queue workToDoQueue
		, of each worker id to which is is assigned """
		
		workerIndex = 0
		workerKeys = list( self.workers.keys() )
		numTotalWorkers = len(workerKeys)
		
		priority_number = 0
		
		for keyitem in self.mods.keys():
			
			thisTaskPlugin = self.mods[keyitem]
			
			if thisTaskPlugin.pluginType in [ Types.MODULE_NEWS_CONTENT, Types.MODULE_NEWS_API, Types.MODULE_DATA_CONTENT, Types.MODULE_NEWS_AGGREGATOR ]:
				
				# intentionally assign duplicate priority numbers intentionally to URLs of all plugins
				priority_number = priority_number + 1
				
				# logger.debug("Allocating URL '%s' of plugin '%s' into the queue of worker %s with priority %s", sURL, keyitem, workerKeys[workerIndex], priority_number )
				
				self.workers[ workerKeys[workerIndex] ].workToDoQueue.put(
					 (priority_number, keyitem )
					  )
				# at this point, the queue of the worker has items of structure: "plugin name"
				self.workers[ workerKeys[workerIndex] ].setTaskType(Types.TASK_GET_URL_LIST )
				self.workers[ workerKeys[workerIndex] ].setRunDate( self.runDate )
			
				# increment the worker index
				workerIndex = workerIndex + 1
				
				# wrap the worker index back to 0 if it reached the end of the list
				if workerIndex==numTotalWorkers:
					workerIndex = 0



	def processModulestoGetURLList(self):
		""" Retrieve the Listing of URLs """
		
		# initialize the queue with pre-fetch jobs, i.e. fetch headings and urls for a given date

		logger.debug("Process plugins On Worker threads to get URL listing for given date")
		
		# loop through workers, and start their threads:
		for keyitem in self.workers.keys():
			self.workers[keyitem].start()
		
		# wait for all of them to finish
		for keyitem in self.workers.keys():
			self.workers[keyitem].join()
		
		logger.debug('Completed retrieving URL data on all worker threads')
		
		# fetch and then read URL list data that was retrieved from each plugin:
		for pluginName in self.mods.keys():
			
			thisPlugin = self.mods[pluginName]
			
			# add the URLs retrieved by this plugin to the common URL frontier:
			if thisPlugin.pluginType in [Types.MODULE_NEWS_CONTENT, Types.MODULE_NEWS_API, Types.MODULE_DATA_CONTENT]:
				self.URL_frontier[pluginName] = thisPlugin.listOfURLS
				
			elif thisPlugin.pluginType == Types.MODULE_NEWS_AGGREGATOR:
				logger.info("Now allocating URLs from news aggregator %s to individual plugins.", pluginName)
				mixedURLsList = thisPlugin.listOfURLS
				# TODO : implement this fully
			
			# empty the URL listing inside each plugin class
			thisPlugin.listOfURLS = []



	def assignProcessingModulesToQueue(self):
		""" assign data processing plugins To Queue """
		
		logger.debug("Assigning data processing plugins to the task queue")
		workerIndex = 0
		workerKeys = list( self.workers.keys() )
		numTotalWorkers = len(workerKeys)
		
		priority_number = 0

		for keyitem in self.mods.keys():

			if self.mods[keyitem].pluginType == Types.MODULE_DATA_PROCESSOR:
				# intentionally assign duplicate priority numbers intentionally to URLs of all plugins
				priority_number = priority_number + 1
				
				# logger.debug("Allocating URL '%s' of plugin '%s' into the queue of worker %s with priority %s", sURL, keyitem, workerKeys[workerIndex], priority_number )
				
				self.workers[ workerKeys[workerIndex] ].workToDoQueue.put(
					 (priority_number, keyitem )
					  )
				self.workers[ workerKeys[workerIndex] ].setTaskType(Types.TASK_PROCESS_DATA )
				# at this point, the queue of the worker has items of structure: "plugin name"
				
				# increment the worker index
				workerIndex = workerIndex + 1
				
				# wrap the worker index back to 0 if it reached the end of the list
				if workerIndex==numTotalWorkers:
					workerIndex = 0		



	def assignURLsToQueue(self):
		""" Assign URL to workers by adding URL name as the key in queue workToDoQueue
		, of each worker id to which it is assigned """
		
		workerIndex = 0
		workerKeys = list( self.workers.keys() )
		numTotalWorkers = len(workerKeys)
		
		for keyitem in self.mods.keys():
			
			priority_number = 0
			
			thisTaskPlugin = self.mods[keyitem]

			if thisTaskPlugin.pluginType in [Types.MODULE_NEWS_CONTENT, Types.MODULE_NEWS_API, Types.MODULE_DATA_CONTENT]:
				try:
					pluginURLS = self.URL_frontier[keyitem]
					
					# loop through all urls of the plugin
					for sURL in pluginURLS:
						
						priority_number = priority_number + 1 # intentionally assign duplicate priority numbers intentionally to URLs of all plugins
                                                
						self.workers[ workerKeys[workerIndex] ].workToDoQueue.put(
							 (priority_number, (keyitem, sURL) )
							  )
						
						# at this point, the queue of the worker has items of structure: ("plugin name", "https:/site/page")
						self.workers[ workerKeys[workerIndex] ].setTaskType(Types.TASK_GET_DATA )
						
						# increment the worker index
						workerIndex = workerIndex + 1
						
						# wrap the worker index back to 0 if it reached the end of the list
						if workerIndex==numTotalWorkers:
							workerIndex = 0
							
				except Exception as e:
					logger.error("Error assigning modules to workers: %s", e)
					
			else:
				logger.info("Not allocating any URLs for retrieval for plugin: %s", keyitem)




	def processURLsOnWorkers(self):
		""" Assign URL to workers by adding URL name as the key in queue workToDoQueue
		, and worker id to which is is assigned as the dict's value """

		
		# loop through workers, and start their threads:
		for keyitem in self.workers.keys():
			self.workers[keyitem].start()
		
		# wait for all of them to finish
		for keyitem in self.workers.keys():
			self.workers[keyitem].join()
		
		logger.debug('Completed retrieving URL data on worker threads')
		
		for pluginName in self.mods.keys():
			
			logger.debug("Collecting retrieval metrics for plugin: %s", pluginName)
			totalCount = 0
			
			thisPlugin = self.mods[pluginName]
			
			if thisPlugin.pluginType in [ Types.MODULE_NEWS_CONTENT, Types.MODULE_NEWS_API, Types.MODULE_DATA_CONTENT ]:
				
				# get URLs completed from dict stored for each plugin:
				for urlDataKey in self.mods[pluginName].uRLdata.keys():
					
					self.addRetrievedURLasCompleted(pluginName, urlDataKey)
					
					totalCount = totalCount + self.mods[pluginName].uRLdata[urlDataKey]
				
				logger.info("Retrieved %s characters of raw data for plugin: %s", totalCount, pluginName)



	def processDataOnWorkers(self):
		""" Process Data on Workers:
		loop through data plugins and execute these in serial order """
		
		# fetch and  read data that was processed by each plugin:
		for pluginName in self.mods.keys():
			
			thisPlugin = self.mods[pluginName]
			
			if thisPlugin.pluginType == Types.MODULE_DATA_PROCESSOR:
				
				thisPlugin.processData()
				
				logger.debug('Collecting processed data from plugin: %s', pluginName)
			
		

	def saveCompletedURLsToJSON(self):
		""" Save Completed Queue To JSON """
		
		# merge this session's retrieved URL with previous list of URL
		self.workCompletedURLs.URLObj = deDupeList( self.workCompletedURLs.URLObj + self.previouslyCompletedURLs.URLObj )
		
		# then save it to file
		self.workCompletedURLs.writeToJSON( self.jsonFileName )



	def runWebSourceIdentificationJobs(self):
		""" Process Queue to run all web source (URL) identification jobs
		"""
		
		# To begin with, get the listing of url's
		self.initWorkers()
		self.assignScrapingModulesToQueue()
		self.processModulestoGetURLList()
		
		# remove previously retrieved web content:
		self.readPreviouslyCompeltedQueuefromJSON()
		self.dedupeURLs()



	def runWebRetrievalJobs(self):
		""" Process Queue to run all Web Retrieval Jobs
		"""
		
		# start fetching data from each URL in URL frontier
		self.initWorkers()
		self.assignURLsToQueue()		
		self.processURLsOnWorkers()
		
		# save complete list of URLs retrieved
		self.saveCompletedURLsToJSON()

		

	def runDataProcessingJobs(self):
		""" process Queue to run Data Processing Jobs
		"""

		# perform any data processing required on fetched data:
		self.initWorkers()
		self.assignProcessingModulesToQueue()		
		self.processDataOnWorkers()


## end of file ##