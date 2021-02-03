#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: baseModule.py
 Application: The NewsLookout Web Scraping Application
 Date: 2020-01-11
 Purpose: base class that is the parent for all plugins for the application
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
from datetime import date, datetime
import time
import random
import re
from ast import parse

import logging
logger = logging.getLogger(__name__)


# import web retrieval and text processing python libraries:
from bs4 import BeautifulSoup
import newspaper
from newspaper import Article, Source
import requests
import urllib3
from urllib3.exceptions import InsecureRequestWarning
import nltk
import lxml
import cchardet


# import this project's python libraries:
from scraper_utils import normalizeURL, NewsArticle, cutStrBetweenTags, cutStrFromTag, calculateCRC32
from scraper_utils import retainValidArticles, removeInValidArticles, deDupeList, getCookiePolicy, loadAndSetCookies
from scraper_utils import Types


####################################


class baseModule:
    """This is the base/parent class for all plugins."""
    
    historicURLs = 0
    userAgentStr = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A'
    fetch_timeout = 60
    retryCount = 2
    retryWaitFixed = 37
    retry_wait_rand_max_sec = 17
    retry_wait_rand_min_sec=1
    
    newspaper_config = None
    customHeader = dict()
    configData = dict()
    configReader = None
    baseDirName = ""
    cookieJar = None
    URLToFetch = ""

    
    tempArticleData = None
    newsPaperArticle = None
    
    #########################


        
    def readConfigObj(self, configElement ):
        return( self.configReader.get('plugins', configElement ) )
        
        
        
    def config(self, configDict):
        """ Configure the plugin """
        
        self.configData = configDict
        
        try:
            logger.debug("Reading the configuration parameters")
            
            self.fetch_timeout = self.configData['fetch_timeout']
            self.userAgentStr = self.configData['user_agent']
            
            self.baseDirName = self.configData['data_dir']
            
            if self.configData['save_html'].lower() =="true":
                self.bSaveHTMLFile = True
            else:
                self.bSaveHTMLFile = False
            
            self.retryCount = self.configData['retry_count']
            self.retryWaitFixed = self.configData['retry_wait_sec']
            self.retry_wait_rand_max_sec = int(self.configData['retry_wait_rand_max_sec'])
            self.retry_wait_rand_min_sec = int(self.configData['retry_wait_rand_min_sec'])
            
            self.proxies = self.configData[ 'proxies' ]
            
            self.newspaper_config = self.configData['newspaper_config']
            
            self.configReader = self.configData['configReader']
            
        except Exception as e:
            logger.error("Could not read configuration parameters: %s", e)


        try:
            logger.debug("Applying the configuration parameters")

            for urlRegex in self.urlUniqueRegexps:
                # logger.debug("Compiling match pattern for URL identification: %s", urlRegex)
                self.urlMatchPatterns.append( re.compile( urlRegex ) )
            
            
            for authorRegex in self.authorRegexps:
                # logger.debug("Compiling match pattern for Authors: %s", authorRegex)
                self.authorMatchPatterns.append( re.compile( authorRegex ) )
            
            
            for dateRegex in self.articleDateRegexps.keys():
                # logger.debug("Compiling match pattern for dates: %s", dateRegex )
                self.dateMatchPatterns[dateRegex] = ( re.compile( dateRegex ), self.articleDateRegexps[dateRegex] )
            
            # Suppress only the single warning from urllib3 for not verifying SSL certificates
            requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
            
            cookiePolicy =  getCookiePolicy( self.allowedDomains )
            
            self.cookieJar = requests.cookies.RequestsCookieJar(policy=cookiePolicy)
            
            # opener = urllib3.request.build_opener( urllib.request.HTTPCookieProcessor(self.cookieJar) )
            # r = opener.open("http://example.com/")
            
            self.customHeader = {'user-agent': self.userAgentStr }

        except Exception as e:
            logger.error("Could not apply configuration parameters: %s", e)
    


         
    def makeUniqueFileName(self, uniqueID ):
        return(  type(self).__name__ + "_" + str(uniqueID) )




    def sleepBeforeNextFetch(self):
        pauseTime = self.retryWaitFixed + random.randint(
            self.retry_wait_rand_min_sec
            , self.retry_wait_rand_max_sec)
        logger.debug("Pausing web retrieval for %s seconds.", pauseTime)
        time.sleep( pauseTime )



    def fetchDataFromURL(self, uRLtoFetch, WorkerID ):
        """ fetch Data From URL """
        
        logger.debug("Fetching %s, Worker ID %s", uRLtoFetch.encode("ascii") , WorkerID)
        
        self.URLToFetch = uRLtoFetch
        
        articleUniqueID = self.getUniqueIDFromURL(uRLtoFetch)
        
        rawData = self.fetchRawDataFromURL(uRLtoFetch, WorkerID)
        
        if type( rawData).__name__ =='Article':
            htmlContent = rawData.html
            
        elif type( rawData).__name__ =='str':
            htmlContent = rawData
        
        validData = self.parseFetchedData(uRLtoFetch, rawData, WorkerID)
        
        self.tempArticleData = validData
        
        
        if validData.getTextSize() > self.minArticleLengthInChars:
            
            validData.setArticleID( articleUniqueID )
            
            # write news article object 'validData' to file:
            filename = self.makeUniqueFileName( articleUniqueID )
            
            validData.writeFiles( filename
                                  , self.baseDirName
                                  , htmlContent
                                  , saveHTMLFile=self.bSaveHTMLFile )
            
            # save count of characters of downloaded data for the given URL
            self.uRLdata[uRLtoFetch] = len(htmlContent)
            
        else:
            logger.error("Insufficient or invalid data (%s characters) retrieved for URL: %s"
                         , validData.getTextSize()
                         , uRLtoFetch.encode('ascii') )

        self.URLToFetch = ""
        
        
        

    def fetchRawDataFromURL(self, uRLtoFetch, WorkerID ):
        """ fetching content From URL """
        
        logger.debug("Downloading Raw Data for URL %s, Worker ID %s", uRLtoFetch.encode(), WorkerID)
        
        try:
            # create newspaper library's article object:
            newsPaperArticle = Article(uRLtoFetch, config=self.newspaper_config)
            
            #  retrieve data from the URL
            newsPaperArticle.download()
            
        except Exception as e:
            logger.error("Exception downloading raw data From URL %s: %s", uRLtoFetch, e )
            newsPaperArticle.set_html("")
        
        return(newsPaperArticle)

    
    
    def extractArticlesListWithNewsP(self ):
        """ extract Article Text using the Newspaper library """
        
        thisNewsPaper = newspaper.build( self.mainURL
                                         , config=self.newspaper_config )
        
        newspaperSourcedURLS = retainValidArticles( thisNewsPaper.articles
                                     , self.validURLStringsToCheck)
            
        # normalize the list of URLs:
        for uRLIndex in range( len(newspaperSourcedURLS) ):
            self.listOfURLS.append( normalizeURL( newspaperSourcedURLS[uRLIndex] ) )
    



    def getURLsListForDate(self, runDate):
        """ Retrieve the URLs List For a given Date"""
        
        logger.info("%s: Fetching list of urls for date: %s", type(self).__name__ , str( runDate.strftime("%Y-%m-%d") ) )
        
        try:

            self.extractArticlesListWithBeauSoup()
            
            self.extractArticlesListWithNewsP()
            
            # remove invalid articles:
            self.listOfURLS = removeInValidArticles( self.listOfURLS, self.invalidURLSubStrings )
            
            # de-dupe articles list:
            listLengthBeforeDedupe = len(self.listOfURLS)
            self.listOfURLS = deDupeList(self.listOfURLS)
            
            if listLengthBeforeDedupe > len(self.listOfURLS):
                logger.info( "%s: Total count of valid articles to be retrieved = %s (after removing %s duplicates)"
                             , type(self).__name__ 
                             , len(self.listOfURLS)
                             , listLengthBeforeDedupe - len(self.listOfURLS) )
            else:
                logger.info( "%s: Total count of valid articles to be retrieved = %s", type(self).__name__ , len(self.listOfURLS) )
            
        except Exception as e:
            logger.error("%s: Error trying to retrieve listing of URLs: %s", type(self).__name__ , e)
    
    
 
    def parseFetchedData(self, url_index):
        """ parse Fetched Data """
        logger.error("fetchDataFromURL() of base class was called.")
        raise Exception("Base function parseFetchedData() must be overridden.")
        return -1


    def processData(self, runDate ):
        """ process data """
        logger.error("processData() of base class was called.")
        raise Exception("Base function processData() must be overridden.")
        return -1        


## end of file ##