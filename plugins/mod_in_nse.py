#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: mod_in_nse.py
 Application: The NewsLookout Web Scraping Application
 Date: 2020-01-11
 Purpose: Plugin for the National Stock Exchange Data, India
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
from datetime import date, datetime, timedelta
import time
import random
import re
from ast import parse
import os, sys

import logging
logger = logging.getLogger(__name__)

# import web retrieval and text processing python libraries:
from bs4 import BeautifulSoup
import newspaper
from newspaper import Article, Source
import requests
from urllib3.exceptions import InsecureRequestWarning
import nltk
import lxml
import cchardet
import zipfile

# import this project's python libraries:
from baseModule import baseModule
from scraper_utils import normalizeURL, NewsArticle, cutStrBetweenTags, cutStrFromTag, calculateCRC32
from scraper_utils import retainValidArticles, removeInValidArticles
from scraper_utils import Types


####################################


class mod_in_nse(baseModule):
    """ Web Scraping plugin: mod_in_nse
    For National Stock Exchange
    Country: India
    Save the BhavCopy file from the exchange at End of day
    """
    
    minArticleLengthInChars = 10000
    pluginType = Types.MODULE_DATA_CONTENT # implies data content fetcher
    
    mainURL_prefix = 'https://www1.nseindia.com/archives/equities/bhavcopy/pr/PR'
    mainURL_suffix = ".zip"
    # https://www1.nseindia.com/archives/equities/bhavcopy/pr/PRDDMMYY.zip

    listOfURLS = []
    urlUniqueRegexps = ["(^https://www1.nseindia.com/archives/equities/bhavcopy/pr/PR)([0-9]+)(.zip$)"]
    urlMatchPatterns = []
    uRLdata = dict()
    authorRegexps = []
    articleDateRegexps = {}
    allowedDomains = ["www1.nseindia.com", "www.nseindia.com"]
    
    
    def __init__(self):
        """ Initialize the object """
        
        super().__init__()




    def getURLsListForDate(self, runDate):
        """ Retrieve the URLs List For a given Date"""
        
        logger.info("Fetching list of urls for date: %s",  str( runDate.strftime("%Y-%m-%d") ) )
        
        self.listOfURLS = []
        
        try:

            urlForDate = self.mainURL_prefix + runDate.strftime("%d%m%y") + self.mainURL_suffix

            self.listOfURLS = [urlForDate ]
            
            logger.info( "Total count of valid articles to be retrieved = %s"
                             , len(self.listOfURLS) )
            
        except Exception as e:
            logger.error("Error trying to retrieve listing of URLs: %s", e)




    def fetchDataFromURL(self, uRLtoFetch, WorkerID ):
        """ Fetch data From given URL """
        
        fullPathName = ""
        dirPathName = ""
        sizeOfDataDownloaded = -1
        
        logger.debug("Fetching %s, Worker ID %s", uRLtoFetch.encode("ascii") , WorkerID)
        
        (publishDate, dataUniqueID) = self.getUniqueIDFromURL(uRLtoFetch)
        
        rawData = self.fetchRawDataFromURL(uRLtoFetch, WorkerID)
        
        # write data to file:
        fileNameWithOutExt = self.makeUniqueFileName( dataUniqueID )
        
        sizeOfDataDownloaded = len(rawData)
        
        if sizeOfDataDownloaded > self.minArticleLengthInChars:
            
            try:
                publishDateStr = str( publishDate.strftime("%Y-%m-%d") )
                
                dirPathName = os.path.join( self.configData['data_dir'] , publishDateStr )
                
                # first check if directory of given date exists or not
                if os.path.isdir(dirPathName)==False:
                    # since dir does not exist, so try creating it:
                    os.mkdir(dirPathName)
                    
            except Exception as theError:
                logger.error("Error creating directory '%s', Exception was: %s", dirPathName, theError)
            
    
            try:
                fullPathName = os.path.join(  dirPathName , fileNameWithOutExt + ".zip" )
                
                with open(fullPathName , 'wb' ) as fp:
                    n = fp.write( rawData )
                    logger.debug("Wrote %s bytes to file: %s", n, fullPathName)
                    fp.close()
                
                self.parseFetchedData(fullPathName, dirPathName, WorkerID)
                
            except Exception as theError:
                logger.error("Error writing downloaded data to zip file '%s': %s", fullPathName, theError)

        else:
            logger.info("Downloaded data zip file '%s' is not of sufficient size (its length %s is not more than %s), hence ignoring it."
                        , fullPathName, len(rawData), self.minArticleLengthInChars )

        # save count of characters of downloaded data for the given URL
        self.uRLdata[uRLtoFetch] = sizeOfDataDownloaded


    def fetchRawDataFromURL(self, uRLtoFetch, WorkerID ):
        """ fetching content From URL """
        
        logger.debug("Downloading Raw Data for URL %s, Worker ID %s", uRLtoFetch.encode(), WorkerID)
        downloadedData = ""
        
        try:
            #  retrieve data from the URL
            httpsResponse = requests.get( uRLtoFetch
                                          , headers=self.customHeader
                                          , proxies=self.proxies
                                          , verify=False # disables checking CA certificates of proxies
                                           )
            
            downloadedData = httpsResponse.content
            
        except Exception as e:
            logger.error("Exception downloading raw data From URL %s: %s", uRLtoFetch, e )
        
        return(downloadedData)




    def parseFetchedData(self, zipFileName, dataDirForDate, WorkerID ):
        """Parse the fetched Data"""
        
        zipDatafile = None
        
        logger.debug("Expanding and parsing the fetched Zip archive, WorkerID = %s", WorkerID)
        
        try:
            zipDatafile = zipfile.ZipFile(zipFileName, mode='r' )
            
            for memberZipInfo in zipDatafile.infolist():
                
                if memberZipInfo.filename.find('Readme.txt')<0:
                    
                    logger.debug("Extracting file '%s' from zip archive.", memberZipInfo.filename )
                    
                    zipDatafile.extract( memberZipInfo, path=dataDirForDate)
                    
                    # rename extracted files to prefix module name:
                    # memberZipInfo.filename -> self.type().__name__ + '_' + memberZipInfo.filename
                    logger.debug("Renaming %s to %s"
                                , os.path.join( dataDirForDate, memberZipInfo.filename )
                                , os.path.join( dataDirForDate, type(self).__name__ + "_" + memberZipInfo.filename ) )
                    
                    os.rename( os.path.join( dataDirForDate, memberZipInfo.filename )
                               , os.path.join( dataDirForDate, type(self).__name__ + "_" + memberZipInfo.filename ) )
            
            
            zipDatafile.close()
            
            # delete zip file as its no longer required:
            os.remove(zipFileName)
            
        except Exception as e:
            logger.error("Error parsing the fetched Zip archive: %s", e)
            
        



    def getUniqueIDFromURL(self, URLToFetch):
        """ get Unique ID From URL by extracting RegEx patterns matching any of urlMatchPatterns """
        
        # use today's date as default
        uniqueString = datetime.now().strftime('%d%m%y')
        date_obj = None
        
        if len(URLToFetch) > 6:
            
            for urlPattern in self.urlMatchPatterns:
                
                try:
                    
                    result = urlPattern.search(URLToFetch)
                    
                    uniqueString = result.group(2)
                    
                    date_obj = datetime.strptime( uniqueString, '%d%m%y' )
                    
                    # if we did not encounter any error till this point, then this is the answer, so exit loop
                    break
                
                except Exception as e:
                    logger.debug("Error identifying unique ID of URL: %s , URL was: %s, Pattern: %s"
                                 , e
                                 , URLToFetch.encode('ascii')
                                 , urlPattern )

        return( (date_obj, uniqueString) )
    
    

## end of file ##