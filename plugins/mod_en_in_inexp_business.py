#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: mod_en_in_inexp_business.py
 Application: The NewsLookout Web Scraping Application
 Date: 2020-01-11
 Purpose: Plugin for the Indian Express, Business
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

import logging
from ast import parse
logger = logging.getLogger(__name__)

# import web retrieval and text processing python libraries:
from bs4 import BeautifulSoup
import newspaper
from newspaper import Article, Source
import nltk
import requests
from urllib3.exceptions import InsecureRequestWarning
import lxml
import cchardet

# import this project's python libraries:
from baseModule import baseModule
from scraper_utils import normalizeURL, NewsArticle, cutStrBetweenTags, cutStrFromTag, calculateCRC32
from scraper_utils import retainValidArticles, removeInValidArticles
from scraper_utils import Types

####################################



class mod_en_in_inexp_business(baseModule):
    """ Web Scraping plugin: mod_en_in_inexp_business
    For Indian Express Newspaper
    """
    
    minArticleLengthInChars = 400
    pluginType = Types.MODULE_NEWS_CONTENT # implies web-scraper for news content
    mainURL = 'https://www.newindianexpress.com/business/'
    rss_feed = "https://www.newindianexpress.com/Nation/rssfeed/?id=170&getXmlFeed=true"
    
    # fetch only URLs containing the following substrings:
    validURLStringsToCheck = ['https://www.newindianexpress.com/nation/'
          , 'business'
          , 'https://www.newindianexpress.com/opinions/'
          , 'https://www.newindianexpress.com/world/'
          , 'https://indianexpress.com/' ]
    
    # never fetch these URLs:
    invalidURLSubStrings = []
    
    urlUniqueRegexps = ["(^https.*)(\-)([0-9]+)(\.html$)"
                        , "(^https\://indianexpress.com/article/.*)(\-)([0-9]+)(/$)"
                        , "(^https\://indianexpress.com/article/.*)(\-)([0-9]+)(\.html$)" ]
    
    invalidTextStrings = []
    
    articleDateRegexps = {}
    authorRegexps = []    
    dateMatchPatterns = dict()    
    urlMatchPatterns = []
    authorMatchPatterns = []
    
    allowedDomains = ["indianexpress.com", "www.newindianexpress.com"]
    listOfURLS = []
    uRLdata = dict()
    urlMatchPatterns = []
    
    
    def __init__(self):
        """ Initialize the object """
        
        super().__init__()
        



    def getURLListFromRSSFeed(self):
        """ source list from RSS feed """
        listOfURLs = []
        
        try:
            # Suppress only the single warning from urllib3 for not verifying SSL certificates
            requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
            
            httpsRequests = requests.get( self.rss_feed
                                          , headers=self.customHeader
                                          , proxies=self.proxies
                                          , verify=False # disables checking CA certificates of proxies
                                           )
            
            rss_feed_xml = BeautifulSoup( httpsRequests.text , 'lxml-xml')
            
            for item in rss_feed_xml.channel:
                if item.name == "item":
                    listOfURLs.append( normalizeURL( item.link.contents[0]) )
        except Exception as e:
            logger.error("Error getting urls listing from RSS feed: %s", e)
        
        return(listOfURLs)
    



    def getUniqueIDFromURL(self, uRLtoFetch):
        """ get Unique ID From URL """
        
        uniqueString = ""
        
        try:
            # calculate CRC string if url are not usable:
            uniqueString = str( calculateCRC32( uRLtoFetch.encode('utf-8') ) )
            
        except Exception as e:
            logger.error("Error calculating CRC32 of URL: %s , URL was: %s", e, uRLtoFetch.encode('ascii'))
        
        
        if len(uRLtoFetch) > 6:
            for urlPattern in self.urlMatchPatterns:
                
                try:
                    result = urlPattern.search(uRLtoFetch)
                    uniqueString = result.group(3)
                    # if not error till this point then exit
                    break
                
                except Exception as e:
                    logger.error("Error identifying unique ID of URL: %s , URL was: %s, Pattern: %s"
                                 , e
                                 , uRLtoFetch.encode('ascii')
                                 , urlPattern )
                    
        else:
            logger.error("Invalid URL found when trying to identify unique ID: %s", uRLtoFetch.encode('ascii') )
            
        return(uniqueString)
        



    def extractIndustries(self, htmlText):
        """  extract Industries """
        
        industries = []
        
        try:
            logger.debug("Extracting industries identified by the article.")
            article_html = BeautifulSoup( htmlText, 'lxml')
            
            body_root = article_html.find( "span", "ag")
            
        except Exception as e:
            logger.error(e)
            
        return(industries)


        
    def extractAuthors( self, htmlText ):
        """ extract Authors/Agency/Source from html"""

        # default authors are blank list:
        authors = []
        
        try:
            strNewsAgent = cutStrBetweenTags( htmlText, '<span class="author_des">By', '</span></span>' )
            strNewsAgent = cutStrBetweenTags( strNewsAgent, 'target="_blank">', '</a>' )
            
            if len(strNewsAgent)<1:
                raise Exception("Could not identify news agency/source.")
            else:
                authors = [strNewsAgent]
            
        except Exception as e:
            logger.error("Error extracting news agent from text: %s", e)            
        
        return( authors )


        
    def extractPublishedDate( self, htmlText ):
        """ extract Published Date from html"""
        
        # default is todays date-time:
        date_obj = datetime.now()
        
        # extract published date
        strJSDatePart = cutStrBetweenTags( htmlText, '"datePublished":"', '+05:30","dateModified"')
        
        try:
            date_obj = datetime.strptime(strJSDatePart, '%Y-%m-%dT%H:%M:%S')
            
        except Exception as e:
            logger.error("Exception converting publish date string to object: %s, string to parse: %s"
                         , e, strTimeUpdated )
        
        return date_obj        




    def extractArticlesListWithBeauSoup(self ):
        """ Extract the article listing from RSS feed using the Beautiful Soup library """
        
        self.listOfURLS = []
        
        try:
            # Suppress only the single warning from urllib3 for not verifying SSL certificates
            requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
            
            httpsRequests = requests.get( self.rss_feed
                                          , headers=self.customHeader
                                          , proxies=self.proxies
                                          , verify=False # disables checking CA certificates of proxies
                                           )
            
            rss_feed_xml = BeautifulSoup( httpsRequests.text , 'lxml-xml')
            
            for item in rss_feed_xml.channel:
                if item.name == "item":
                    self.listOfURLS.append( normalizeURL( item.link.contents[0]) )
            
        except Exception as e:
            logger.error("Error getting urls listing from RSS feed: %s", e)
        
        self.listOfURLS = retainValidArticles( self.listOfURLS
                                          , self.validURLStringsToCheck)   



    def extractArticleTextWithNewsP(self, sURL ):
        """ extract Article Text """
        pass
    
    

    def extractArticleTextWithBeauSoup(self, htmlContent ):
        """ Extract article's text using the Beautiful Soup library """
        articleText = ""
        
        try:
            # get article text data by parsing specific tags:
            article_html = BeautifulSoup( htmlContent, 'lxml')
            
            # <div id="storyContent" class="articlestorycontent">
            body_root = article_html.find_all( "div", "articlestorycontent")
            if len(body_root)>0:
                    articleText = body_root[0].getText()
            
            #except Warning as w:
            #    logger.warn("Warning when extracting text via BeautifulSoup: %s", w)
            
        except Exception as e:
            logger.error("Exception extracting article via tags: %s", e )
            
        return( articleText )        
    
    


    def parseFetchedData(self, uRLtoFetch, rawData, WorkerID ):
        """Parse the fetched Data"""
        
        logger.debug("parse Fetched Data at WorkerID = %s", WorkerID)
        parsedCleanData = None
        
        try:
            rawData.parse()
            
            invalidFlag = False
            for badString in self.invalidTextStrings:
                if rawData.text.find(badString) >= 0:
                    logger.error("Found invalid text in data extracted: %s", badString)
                    invalidFlag = True
                    rawData.text = ""
            
            # check if article content is not valid or is too little
            if invalidFlag == True or len(rawData.text) < self.minArticleLengthInChars:
                rawData.text = self.extractArticleTextWithBeauSoup( rawData.html)
            
            # identify news agency/source if it is not properly recognized:
            if len(rawData.authors)<1:
                
                rawData.set_authors(
                     self.extractAuthors(
                         rawData.html)
                      )
            
            if rawData.publish_date==None or rawData.publish_date=='' or (not rawData.publish_date) :
                # extract published date by searching for specific tags
                rawData.publish_date = self.extractPublishedDate( rawData.html )
        
        
            rawData.nlp()
                
        except Exception as e:
            logger.error("Error parsing raw data for URL %s: %s", uRLtoFetch, e)
        
        
        try:
            parsedCleanData = NewsArticle()

            parsedCleanData.importNewspaperArticleData(rawData)

            parsedCleanData.setIndustries(
                self.extractIndustries(rawData.html)
                )

        except Exception as e:
            logger.error("Error storing parsed data for URL %s: %s", uRLtoFetch, e)
            
        return( parsedCleanData )





## end of file ##