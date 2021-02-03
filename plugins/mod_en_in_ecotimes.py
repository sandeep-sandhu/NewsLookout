#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: mod_en_in_ecotimes.py
 Application: The NewsLookout Web Scraping Application
 Date: 2020-01-11
 Purpose: Plugin for the Economic Times
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

# import this project's python libraries:
from baseModule import baseModule
from scraper_utils import normalizeURL, NewsArticle, cutStrBetweenTags, cutStrFromTag, calculateCRC32
from scraper_utils import retainValidArticles, removeInValidArticles
from scraper_utils import Types


####################################


class mod_en_in_ecotimes(baseModule):
    """ Web Scraping plugin: mod_en_in_ecotimes
    For Economic times Newspaper
    Language: English
    Country: India
    """
    
    minArticleLengthInChars = 400
    pluginType = Types.MODULE_NEWS_CONTENT # implies web-scraper for news content
    mainURL = 'https://economictimes.indiatimes.com/industry'
    rss_feed = "https://economictimes.indiatimes.com/rssfeedsdefault.cms"
    
    # fetch only URLs containing the following substrings:
    validURLStringsToCheck = ['economictimes.indiatimes.com/'
          , 'business']
    
    # never fetch these URLs:
    invalidURLSubStrings = ['//www.indiatimes.com/'
                                 , '//timesofindia.indiatimes.com/'
                                 , '//economictimes.indiatimes.com/et-search/' ]
    
    urlUniqueRegexps = ["(http.+\/economictimes\.indiatimes\.com)(.*\/)([0-9]+)(\.cms)"
                       , "(\.economictimes\.indiatimes\.com\/)(.+\/)([0-9]+)" ]

    articleDateRegexps = { "(<meta name=\"created-date\" content=\")([a-zA-Z]{3}, [0-9]{1,2} [a-zA-Z]{3} 20[0-9]{2} [0-9]{1,2}:[0-9]{2}:[0-9]{2} \+0530)(\" \/>)" : "%a, %d %b %Y %H:%M:%S %z" # Thu, 23 Jan 2020 11:00:00 +0530
                          , "(<meta name=\"publish-date\" content=\")([a-zA-Z]{3}, [0-9]{1,2} [a-zA-Z]{3} 20[0-9]{2} [0-9]{1,2}:[0-9]{2}:[0-9]{2} \+0530)(\" \/>)": "%a, %d %b %Y %H:%M:%S %z" # Thu, 23 Jan 2020 11:00:00 +0530
                          , "(\"datePublished\":\")([a-zA-Z]{3}, [0-9]{1,2} [a-zA-Z]{3} 20[0-9]{2} [0-9]{1,2}:[0-9]{2}:[0-9]{2} \+0530)(\")": "%a, %d %b %Y %H:%M:%S %z" # Thu, 23 Jan 2020 11:00:00 +0530
                          , "(\"dateModified\":\")([a-zA-Z]{3}, [0-9]{1,2} [a-zA-Z]{3} 20[0-9]{2} [0-9]{1,2}:[0-9]{2}:[0-9]{2} \+0530)(\")": "%a, %d %b %Y %H:%M:%S %z" # Thu, 23 Jan 2020 12:05:00 +0530
                          , "(<li class=\"date\">Updated: )([a-zA-Z]+ [0-9]{1,2}, 20[0-9]{2}, [0-9]{1,2}:[0-9]{2})( IST<\/li>)": "%B %d, %Y, %H:%M" # January 23, 2020, 12:05
                          , "(data\-date=\")([0-9]{4}\-[0-9]{2}\-[0-9]{2})(\">)": "%Y-%m-%d" # 2020-01-23
                          , "(data\-article\-date=')([0-9]{4}\-[0-9]{2}\-[0-9]{2})(')" : "%Y-%m-%d" # 2020-01-23
                          , "(\"datePublished\": \")(20[0-9]{2}\-[0-9]{2}\-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(\+05:30\")" : "%Y-%m-%dT%H:%M:%S" # "datePublished": "2020-01-30T22:12:00+05:30"
                          , "(\"dateModified\": \")(20[0-9]{2}\-[0-9]{2}\-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(\+05:30\")" : "%Y-%m-%dT%H:%M:%S"  # "dateModified": "2020-01-30T22:15:00+05:30"
                          , "('publishedDate': ')(20[0-9]{2}\-[0-9]{2}\-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(\+05:30')" : "%Y-%m-%dT%H:%M:%S" # 'publishedDate': '2020-01-01T22:39:00+05:30'
                           }
    dateMatchPatterns = dict()
                    
    articleIndustryRegexps = [ "(data-category-name=')([a-zA-Z0-9 \-,]+)(')" ]
    
    authorRegexps = ["(agency:')([a-zA-Z0-9 ]+)(')"
                     , "(channel :  ')([a-zA-Z0-9 ]+)(',)"
                     , "(agename=')([a-zA-Z0-9 ]+)(';)"
                     , "(<div class=\"ag tac\">)([a-zA-Z0-9 ]+)(<\/div>)"
                     , "(\"publisher\":{\"@type\":\"Organization\",\"name\":\")([a-zA-Z0-9 ]+)(\")"
                     , "(\.economictimes\.indiatimes\.com\/agency\/.+\" target=\"_blank\">)([a-zA-Z0-9 ]+)(<\/a>)"
                     ]
    authorMatchPatterns = []
    
    urlMatchPatterns = []
    
    invalidTextStrings = ["If you choose to ignore this message, we'll assume that you are happy to receive all cookies"]
    
    allowedDomains = ["economictimes.indiatimes.com"]
    listOfURLS = []
    uRLdata = dict()
    
    
    
    def __init__(self):
        """ Initialize the object """
        
        super().__init__()




    def parseFetchedData(self, uRLtoFetch, rawData, WorkerID ):
        """Parse the fetched Data"""
        
        logger.debug("Parsing the fetched Data, WorkerID = %s", WorkerID)
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
            
            
            if rawData.publish_date==None or rawData.publish_date=='' or (not rawData.publish_date) :
                # extract published date by searching for specific tags
                rawData.publish_date = self.extractPublishedDate(rawData.html )
            
            
            # identify news agency/source if it is not properly recognized:
            if ( len(rawData.authors) < 1 
                 or ( len(rawData.authors)>0
                       and rawData.authors[0].find('<')>=0 )
                  ):
                
                rawData.set_authors(
                    self.extractAuthors( rawData.html )
                     )
            
            rawData.nlp()
            
        except Exception as e:
            logger.error("Error parsing raw data for URL %s: %s", uRLtoFetch, e)
            
        try:
            parsedCleanData = NewsArticle()
            
            parsedCleanData.importNewspaperArticleData( rawData )
            
            parsedCleanData.setIndustries(
                self.extractIndustries(rawData.html)
                )

        except Exception as e:
            logger.error("Error storing parsed data for URL %s: %s", uRLtoFetch, e)
            
        return( parsedCleanData )




    def extractIndustries(self, htmlText):
        """  extract Industries """
        industries = []
        
        try:
            logger.debug("Extracting industries identified by the article.")
            article_html = BeautifulSoup( htmlText, 'lxml')
            
            
            
        except Exception as e:
            logger.error(e)
        
        return(industries)



    def extractAuthors( self, htmlText ):
        """ Extract Authors/Agency/Source of the article from its raw html code """
        
        authors = []
        authorStr = ""
        
        for authorMatch in self.authorMatchPatterns:
            
            logger.debug("Trying match pattern: %s", authorMatch)
            
            try:
                result = authorMatch.search(htmlText)
                authorStr = result.group(2)
                authors = authorStr.split(',')
                # At this point, the text was correctly extracted, so exit the loop
                break
            
            except Exception as e:
                logger.debug("Exception identifying article authors: %s; string to parse: %s, URL: %s"
                             , e, authorStr, self.URLToFetch )
                
        if authorStr == "":
            logger.debug("Re-attempting identifying article's author for URL: %s"
                             , self.URLToFetch )
            
            try:
                article_html = BeautifulSoup( htmlText, 'lxml')
                
                body_root = article_html.find( "span", "ag")
                
                if len(body_root.getText() ) <1:
                    if body_root.find("img")==None:
                        authors = []
                    else:
                        authors = [ body_root.img['alt'] ]
                else:
                    authors = [ body_root.getText() ]
    
            except Exception as e:
                logger.error("Error when re-attempting extracting authors from article: %s, URL: %s", e, self.URLToFetch)
            
            
        return( authors )
    



    def extractPublishedDate( self, htmlText ):
        """ Extract Published Date from html"""

        # default is todays date:
        date_obj = datetime.now()
        curr_datetime = date_obj
        
        dateString = ""
        datetimeFormatStr = ""
    
        for dateRegex in self.dateMatchPatterns.keys():
            ( datePattern, datetimeFormatStr ) = self.dateMatchPatterns[dateRegex]
            
            try:
                result = datePattern.search(htmlText)
                dateString = result.group(2)
                date_obj = datetime.strptime( dateString, datetimeFormatStr )
                
                # if we did not encounter any error till this point, then this is the answer, so exit loop
                break
            
            except Exception as e:
                logger.debug("Exception identifying article date: %s, string to parse: %s, using regexp: %s, URL: %s"
                             , e, dateString, dateRegex, self.URLToFetch )
                
        if curr_datetime == date_obj:
            logger.error("Exception identifying article's date for URL: %s"
                             , self.URLToFetch )
            
        return date_obj


    
    
    def extractArticlesListWithBeauSoup(self ):
        """ extract Article listing using the Beautiful Soup library 
         source list from RSS feed """
         
        self.listOfURLS = []
        
        try:
            
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
        """ extract Article Text with newspaper library """
        pass
    
    

    def extractArticleTextWithBeauSoup(self, htmlContent ):
        """ extract Article Text using Beautiful Soup library """
        
        articleText = ""
        
        try:
            # get article text data by parsing specific tags:
            article_html = BeautifulSoup( htmlContent, 'lxml')
            
            body_root = article_html.find_all( "div", "artText")
            
            if len(body_root)>0:
                    articleText = body_root[0].getText()
            else:
                body_root = article_html.find_all( "div", "artcle-txt")
                if len(body_root)>0:
                    articleText = body_root[0].getText()
                
            #except Warning as w:
            #    logger.warn("Warning when extracting text via BeautifulSoup: %s", w)
            
        except Exception as e:
            logger.error("Exception extracting article via tags: %s", e )
            
        return( articleText )




    def getUniqueIDFromURL(self, URLToFetch):
        """ get Unique ID From URL by extracting RegEx patterns matching any of urlMatchPatterns """
        
        uniqueString = ""
        crcValue = "crc"
        
        try:
            # calculate CRC string if url are not usable:
            crcValue = str( calculateCRC32( URLToFetch.encode('utf-8') ) )
            uniqueString = crcValue
            
        except Exception as e:
            logger.error("Error calculating CRC32 of URL: %s , URL was: %s", e, URLToFetch.encode('ascii'))
        
        
        if len(URLToFetch) > 6:
            for urlPattern in self.urlMatchPatterns:
                
                try:
                    result = urlPattern.search(URLToFetch)
                    uniqueString = result.group(3)
                    # if we did not encounter any error till this point, then this is the answer, so exit loop
                    break
                
                except Exception as e:
                    logger.debug("Retrying identifying unique ID of URL, error: %s , URL was: %s, Pattern: %s"
                                 , e
                                 , URLToFetch.encode('ascii')
                                 , urlPattern )
                    
        else:
            logger.error("Invalid URL found when trying to identify unique ID: %s", URLToFetch.encode('ascii') )
            
        if uniqueString == crcValue:
            logger.error("Error identifying unique ID of URL: %s, hence using CRC32 code: %s"
                             , URLToFetch.encode('ascii'), uniqueString )
        
        return(uniqueString)
    
    

## end of file ##