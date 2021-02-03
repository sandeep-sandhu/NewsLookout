#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: scraper_utils.py
 Application: The NewsLookout Web Scraping Application
 Date: 2020-01-11
 Purpose: Helper class with utility functions supporting the web scraper
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
import sys, os
import importlib
from datetime import date, datetime
import json
from json import JSONEncoder
import base64
import bz2
from collections import OrderedDict 
import http.cookiejar
from http.cookiejar import CookieJar, DefaultCookiePolicy

from crccheck.crc import Crc32, CrcXmodem
from crccheck.checksum import Checksum32
import zlib
import zipfile


# import web retrieval and text processing python libraries:
import nltk
from bs4 import BeautifulSoup
import newspaper
from newspaper import Article

# setup logging
import logging
logger = logging.getLogger(__name__)

####################################

__author__ = "Sandeep Singh Sandhu"
__copyright__ = "Copyright 2020, The Python WebScraper"
__credits__ = ["Sandeep Singh Sandhu"]
__license__ = "GPL"
__version__ = "1.6"
__maintainer__ = "Sandeep Singh Sandhu"
__email__ = "sandeep.sandhu@gmx.com"
__status__ = "Production"

####################################


class Types:
    MODULE_NEWS_CONTENT=1
    MODULE_NEWS_AGGREGATOR=2
    MODULE_NEWS_API=4
    MODULE_DATA_CONTENT=8
    MODULE_DATA_PROCESSOR=16
    
    TASK_GET_URL_LIST=1
    TASK_GET_DATA=2
    TASK_PROCESS_DATA=4



def retainValidArticles(articleList, validURLPatternsList ):
    """ retain only valid URLs """
    
    valid_articles = []

    for article in articleList:

        # only keep following URLs which contain strings matching pattern
        for strCheck in validURLPatternsList:
            try:
                if type( article).__name__ =='Article':
                    
                    if article.url.find(strCheck) > -1 and len(article.url)>9 :
                        # this article URL contains the valid article substring, hence, add it to the valid list
                        valid_articles.append(article.url)
                        break;
                    
                elif type( article).__name__ =='str':
                    
                    if article.find(strCheck) > -1 and len(article)>9 :
                        valid_articles.append(article)
                        break;
                    
            except Exception as e:
                logger.error( "ERROR retaining valid article list: %s", e )
        

    # logger.debug( "Count of valid articles remaining: %s", len(valid_articles) )
    return(valid_articles)



def removeInValidArticles(articleList, invalidURLPatternsList ):
    """ remove InValid Articles """
    
    valid_articles = []

    for article in articleList:
        
        try:
            # delete URLs which contain any of the strings matching pattern
            checkCondition = True
            
            for strCheck in invalidURLPatternsList:
                
                if type( article).__name__ =='Article':
                    checkCondition = checkCondition and (article.url.find(strCheck) == -1)
                    
                elif type( article).__name__ =='str':
                    checkCondition = checkCondition and (article.find(strCheck) == -1)
                
            if checkCondition==True:
                valid_articles.append(article)
                
        except Exception as e:
            logger.error( "ERROR filtering out invalid article list: %s", e )
        
    return(valid_articles)



def cutStrFromTag(sourceStr, startTagStr):
    """ Cut part of the source String starting from substring from Tag till its end """
    resultStr = ""
    
    try:
        start_pos = sourceStr.find(startTagStr) + len(startTagStr)
        
        if start_pos> -1:
            resultStr = sourceStr[ start_pos: ]
            
    except Exception as e:
        logger.error( "ERROR extracting string starting from tags: %s", e )
        
    return( resultStr )



def cutStrBetweenTags(sourceStr, startTagStr, endTagStr):
    """ Cut source string between given substring Tags """

    resultStr = ""
    
    try:
        start_pos = sourceStr.find(startTagStr) + len(startTagStr)
        
        if start_pos> -1:
            snipped_string = sourceStr[ start_pos: ]
            end_pos = snipped_string.find(endTagStr)
            
            if end_pos> -1:
                resultStr = snipped_string[ :end_pos ]
            
    except Exception as e:
        logger.error( "ERROR extracting string between two tags: %s", e )
        
    return( resultStr )



def checkAndParseDate(dateStr):
    """ Check and Parse Date String, set it to todays date if its in future """
    
    logger.debug("Checking date string: %s", dateStr)
    
    try:
        runDate = datetime.strptime( dateStr, '%Y-%m-%d')
        
    except Exception as e:
        logger.error("Invalid date for retrieval (%s): %s; using todays date instead."
                     , dateStr, e )
    
    # get the current local date 
    today = date.today()
    
    if runDate.date() > today:
        logger.error("Date for retrieval (%s) cannot be after today's date; using todays date instead."
                     , runDate.date() )
        runDate = datetime.now()
    
    return( runDate )



def loadPlugins(configData):
    """load only enabled plugins from the class files in the package directory"""
    pluginsDict = dict()
    
    enabledPluginNames = configData['enabledPlugins'].split(',')
    
    plugins_dir = configData['plugins_dir']
    modulesPackageName = os.path.basename(plugins_dir)
    
    sys.path.append( configData['install_prefix'] )
    sys.path.append( plugins_dir )
    
    for modfilename in importlib.resources.contents( modulesPackageName ):
        
        # get full path:
        path = os.path.join(plugins_dir, modfilename)
        
        if os.path.isdir(path):# skip directories
            continue
        
        # get only file name without file extension:
        modName = os.path.splitext(modfilename)[0]
        
        # only then load the module if module is enabled in the config file:
        if modName in enabledPluginNames:
            className = modName # since class names of plugins are same as their module names
            try:
                logger.debug( "Importing web-scraping plugin class: %s", modName )
                classObj = getattr( importlib.import_module( modName , package=modulesPackageName ) , className )
                pluginsDict[ modName ] = classObj()
            except Exception as e:
                logger.error("While importing plugin %s got exception: %s", modName, e)
    
    return pluginsDict



def deDupeList(listWithDuplicates):
    """ dedupe a given List by converting into a dict
    , and then re-converting to a list back again."""
    
    return( list(
                 OrderedDict.fromkeys(
                     listWithDuplicates
                     )
                 )
            )



def normalizeURL(articleURL):
    """ Normalize the URL """
    normalisedURL = articleURL.lower()
    
    # run through url encoder
    
    # resolve relative urls
    
    return(normalisedURL)



def checkAndGetNLTKData():
    """ Check if NLTK taggers and tokernzers are available.
    If not, then download the NLTK Data """
    
    try:
        fsPointer = nltk.data.find('tokenizers/punkt')
        logger.debug("NLTK punkt tokenizers is available.")
    except Exception as e:
        downloadResult = nltk.download('punkt')
        logger.debug("Download of punkt successful? %s", downloadResult)
        
    try:
        fsPointer = nltk.data.find('taggers/maxent_treebank_pos_tagger')
        logger.debug("NLTK maxent_treebank_pos_tagger is available.")
    except Exception as e:
        downloadResult = nltk.download('maxent_treebank_pos_tagger')
        logger.debug("Download of maxent_treebank_pos_tagger successful? %s", downloadResult)
        
    try:
        fsPointer = nltk.data.find('corpora/reuters.zip')
        logger.debug("NLTK reuters is available.")
    except Exception as e:
        downloadResult = nltk.download('reuters')
        logger.debug("Download of reuters successful? %s", downloadResult)
        
    try:
        fsPointer = nltk.data.find('corpora/universal_treebanks_v20.zip')
        logger.debug("NLTK universal_treebanks_v20 is available.")
    except Exception as e:
        downloadResult = nltk.download('universal_treebanks_v20')
        logger.debug("Download of universal_treebanks_v20 successful? %s", downloadResult)

    
    
    

def calculateCRC32(text):    
    """ CRC32 calculation """
    
    crc = zlib.crc32(text) % (2**32)
    
    return( hex( crc) )



def loadAndSetCookies(cookieFileName):
    """ load and Set Cookies from file """
    
    cookieJar = None
    
    try:
        cookieJar = http.cookiejar.FileCookieJar( cookieFileName )
            
    except Exception as theError:
        logger.error("Exception caught opening cookie file: %s", theError)
    
    return( cookieJar )



def getCookiePolicy( listOfAllowedDomains ):
    """ """
    thisCookiePolicy = None
    
    try:

        thisCookiePolicy = http.cookiejar.DefaultCookiePolicy(  blocked_domains=None
                                           , allowed_domains=listOfAllowedDomains
                                           , netscape=True
                                           , rfc2965=False
                                           , rfc2109_as_netscape=None
                                           , hide_cookie2=False
                                           , strict_domain=False
                                           , strict_rfc2965_unverifiable=True
                                           , strict_ns_unverifiable=False
                                           , strict_ns_domain=DefaultCookiePolicy.DomainLiberal
                                           , strict_ns_set_initial_dollar=False
                                           , strict_ns_set_path=False
                                           #, secure_protocols=("https", "wss")
                                           )
        
        thisCookiePolicy.set_allowed_domains(listOfAllowedDomains)
    
    except Exception as e:
        logger.error("Error setting cookie policy: %s", e)
        
    return(thisCookiePolicy)



# data structures for URL lists, and news article data


class completedURLs(JSONEncoder):
    """ completed URLs """

    URLObj = []

    def default(self, o):
        """ default """
        return o.__dict__


    def readFromJSON(self, jsonFileName):
        logger.debug("Reading JSON file to load previously retrieved URLs %s", jsonFileName)
        try:
            with open(jsonFileName , 'r', encoding='utf-8') as fp:
                self.URLObj = json.load(fp)
                fp.close()
        except Exception as e:
            logger.error("Exception caught reading JSON file: %s", e)
            self.URLObj = []


    def writeToJSON(self, jsonFileName):
        try:
            with open(jsonFileName , 'wt', encoding='utf-8') as fp:
                n = fp.write( self.toJSON() )
                fp.close()
        except Exception as e:
            logger.error("Exception caught writing JSON file: %s", e)


    def addURL( self, sURL):
        """ add URL """
        self.URLObj.append( sURL )


    def removeURL( self, sURL):
        """ add URL """
        self.URLObj.remove( sURL )
        
        
    def checkURLExists( self, sURL):
        """ add URL """
        return( sURL in self.URLObj )

    def toJSON(self):
        # dict is object, and lists are converted to arrays
        return json.dumps( self.URLObj )



class NewsArticle(JSONEncoder):
    """ article data structure and object """
    
    urlData = dict()
    uniqueID = ""
    
    
    def setPublishDate(self, publishDate):
        try:
            self.urlData["pubdate"]=str( publishDate.strftime("%Y-%m-%d") )
            
        except Exception as e:
            logger.error("Error setting publish date of article: %s", e)
            self.urlData["pubdate"]=str( datetime.now().strftime("%Y-%m-%d") )



    def setTitle(self, articleTitle):
        self.urlData["title"]=str(articleTitle)



    def getBase64FromHTML(articleHTMLText):
        """ Get Base64 text data From HTML text """
        
        htmlBase64 = ""
        
        try:
            encodedBytes = base64.b64encode(articleHTMLText.encode("utf-8"))
            htmlBase64=str(encodedBytes, "ascii")
            
        except Exception as e:
            logger.error("Error setting html content: %s", e)
        
        return(htmlBase64)

    
    
    def getHTMLFromBase64(htmlBase64):
        """ get HTML text From Base64 data """
        
        decoded_bytes = ""
        
        try:
            base64_bytes = htmlBase64.encode('ascii')
            decoded_bytes = base64.b64decode(base64_bytes)
            
        except Exception as e:
            logger.error("Error setting html content: %s", e)
            
        return( decoded_bytes.decode('utf-8') )
    


    def getText(self):
        return( self.urlData["text"] )



    def getTextSize(self):
        return( len(self.urlData["text"]) )


        
    def cleanText(self, textInput):
        """ clean text, e.g. replace unicode characters, etc. """
        
        if len(textInput)>1:
            
            cleanText = textInput.replace("\u2014", "-")
            
            cleanText = cleanText.replace("\u2013", "-")
            
            cleanText = cleanText.replace("\n", " ")
            
            cleanText = cleanText.replace("\u2019", "'")
            cleanText = cleanText.replace("\u2018", "'")
            cleanText = cleanText.replace("\u201d", "'")
            cleanText = cleanText.replace("\u201c", "'")
            
            cleanText = cleanText.replace("\U0001f642", " ")
            cleanText = cleanText.replace("\u200b", " ")
            
            cleanText = cleanText.replace("\x93", " ")
            cleanText = cleanText.replace("\x94", " ")
            
            cleanText = cleanText.replace("''", "'")
            
        else:
            cleanText = textInput
        
        return( cleanText )
    


    def setKeyWords(self, articleKeyWordsList ):
        self.urlData["keywords"] = articleKeyWordsList
        


    def setText(self, articleText):
        self.urlData["text"]=  self.cleanText( articleText )



    def setIndustries(self, articleIndustryList):
        self.urlData["industries"] =  articleIndustryList



    def setCategory(self, articleCategory):
        self.urlData["category"]=str(articleCategory)
    
    
    def setURL(self, sURL):
        self.urlData["URL"]=str(sURL)


    def setArticleID(self, uniqueID):
        self.uniqueID=uniqueID
        self.urlData["uniqueID"]=str(uniqueID)
        
        
    def setSource(self, sourceName):
        self.urlData["sourceName"]=str(sourceName)


    def toJSON(self):
        """ Converts python object into json.
        See reference page: https://docs.python.org/3/library/json.html """

        return json.dumps( self.urlData )



    def readFromJSON(self, jsonFileName):
        """ read from JSON file into object """
        logger.debug("Reading JSON file to load previously saved article %s", jsonFileName)
        try:

            with open(jsonFileName , 'r', encoding='utf-8') as fp:
                self.urlData = json.load(fp)
                fp.close()
                
        except Exception as theError:
            logger.error("Exception caught reading JSON file: %s", theError)




    def writeFiles(self, fileNameWithOutExt, baseDirName, htmlContent, saveHTMLFile=False ):
        """ write output To JSON and/or html file """
        
        fullHTMLPathName = ""
        fullPathName = ""
        jsonContent = ""
        dirPathName = ""
        
        try:
            dirPathName = os.path.join( baseDirName, self.urlData["pubdate"] )
            
            # first check if directory of given date exists, or not
            if os.path.isdir(dirPathName)==False:
                # dir does not exist, so try creating it:
                os.mkdir(dirPathName)
                
        except Exception as theError:
            logger.error("Exception caught creating directory %s: %s", dirPathName, theError)
        
        try:
            if saveHTMLFile == True:
                fullHTMLPathName = os.path.join(  dirPathName , fileNameWithOutExt + ".html.bz2" )
                with bz2.open(fullHTMLPathName, "wb") as fpt:
                    # Write compressed data to file
                    unused = fpt.write( htmlContent.encode("utf-8") )
                    fpt.close()
                    
        except Exception as theError:
            logger.error("Exception caught writing data to html file %s: %s", fullHTMLPathName, theError)
        
        try:
            jsonContent = self.toJSON()
            
            fullPathName = os.path.join(  dirPathName , fileNameWithOutExt + ".json" )
            with open(fullPathName , 'wt', encoding='utf-8') as fp:
                n = fp.write( jsonContent )
                fp.close()
                
        except Exception as theError:
            logger.error("Exception caught saving data to json file %s: %s", fullPathName, theError)
            # throw the exception back to calling routines:
            raise theError
                    



    def importNewspaperArticleData(self, newspaperArticle):
        """ Import Data from newspaper library's Article class """
        
        try:
            # check and get authors/sources
            if len(newspaperArticle.authors)>0:
                self.setSource( newspaperArticle.authors[0] )
            else:
                self.setSource("")
            
            # set publishDate as current date time if article's publish_date is null
            if newspaperArticle.publish_date==None or newspaperArticle.publish_date=='':
                self.setPublishDate( datetime.now() )
            else:
                self.setPublishDate( newspaperArticle.publish_date )
            
            self.setText( newspaperArticle.text )
            self.setTitle( newspaperArticle.title )
            self.setURL( newspaperArticle.url)
            
            allKeywords = []
            if type( newspaperArticle.keywords ).__name__ =='list':
                allKeywords = allKeywords + newspaperArticle.keywords
            
            if type( newspaperArticle.meta_data['keywords'] ).__name__ =='str':
                allKeywords = allKeywords + newspaperArticle.meta_data['keywords'].split(',')
                
            if type( newspaperArticle.meta_data['news_keywords'] ).__name__ =='str':
                allKeywords = allKeywords + newspaperArticle.meta_data['news_keywords'].split(',')
            
            self.setKeyWords( allKeywords )
            
        except Exception as theError:
            logger.error("Exception caught importing newspaper article: %s", theError )        
        

## end of file ##