#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: data_structs.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-06-01
 Purpose: Helper class with data structures supporting the web scraper
 Copyright 2021, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu, sandeep.sandhu@gmx.com

Provides:
    Types
    URLListHelper
    ExecutionResult
    NewsArticle


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

# import standard python libraries:
import logging
import os
from datetime import datetime
import json
import queue
from json import JSONEncoder
import bz2
import base64
import sqlite3 as lite
import re

# import internal libraries
from scraper_utils import deDupeList, fixSentenceGaps


##########

# setup logging
logger = logging.getLogger(__name__)

##########


class Types:
    MODULE_NEWS_CONTENT = 1
    MODULE_NEWS_AGGREGATOR = 2
    MODULE_NEWS_API = 4
    MODULE_DATA_CONTENT = 8
    MODULE_DATA_PROCESSOR = 16

    TASK_GET_URL_LIST = 32
    TASK_GET_DATA = 64
    TASK_PROCESS_DATA = 128

    STATE_GET_URL_LIST = 10
    STATE_FETCH_CONTENT = 20
    STATE_PROCESS_DATA = 40
    STATE_STOPPED = 80
    STATE_NOT_STARTED = 160

    def decodeNameFromIntVal(typeIntValue):
        attrNames = dir(Types)
        for name in attrNames:
            attrIntVal = getattr(Types, name, None)
            if attrIntVal == typeIntValue:
                return(name)


##########

# objects/data structures for URL lists, and news article data


class ScrapeError(Exception):
    pass


class URLListHelper(JSONEncoder):
    """ Utility class that saves and retrieves completed URLs
    Uses semaphores extensively to avoid database lock and access problems
    when writing/reading data in this multi-threaded application
    """
    ddl_url_table = str('create table if not exists URL_LIST' +
                        '(url TEXT, plugin varchar(100), pubdate DATE, rawsize long, datasize long)')
    ddl_pending_urls_table = str('create table if not exists pending_urls' +
                                 '(url varchar(255), plugin_name varchar(100), attempts integer)')
    # NOTE: The TIMESTAMP field accepts a string in ISO 8601 format 'YYYY-MM-DD HH:MM:SS.mmmmmm' or datetime.datetime object:
    ddl_failed_urls_table = str('create table if not exists FAILED_URLS' +
                                '(url TEXT, plugin_name varchar(100), failedtime timestamp)')

    def __init__(self, dataFileName, dbAccessSemaphore):
        """ Initialize the object """
        self.dbFileName = dataFileName
        self.dbAccessSemaphore = dbAccessSemaphore
        self.completedQueue = queue.Queue()

    def default(self, o):
        """ default """
        return o.__dict__

    def openConnFromfile(dataFileName):
        """ Read SQLite database file and get its connection.
        This db stores previously saved URLs.
        """
        sqlCon = None
        try:
            logger.debug("Trying to open connection to history URLs sqlite DB file '%s'", dataFileName)
            sqlCon = lite.connect(dataFileName, detect_types=lite.PARSE_DECLTYPES | lite.PARSE_COLNAMES)
            cur = sqlCon.cursor()
            # identify if tables are missing, if so create these:
            cur.execute(URLListHelper.ddl_url_table)
            cur.execute(URLListHelper.ddl_pending_urls_table)
            cur.execute(URLListHelper.ddl_failed_urls_table)
            sqlCon.commit()
        except lite.Error as e:
            logger.error("SQLite database error connecting to previous saved URLs db: %s", e)
        except Exception as e:
            logger.error("Error connecting to previous saved URLs db: %s", e)
        return(sqlCon)

    def printDBStats(self):
        """ print SQLite database Stats and version no.
        """
        sqlCon = None
        try:
            logger.debug("Print SQLite stats: Waiting for db exclusive access...")
            acqResult = self.dbAccessSemaphore.acquire()
            if acqResult is True:
                logger.debug("Print SQLite stats: Got exclusive db access")
                sqlCon = URLListHelper.openConnFromfile(self.dbFileName)
                cur = sqlCon.cursor()
                cur.execute('SELECT SQLITE_VERSION()')
                data = cur.fetchone()
                SQLiteVersion = data[0]
                cur.execute('SELECT count(*) from URL_LIST')
                data = cur.fetchone()
                logger.info(
                    "Total count of URLs retrieved = %s, History Dataset SQLite version: %s", data[0], SQLiteVersion)
        except Exception as e:
            logger.error("While showing previously retrieved URLs stats: %s", e)
        finally:
            if sqlCon:
                sqlCon.close()
            self.dbAccessSemaphore.release()
            logger.debug("Print SQLite stats: Released exclusive db access")

    def removeAlreadyFetchedURLs(self, newURLsList, pluginName):
        """ Remove already fetched URLs from given list by searching history database
        """
        filteredList = []
        sqlCon = None
        try:
            logger.debug("Remove already fetched URLs: Waiting for db exclusive access...")
            acqResult = self.dbAccessSemaphore.acquire()
            if acqResult is True:
                logger.debug("Remove already fetched URLs: Got exclusive db access")
                sqlCon = URLListHelper.openConnFromfile(self.dbFileName)
                cur = sqlCon.cursor()
                if newURLsList is not None:
                    for listItem in newURLsList:
                        result = cur.execute('select url from URL_LIST where url = ? union all ' +
                                             'select url from FAILED_URLS where url = ?',
                                             (listItem, listItem))
                        rowset = result.fetchall()
                        if rowset is not None and len(rowset) > 0 and rowset[0][0] == listItem:
                            rowset = None
                            # logger.debug('URL already fetched or failed, so not adding to pending list. URL = %s',
                            #     rowset[0][0])
                        else:
                            filteredList.append(listItem)
        except Exception as e:
            logger.error("%s: While removing already fetched URLs: %s", pluginName, e)
        finally:
            if sqlCon:
                sqlCon.close()
            self.dbAccessSemaphore.release()
            logger.debug("Removed already fetched URLs for plugin %s: Released exclusive db access", pluginName)
        return(filteredList)

    def retrieveTodoURLList(self, pluginName):
        """ Retrieve URL's list from the table pending_urls for given plugin name"""
        URLsFromSQLite = []
        sqlCon = None
        try:
            logger.debug("Fetching pending url list: Waiting for db exclusive access...")
            acqResult = self.dbAccessSemaphore.acquire()
            if acqResult is True:
                sqlQuery = "select url from pending_urls where plugin_name='" + pluginName + "'"
                sqlCon = URLListHelper.openConnFromfile(self.dbFileName)
                cur = sqlCon.cursor()
                # execute query and get results:
                cur.execute(sqlQuery)
                allResults = cur.fetchall()  # fill results into list
                for urlTuple in allResults:
                    URLsFromSQLite.append(urlTuple[0])
        except Exception as e:
            logger.error("%s: Error when fetching pending url list from sqlite db: %s", pluginName, e)
        finally:
            if sqlCon:
                sqlCon.close()
            self.dbAccessSemaphore.release()
            logger.debug("Fetched pending url list for plugin %s: Released exclusive db access.", pluginName)
        return(URLsFromSQLite)

    def addURLsToPendingTable(self, urlList, pluginName):
        """ Add URLs To Pending Table
        Check duplicates using SQL:
        select count(*), url from pending_urls group by url having count(*)>1
        """
        sqlCon = None
        try:
            logger.debug("Add URL list to pending table in db: Waiting to get db exclusive access...")
            acqResult = self.dbAccessSemaphore.acquire(timeout=30)
            if acqResult is True:
                logger.debug("Adding URL list to pending table for plugin %s: Got exclusive db access.", pluginName)
                sqlCon = URLListHelper.openConnFromfile(self.dbFileName)
                cur = sqlCon.cursor()
                urlList = deDupeList(urlList)
                for sURL in urlList:
                    cur.execute('insert into pending_urls (url, plugin_name) values (?, ?)',
                                (sURL, pluginName))
                sqlCon.commit()
        except Exception as e:
            logger.error("Error while adding URL list to pending table: %s", e)
        finally:
            if sqlCon:
                sqlCon.close()
            self.dbAccessSemaphore.release()
            logger.debug("Completed adding URL list to pending table for plugin %s: Released exclusive db access.", pluginName)

    def addURLToFailedTable(self, sURL, pluginName, failTime):
        """ Add URL to failed URLs table.
        To avoid duplicates, add URL if it does not already exist in table.
        """
        sqlCon = None
        try:
            logger.debug("Add URL list to pending table in db: Waiting to get db exclusive access...")
            acqResult = self.dbAccessSemaphore.acquire(timeout=30)
            if acqResult is True:
                logger.debug("Add URL list to pending table in db: Got exclusive db access")
                sqlCon = URLListHelper.openConnFromfile(self.dbFileName)
                cur = sqlCon.cursor()
                # get count of url to check if it already exists:
                result = cur.execute('select count(*) from FAILED_URLS where url = ?', (sURL,))
                rowset = result.fetchall()
                # To avoid duplicates, add only if it does not exist:
                if rowset is not None and len(rowset) > 0 and rowset[0][0] == 0:
                    cur.execute('insert into FAILED_URLS (url, plugin_name, failedtime) values (?, ?, ?)',
                                (sURL, pluginName, failTime))
                    cur.execute('delete from pending_urls where url=? and plugin_name=?', (sURL, pluginName))
                    sqlCon.commit()
        except Exception as e:
            logger.error("Error while adding URL list to pending table: %s", e)
        finally:
            if sqlCon:
                sqlCon.close()
            self.dbAccessSemaphore.release()
            logger.debug("Completed adding URL list to pending table in db: Released exclusive db access")

    def writeQueueToDB(self):
        """ write newly retrieved URLs to file
        """
        sqlCon = None
        writeCount = 0
        if not self.completedQueue.empty():
            try:
                logger.debug("Save newly retrieved URLs to history db: Waiting to get db exclusive access...")
                acqResult = self.dbAccessSemaphore.acquire(timeout=30)
                if acqResult is True:
                    logger.debug("Save newly retrieved URLs to history db: Got exclusive db access")
                    sqlCon = URLListHelper.openConnFromfile(self.dbFileName)
                    cur = sqlCon.cursor()
                    while not self.completedQueue.empty():
                        # get all completed urls from queue: When trying to retrieve data the exception was
                        resultObj = self.completedQueue.get()
                        self.completedQueue.task_done()
                        # write each item to table:
                        cur.execute(
                                'insert into URL_LIST (url, plugin, pubdate, rawsize, datasize) VALUES(?, ?, ?, ?, ?)',
                                resultObj.getAsTuple()
                                )
                        writeCount = writeCount + 1
                        cur.execute('delete from pending_urls where url=\'' + resultObj.URL +
                                    '\' and plugin_name=\'' + resultObj.pluginName + '\' ')
                    sqlCon.commit()
                    # delete from pending where url exists in completed table:
                    cur.execute('delete from pending_urls where url in (select url_list.url from url_list' +
                                ' inner join pending_urls on pending_urls.plugin_name=url_list.plugin and' +
                                ' url_list.url=pending_urls.url)')
                    sqlCon.commit()
                    # read back total count of URLs in history table:
                    cur.execute('SELECT count(*) from URL_LIST')
                    data = cur.fetchone()
                    logger.debug("Till date, %s URLs were retrieved.", data[0])
            except Exception as e:
                logger.error("Error while saving newly retrieved URLs to history table: %s", e)
            finally:
                if sqlCon:
                    sqlCon.close()
                self.dbAccessSemaphore.release()
                logger.debug("Save newly retrieved URLs to history db: Released exclusive db access")
        else:
            logger.debug("No URLs saved to history db at this moment.")
        return(writeCount)

##########


class ExecutionResult():
    """ Object that encapsulates the result of data retrieval
    """
    URL = None
    rawDataFileName = None
    savedDataFileName = None
    rawDataSize = 0
    textSize = 0
    publishDate = None
    pluginName = None

    def __init__(self, sURL, htmlContentLen, textContentLen, publishDate, pluginName,
                 dataFileName=None, rawDataFile=None):
        self.URL = sURL
        self.rawDataFileName = rawDataFile
        self.savedDataFileName = dataFileName
        self.rawDataSize = htmlContentLen
        self.textSize = textContentLen
        self.publishDate = publishDate
        self.pluginName = pluginName

    def getAsTuple(self):
        return((self.URL, self.pluginName, self.publishDate, self.rawDataSize, self.textSize))

##########


class NewsArticle(JSONEncoder):
    """ article data structure and object """

    urlData = dict()
    triggerWordFlags = dict()
    uniqueID = ""
    html = None

    def getPublishDate(self):
        return(self.urlData["pubdate"])

    def getURL(self):
        return(self.urlData["URL"])

    def getModuleName(self):
        """ get the name of the module that generated this news item"""
        return(self.urlData["module"])

    def getHTML(self):
        return(self.html)

    def getBase64FromHTML(articleHTMLText):
        """ Get Base64 text data From HTML text
        """
        htmlBase64 = ""
        try:
            encodedBytes = base64.b64encode(articleHTMLText.encode("utf-8"))
            htmlBase64 = str(encodedBytes, "ascii")

        except Exception as e:
            logger.error("Error setting html content: %s", e)

        return(htmlBase64)

    def getHTMLFromBase64(htmlBase64):
        """ get HTML text From Base64 data
        """
        decoded_bytes = ""
        try:
            base64_bytes = htmlBase64.encode('ascii')
            decoded_bytes = base64.b64decode(base64_bytes)
        except Exception as e:
            logger.error("Error setting html content: %s", e)
        return(decoded_bytes.decode('utf-8'))

    def getText(self):
        textContent = ""
        if "text" in self.urlData.keys():
            textContent = self.urlData["text"]
        else:
            logger.error("Article does not have any text field")
        return(textContent)

    def getTextSize(self):
        textSize = 0
        try:
            textSize = len(self.getText())
        except Exception as e:
            logger.error("Error getting text size of article: %s", e)
        return(textSize)

    def getHTMLSize(self):
        htmlSize = 0
        try:
            htmlSize = len(self.html)
        except Exception as e:
            logger.error("Error getting html size of article: %s", e)
        return(htmlSize)

    def getAuthors(self):
        return(self.urlData["sourceName"])

    def getTriggerWords(self):
        return(self.triggerWordFlags)

    def getKeywords(self):
        return(self.urlData["keywords"])

    def getArticleID(self):
        return(self.urlData["uniqueID"])

    def getTextEmbedding(self):
        return(self.nlpDoc)

    def getFileName(self):
        return(self.fileName)

    def setTextEmbedding(self, nlpDoc):
        self.nlpDoc = nlpDoc

    def setHTML(self, htmlContent):
        self.html = htmlContent

    def setFileName(self, fileName):
        self.fileName = fileName

    def setPublishDate(self, publishDate):
        """ set the Publish Date of article """
        try:
            self.urlData["pubdate"] = str(publishDate.strftime("%Y-%m-%d"))
        except Exception as e:
            logger.error("Error setting publish date of article: %s", e)
            self.urlData["pubdate"] = str(datetime.now().strftime("%Y-%m-%d"))

    def setModuleName(self, moduleName):
        """ Set the name of the module that generated this news item"""
        self.urlData["module"] = moduleName

    def setTriggerWordFlag(self, triggerKey, triggerFlag):
        """ Add trigger word flag value for given article"""
        self.triggerWordFlags[triggerKey] = triggerFlag

    def identifyTriggerWordFlags(self, configur):
        """ Identify Trigger Word Flags, read from config file """
        if 'triggerwords' in configur.sections():
            section = configur['triggerwords']
            if section.name == 'triggerwords':
                for key, item in section.items():
                    matchPat = re.compile(str(item).strip())
                    regMatchRes = matchPat.search(self.getText().lower())
                    if regMatchRes is not None:
                        self.setTriggerWordFlag(key, 1)
                    else:
                        self.setTriggerWordFlag(key, 0)
        self.urlData["triggerwords"] = self.triggerWordFlags

    def setTitle(self, articleTitle):
        """ Set the title """
        self.urlData["title"] = str(articleTitle)

    def setKeyWords(self, articleKeyWordsList):
        """ set the keywords in the article
        """
        resultList = []
        try:
            for keyword in articleKeyWordsList:
                # clean words, trim whitespace:
                resultList.append(NewsArticle.cleanText(keyword))
            # de-duplicate the list
            resultList = deDupeList(resultList)
        except Exception as e:
            logger.error("Error cleaning keywords for article: %s", e)
        self.urlData["keywords"] = resultList

    def setText(self, articleText):
        self.urlData["text"] = NewsArticle.cleanText(articleText)

    def setIndustries(self, articleIndustryList):
        self.urlData["industries"] = articleIndustryList

    def setCategory(self, articleCategory):
        self.urlData["category"] = str(articleCategory)

    def setURL(self, sURL):
        self.urlData["URL"] = str(sURL)

    def setArticleID(self, uniqueID):
        self.uniqueID = uniqueID
        self.urlData["uniqueID"] = str(uniqueID)

    def setSource(self, sourceName):
        self.urlData["sourceName"] = str(sourceName)

    def toJSON(self):
        """ Converts python object into json.
        See reference page at - https://docs.python.org/3/library/json.html
        """
        return json.dumps(self.urlData)

    def readFromJSON(self, jsonFileName):
        """ read from JSON file into object
        """
        # logger.debug("Reading JSON file to load previously saved article %s", jsonFileName)
        try:
            with open(jsonFileName, 'r', encoding='utf-8') as fp:
                self.urlData = json.load(fp)
                fp.close()
        except Exception as theError:
            logger.error("Exception caught reading data from JSON file: %s", theError)

    def cleanText(textInput):
        """ Clean text, e.g. replace unicode characters, etc.
        """
        cleanText = textInput
        if len(textInput) > 1:
            try:
                # replace special characters:
                replaceWithSpaces = ['\u0915', '\u092f', '\u0938', '\u091a', '\u0941', '\u093e', '\u0906', '\u092f', '\u094b',
                                     '\u092c', '\u093e', '\u092c', '\u093e', '\u0902', '\u0917', '\u0925', '\u092e', '\u092e',
                                     '\u0930', '\u0908', '\u0926', '\u0932', '\u0905', '\u092d', '\u0923', '\u0902', '\u0924',
                                     '\u0938', '\u092f', '\u093e', '\u092a', '\u0924', '\u0909', '\u092a' '\u091c', '\u0940']
                for uniCodeChar in replaceWithSpaces:
                    cleanText = cleanText.replace(uniCodeChar, " ")
                cleanText = cleanText.replace(" Addl. ", " Additional ")
                cleanText = cleanText.replace("M/s.", "Messers")
                cleanText = cleanText.replace("m/s.", "Messers")
                cleanText = cleanText.replace(' Rs.', ' Rupees ')
                cleanText = cleanText.replace('₹', ' Rupees ')
                cleanText = cleanText.replace('$', ' Dollars ')
                cleanText = cleanText.replace("\t", " ")
                cleanText = cleanText.replace('—', "-")
                cleanText = cleanText.replace("\u2014", "-")
                cleanText = cleanText.replace('–', "-")
                cleanText = cleanText.replace("\u2013", "-")
                cleanText = cleanText.replace('’', "'")
                cleanText = cleanText.replace("\u2019", "'")
                cleanText = cleanText.replace('‘', "'")
                cleanText = cleanText.replace("\u2018", "'")
                cleanText = cleanText.replace('”', "'")
                cleanText = cleanText.replace("\u201d", "'")
                cleanText = cleanText.replace('“', "'")
                cleanText = cleanText.replace("\u201c", "'")
                cleanText = cleanText.replace('​', "'")  # yes, there is a special character here.
                cleanText = cleanText.replace("\u200b", " ")
                cleanText = cleanText.replace('🙂', " ")
                cleanText = cleanText.replace("\U0001f642", " ")
                cleanText = cleanText.replace("\x93", " ")
                cleanText = cleanText.replace("\x94", " ")
                # remove non utf-8 characters
                cleanText = textInput.encode('utf-8', errors="replace").decode('utf-8', errors='ignore').strip()
                cleanText = fixSentenceGaps(cleanText)
            except Exception as e:
                logger.error("Error cleaning text: %s", e)
        return(cleanText)

    def writeFiles(self, fileNameWithOutExt, htmlContent, saveHTMLFile=False):
        """ write output To JSON and/or html file
        """
        fullHTMLPathName = ""
        fullPathName = ""
        jsonContent = ""
        try:
            parentDirName = os.path.dirname(fileNameWithOutExt)
            # first check if directory of given date exists, or not
            if os.path.isdir(parentDirName) is False:
                # dir does not exist, so try creating it:
                os.mkdir(parentDirName)
        except Exception as theError:
            logger.error("Exception when saving article creating parent directory %s: %s", parentDirName, theError)
        try:
            if saveHTMLFile is True:
                fullHTMLPathName = fileNameWithOutExt + ".html.bz2"
                with bz2.open(fullHTMLPathName, "wb") as fpt:
                    # Write compressed data to file
                    fpt.write(htmlContent.encode("utf-8"))
                    fpt.close()
        except Exception as theError:
            logger.error("Exception caught writing data to html file %s: %s", fullHTMLPathName, theError)
        try:
            jsonContent = self.toJSON()
            fullPathName = fileNameWithOutExt + ".json"
            with open(fullPathName, 'wt', encoding='utf-8') as fp:
                fp.write(jsonContent)
                fp.close()
                logger.debug('Saved article as json file: %s', fullPathName)
        except Exception as theError:
            logger.error("Exception caught saving data to json file %s: %s", fullPathName, theError)
            # throw the exception back to calling routines:
            raise theError

    def importNewspaperArticleData(self, newspaperArticle):
        """ Import Data from newspaper library's Article class
        """
        try:
            # check and get authors/sources
            if len(newspaperArticle.authors) > 0:
                self.setSource(newspaperArticle.authors[0])
            else:
                self.setSource("")
            # set publishDate as current date time if article's publish_date is null
            if newspaperArticle.publish_date is None or newspaperArticle.publish_date == '':
                self.setPublishDate(datetime.now())
            else:
                self.setPublishDate(newspaperArticle.publish_date)
            self.setText(newspaperArticle.text)
            self.setTitle(newspaperArticle.title)
            self.setURL(newspaperArticle.url)
            self.setHTML(newspaperArticle.html)
            allKeywords = []
            if type(newspaperArticle.keywords).__name__ == 'list':
                allKeywords = allKeywords + newspaperArticle.keywords
            if type(newspaperArticle.meta_data['keywords']).__name__ == 'str':
                allKeywords = allKeywords + newspaperArticle.meta_data['keywords'].split(',')
            if type(newspaperArticle.meta_data['news_keywords']).__name__ == 'str':
                allKeywords = allKeywords + newspaperArticle.meta_data['news_keywords'].split(',')
            self.setKeyWords(allKeywords)
        except Exception as theError:
            logger.error("Exception caught importing newspaper article: %s", theError)

# # end of file ##
