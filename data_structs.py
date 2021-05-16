#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: data_structs.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-01-14
 Purpose: Helper class with data structures supporting the web scraper
 Copyright 2021, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu, sandeep.sandhu@gmx.com

Provides:
    Types
    URLListHelper
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
import threading
import json
import queue
from json import JSONEncoder
from collections import OrderedDict
import bz2
import base64
import sqlite3 as lite

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

##########

# objects/data structures for URL lists, and news article data


class ScrapeError(Exception):
    pass


class URLListHelper(JSONEncoder):
    """ saves and retrieves completed URLs """

    def __init__(self, dataFileName):
        """ Initialize the object """
        self.dbFileName = dataFileName
        self.dbAccessSemaphore = threading.Semaphore()
        self.completedQueue = queue.Queue()

    def default(self, o):
        """ default """
        return o.__dict__

    def deDupeList(listWithDuplicates):
        """ Dedupe a given List by converting into a dict
        , and then re-converting to a list back again.
        """
        dedupedList = listWithDuplicates

        if type(listWithDuplicates).__name__ == 'list':
            dedupedList = list(
                 OrderedDict.fromkeys(
                     listWithDuplicates
                    )
                )

        return(dedupedList)

    def openConnFromfile(dataFileName):
        """ Read SQLite database file and get its connection.
        This db stores previously saved URLs.
        """
        sqlCon = None

        try:
            logger.debug("Trying to open connection to history URLs sqlite DB file '%s'", dataFileName)

            sqlCon = lite.connect(dataFileName)

            cur = sqlCon.cursor()

            if os.path.isfile(dataFileName):
                cur.execute('delete from NEW_URL_LIST')
                sqlCon.commit()
            else:
                # create a new file with empty table
                cur.execute('CREATE TABLE URL_LIST(url TEXT, plugin varchar(100), pubdate DATE, rawsize long, datasize long)')
                cur.execute('CREATE TABLE NEW_URL_LIST(url TEXT)')
                cur.execute('SELECT SQLITE_VERSION()')
                data = cur.fetchone()
                logger.info("Created new SQLite database to store history of URLs, SQLite version: %s", data[0])

        except lite.Error as e:
            logger.error("SQLite database error connecting to previous saved URLs db: %s", e)

        except Exception as e:
            logger.error("Error connecting to previous saved URLs db: %s", e)

        return(sqlCon)

    def printDBStats(self):
        """ print SQLite database Stats and version no.
        """
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
                    "Total count of URLs retrieved = %s, History dataset SQLite version: %s", data[0], SQLiteVersion)

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
        try:
            logger.debug("Remove already fetched URLs: Waiting for db exclusive access...")
            acqResult = self.dbAccessSemaphore.acquire()
            if acqResult is True:
                logger.debug("Remove already fetched URLs: Got exclusive db access")
                sqlCon = URLListHelper.openConnFromfile(self.dbFileName)
                cur = sqlCon.cursor()
                if newURLsList is not None:
                    for listItem in newURLsList:
                        cur.execute('select url from URL_LIST where url = "' + listItem + '"')
                        data = cur.fetchone()
                        if data is not None and len(data) > 0 and data[0] == listItem:
                            data = None
                            # logger.debug("Already fetched earlier, so removing URL: %s", data[0])
                        else:
                            filteredList.append(listItem)
        except Exception as e:
            logger.error("%s: While removing already fetched URLs: %s", pluginName, e)
        finally:
            if sqlCon:
                sqlCon.close()
            self.dbAccessSemaphore.release()
            logger.debug("Remove already fetched URLs: Released exclusive db access")
        return(filteredList)

    def writeQueueToDB(self):
        """ write newly retrieved URLs to file
        """
        # print('Queue size: ', self.completedQueue.qsize(), "Empty?", self.completedQueue.empty())
        writeCount = 0
        if not self.completedQueue.empty():
            try:
                logger.debug("Save newly retrieved URLs to history db: Waiting to get db exclusive access...")
                acqResult = self.dbAccessSemaphore.acquire(timeout=30)

                if acqResult is True:
                    logger.debug("Save newly retrieved URLs to history db: Got exclusive db access")
                    sqlCon = URLListHelper.openConnFromfile(self.dbFileName)
                    cur = sqlCon.cursor()

                    # get all completed urls from queue:
                    while not self.completedQueue.empty():
                        # write each item to table:
                        (sURL, rawSize, dataSize, pubdate, pluginName) = self.completedQueue.get()
                        self.completedQueue.task_done()
                        # cur.execute('insert into URL_LIST VALUES("' + sURL + '")')
                        cur.execute(
                                'insert into URL_LIST (url, plugin, pubdate, rawsize, datasize) VALUES("'
                                + sURL + '", "'
                                + pluginName + '", "'
                                + pubdate + '", '
                                + str(rawSize) + ', '
                                + str(dataSize) + ')')
                        writeCount = writeCount + 1

                    cur.execute('delete from NEW_URL_LIST')
                    sqlCon.commit()

                    # read back total URLs in history:
                    cur.execute('SELECT count(*) from URL_LIST')
                    data = cur.fetchone()
                    logger.debug("Till date, %s URLs were retrieved.", data[0])

            except Exception as e:
                logger.error("While saving newly retrieved URLs to history table: %s", e)
            finally:
                if sqlCon:
                    sqlCon.close()
                self.dbAccessSemaphore.release()
                logger.debug("Save newly retrieved URLs to history db: Released exclusive db access")
        else:
            logger.debug("No URLs saved to history db at this moment.")

        return(writeCount)

##########


class NewsArticle(JSONEncoder):
    """ article data structure and object """

    urlData = dict()
    uniqueID = ""
    html = None

    def getPublishDate(self):
        return(self.urlData["pubdate"])

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

    def getAuthors(self):
        return(self.urlData["sourceName"])

    def getKeywords(self):
        return(self.urlData["keywords"])

    def setHTML(self, htmlContent):
        self.html = htmlContent

    def setPublishDate(self, publishDate):
        """ set Publish Date of article """
        try:
            self.urlData["pubdate"] = str(publishDate.strftime("%Y-%m-%d"))

        except Exception as e:
            logger.error("Error setting publish date of article: %s", e)
            self.urlData["pubdate"] = str(datetime.now().strftime("%Y-%m-%d"))

    def setModuleName(self, moduleName):
        self.urlData["module"] = moduleName

    def setTitle(self, articleTitle):
        self.urlData["title"] = str(articleTitle)

    def setKeyWords(self, articleKeyWordsList):
        """ set the keywords in the article
        """
        resultList = []
        try:
            for keyword in articleKeyWordsList:
                # clean words, trim whitespace:
                resultList.append(NewsArticle.cleanDirtyText(keyword))
            # de-duplicate the list
            resultList = URLListHelper.deDupeList(resultList)
        except Exception as e:
            logger.error("Error cleaning keywords for article: %s", e)

        self.urlData["keywords"] = resultList

    def setText(self, articleText):
        self.urlData["text"] = self.cleanText(articleText)

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
        logger.debug("Reading JSON file to load previously saved article %s", jsonFileName)
        try:
            with open(jsonFileName, 'r', encoding='utf-8') as fp:
                self.urlData = json.load(fp)
                fp.close()
        except Exception as theError:
            logger.error("Exception caught reading JSON file: %s", theError)

    def cleanDirtyText(dirtyText):
        """ Clean Dirty Text
        """
        cleanText = ""
        dirtyText = dirtyText.strip()
        if len(dirtyText) > 0:
            try:
                cleanText = dirtyText.encode("ascii", "ignore").decode().strip()
            except Exception as e:
                logger.error("Error cleaning text: %s", e)
        return(cleanText)

    def cleanText(self, textInput):
        """ clean text, e.g. replace unicode characters, etc.
        """
        if len(textInput) > 1:
            cleanText = textInput.replace("\u2014", "-")
            cleanText = cleanText.replace("\u2013", "-")
            cleanText = cleanText.replace("\n", " ")
            cleanText = cleanText.replace("\u2019", "'")
            cleanText = cleanText.replace("\u2018", "'")
            cleanText = cleanText.replace("\u201d", "'")
            cleanText = cleanText.replace("\u201c", "'")
            cleanText = cleanText.replace("\U0001f642", " ")
            cleanText = cleanText.replace("\u200b", " ")  # \u200b
            cleanText = cleanText.replace("\x93", " ")
            cleanText = cleanText.replace("\x94", " ")
            cleanText = cleanText.replace("''", "'")
            cleanText = NewsArticle.cleanDirtyText(cleanText)
        else:
            cleanText = textInput
        return(cleanText)

    def writeFiles(self, fileNameWithOutExt, baseDirName, htmlContent, saveHTMLFile=False):
        """ write output To JSON and/or html file
        """
        fullHTMLPathName = ""
        fullPathName = ""
        jsonContent = ""
        dirPathName = ""
        try:
            dirPathName = os.path.join(baseDirName, self.urlData["pubdate"])
            # first check if directory of given date exists, or not
            if os.path.isdir(dirPathName) is False:
                # dir does not exist, so try creating it:
                os.mkdir(dirPathName)
        except Exception as theError:
            logger.error("Exception caught creating directory %s: %s", dirPathName, theError)
        try:
            if saveHTMLFile is True:
                fullHTMLPathName = os.path.join(dirPathName, fileNameWithOutExt + ".html.bz2")
                with bz2.open(fullHTMLPathName, "wb") as fpt:
                    # Write compressed data to file
                    fpt.write(htmlContent.encode("utf-8"))
                    fpt.close()
        except Exception as theError:
            logger.error("Exception caught writing data to html file %s: %s", fullHTMLPathName, theError)
        try:
            jsonContent = self.toJSON()
            fullPathName = os.path.join(dirPathName, fileNameWithOutExt + ".json")
            with open(fullPathName, 'wt', encoding='utf-8') as fp:
                fp.write(jsonContent)
                fp.close()
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
