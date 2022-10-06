#!/usr/bin/env python
# -*- coding: utf-8 -*-

# #########################################################################################################
# File name: session_hist.py                                                                              #
# Application: The NewsLookout Web Scraping Application                                                   #
# Date: 2021-06-23                                                                                        #
# Purpose: Session History Database Class that records session history for the web scraper                #
# Copyright 2021, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu, sandeep.sandhu@gmx.com  #
#                                                                                                         #
# Provides:                                                                                               #
#    SessionHistory                                                                                        #
#                                                                                                         #
#                                                                                                         #
# Notice:                                                                                                 #
# This software is intended for demonstration and educational purposes only. This software is             #
# experimental and a work in progress. Under no circumstances should these files be used in               #
# relation to any critical system(s). Use of these files is at your own risk.                             #
#                                                                                                         #
# Before using it for web scraping any website, always consult that website's terms of use.               #
# Do not use this software to fetch any data from any website that has forbidden use of web               #
# scraping or similar mechanisms, or violates its terms of use in any other way. The author is            #
# not liable for such kind of inappropriate use of this software.                                         #
#                                                                                                         #
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,                     #
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR                #
# PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE               #
# FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR                    #
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER                  #
# DEALINGS IN THE SOFTWARE.                                                                               #
#                                                                                                         #
# #########################################################################################################


# import standard python libraries:
import datetime
import logging
import os
import sqlite3 as lite
import threading

# import internal libraries
import data_structs
from scraper_utils import deDupeList


##########

# setup logging
logger = logging.getLogger(__name__)

##########


class SessionHistory:
    """ Utility class that saves and retrieves completed URLs
    Uses semaphores extensively to avoid database lock and access problems
    when writing/reading data in this multi-threaded application
    """
    ddl_url_table = str('create table if not exists URL_LIST' +
                        '(url TEXT, plugin varchar(100), pubdate DATE, rawsize long, datasize long)')
    ddl_pending_urls_table = str('create table if not exists pending_urls (url varchar(255) NOT NULL PRIMARY KEY,' +
                                 ' plugin_name varchar(100), attempts integer)')
    # NOTE: The TIMESTAMP field accepts a string in ISO 8601 format 'YYYY-MM-DD HH:MM:SS.mmmmmm'
    # or datetime.datetime object:
    ddl_failed_urls_table = str('create table if not exists FAILED_URLS' +
                                '(url TEXT, plugin_name varchar(100), failedtime timestamp)')
    ddl_deleted_dups_table = str('create table if not exists deleted_duplicates' +
                                 '(url TEXT, plugin varchar(100), pubdate DATE, filename TEXT)')
    db_connect_timeout = 180

    def __init__(self,
                 dataFileName: str,
                 dbAccessSemaphore: threading.Semaphore):
        """
         Initialize the history tracking and persistence object

        :param dataFileName:
        :param dbAccessSemaphore:
        """
        self.dbFileName = dataFileName
        self.dbAccessSemaphore = dbAccessSemaphore
        super().__init__()

    @staticmethod
    def openConnFromfile(dataFileName: str) -> lite.Connection:
        """ Read SQLite database file and get its connection.
        This db stores previously saved URLs.
        """
        # intentionally do not catch any errors, let these bubble up to the calling function:
        logger.debug("Trying to open connection to history URLs sqlite DB file '%s'", dataFileName)
        sqlCon = lite.connect(dataFileName,
                              timeout=SessionHistory.db_connect_timeout,
                              detect_types=lite.PARSE_DECLTYPES | lite.PARSE_COLNAMES)
        cur = sqlCon.cursor()
        # identify if tables are missing, if so create these:
        cur.execute(SessionHistory.ddl_url_table)
        cur.execute(SessionHistory.ddl_pending_urls_table)
        cur.execute(SessionHistory.ddl_failed_urls_table)
        cur.execute(SessionHistory.ddl_deleted_dups_table)
        sqlCon.commit()
        return sqlCon

    def printDBStats(self) -> tuple:
        """ Print SQLite database statistics and the SQLite version number.
        """
        sqlCon = None
        try:
            logger.debug("Print SQLite stats: Waiting for db exclusive access...")
            acqResult = self.dbAccessSemaphore.acquire()
            if acqResult is True:
                logger.debug("Print SQLite stats: Got exclusive db access")
                sqlCon = SessionHistory.openConnFromfile(self.dbFileName)
                cur = sqlCon.cursor()
                cur.execute('SELECT SQLITE_VERSION()')
                data = cur.fetchone()
                SQLiteVersion = data[0]
                cur.execute('SELECT count(*) from URL_LIST')
                data = cur.fetchone()
                logger.info("Total count of URLs retrieved = %s, History Dataset SQLite version: %s",
                            data[0],
                            SQLiteVersion)
                return (data[0], SQLiteVersion)
        except Exception as e:
            logger.error(f"While showing previously retrieved URLs stats: {e}")
        finally:
            if sqlCon:
                sqlCon.close()
            self.dbAccessSemaphore.release()
            logger.debug("Print SQLite stats: Released exclusive db access")

    def url_was_attempted(self, sURL: str, pluginName: str) -> bool:
        """Check after querying the session history database, whether the url was attempted or not.

        :param sURL: URL to query.
        :param pluginName: (Optional) Name of the plugin to search database for.
        :return: True if URL was attempted earlier.
        """
        # TODO: change method to check multiple URLs
        searchResult = False
        sqlCon = None
        try:
            logger.debug("Search already fetched URLs: Waiting for db exclusive access...")
            acqResult = self.dbAccessSemaphore.acquire()
            if sURL is not None and acqResult is True:
                logger.debug("Search already fetched URLs: Got exclusive db access")
                sqlCon = SessionHistory.openConnFromfile(self.dbFileName)
                cur = sqlCon.cursor()
                result = cur.execute('select url from URL_LIST where url = ? union all ' +
                                     'select url from FAILED_URLS where url = ?',
                                     (sURL, sURL))
                rowset = result.fetchall()
                if rowset is not None and len(rowset) > 0 and rowset[0][0] == sURL:
                    searchResult = True
                    logger.debug(f'URL {rowset[0][0]} already fetched or failed, so not fetching')
                else:
                    searchResult = False
        except Exception as e:
            logger.error(f"{pluginName}: While searching url in already fetched or failed table: {e}")
        finally:
            if sqlCon:
                sqlCon.close()
            self.dbAccessSemaphore.release()
            logger.debug(f"Search already fetched URLs for plugin {pluginName}: Released exclusive db access")
        return searchResult

    def removeAlreadyFetchedURLs(self, newURLsList: list, pluginName: str) -> list:
        """ Remove already fetched URLs from given list by searching history database
        """
        filteredList = []
        sqlCon = None
        try:
            logger.debug("Remove already fetched URLs: Waiting for db exclusive access...")
            acqResult = self.dbAccessSemaphore.acquire()
            if acqResult is True:
                logger.debug("Remove already fetched URLs: Got exclusive db access")
                sqlCon = SessionHistory.openConnFromfile(self.dbFileName)
                cur = sqlCon.cursor()
                if newURLsList is not None:
                    # TODO: parallelize this to search entire list in one iteration, use temp tables and SQL
                    for listItem in newURLsList:
                        result = cur.execute('select url, pubdate from URL_LIST where url = ? union all ' +
                                             'select url, NULL from FAILED_URLS where url = ?',
                                             (listItem, listItem))
                        rowset = result.fetchall()
                        logger.debug(f'{pluginName}: Checking item "{listItem}", got sql result = {rowset}')
                        if rowset is not None and len(rowset) > 0 and rowset[0][0] == listItem:
                            rowset = None
                        else:
                            filteredList.append(listItem)
        except Exception as e:
            logger.error("%s: While removing already fetched URLs: %s", pluginName, e)
        finally:
            if sqlCon:
                sqlCon.close()
            self.dbAccessSemaphore.release()
            logger.debug("Removed already fetched URLs for plugin %s: Released exclusive db access", pluginName)
        return filteredList

    def retrieveTodoURLList(self, pluginName: str) -> list:
        """ Retrieve URL list from the pending_urls table for the given plugin name

        :param pluginName: The name of the plugin for which pending URLs need to be listed
        :return: List of pending URLs
        """
        URLsFromSQLite = []
        sqlCon = None
        try:
            logger.debug("Fetching pending url list: Waiting for db exclusive access...")
            acqResult = self.dbAccessSemaphore.acquire()
            if acqResult is True:
                sqlQuery = "select distinct url from pending_urls where plugin_name='" + pluginName +\
                           "' and url not in (select url from failed_urls) and url not in (select url from url_list)"
                sqlCon = SessionHistory.openConnFromfile(self.dbFileName)
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
        URLsFromSQLite = deDupeList(URLsFromSQLite)
        logger.info(f'{pluginName}: Identified {len(URLsFromSQLite)} URLs from pending table of history database.')
        return URLsFromSQLite

    def addURLsToPendingTable(self, urlList: list, pluginName: str):
        """ Add newly identified URLs to the pending Table.

        Check duplicates using SQL:
        select count(*), url from pending_urls group by url having count(*)>1

        :param urlList: List of URLs to be added
        :param pluginName: Name of the plugin for which the URLs are to be added.
        :return:
        """
        sqlCon = None
        try:
            logger.debug("Add URL list to pending table in db: Waiting to get db exclusive access...")
            acqResult = self.dbAccessSemaphore.acquire(timeout=30)
            if acqResult is True:
                logger.debug("Adding URL list to pending table for plugin %s: Got exclusive db access.", pluginName)
                sqlCon = SessionHistory.openConnFromfile(self.dbFileName)
                cur = sqlCon.cursor()
                urlList = deDupeList(urlList)
                # TODO: parallelize this to process entire list in one iteration
                for sURL in urlList:
                    # Ideally, if url already exists, the no. of attempts should be incremented by 1, but it is omitted
                    cur.execute('insert or ignore into pending_urls (url, plugin_name, attempts) values (?, ?, 1)',
                                (sURL, pluginName))
                sqlCon.commit()
        except Exception as e:
            logger.error(f"Error while adding URL list to pending table: {e}")
        finally:
            if sqlCon:
                sqlCon.close()
            self.dbAccessSemaphore.release()
            logger.debug(f"Completed adding URL list to pending table for plugin {pluginName}:" +
                         " Released exclusive db access.")

    def addURLToFailedTable(self,
                            fetchResult: data_structs.ExecutionResult,
                            pluginName: str,
                            failTime: datetime.datetime):
        """ Add URL to failed URLs table.
        To avoid duplicates, add a URL only if it does not already exist in table.

        :param fetchResult: ExecutionResult object from the attempt to fetch this URL
        :param pluginName: Name of the plugin which was attempting to fetch this URL
        :param failTime: Date-time when the URL retrieval failed
        """
        sqlCon = None
        try:
            sURL = fetchResult.URL
            logger.debug("Add URL list to pending table in db: Waiting to get db exclusive access...")
            acqResult = self.dbAccessSemaphore.acquire(timeout=30)
            if acqResult is True:
                logger.debug("Add URL list to pending table in db: Got exclusive db access")
                sqlCon = SessionHistory.openConnFromfile(self.dbFileName)
                cur = sqlCon.cursor()
                # get count of url to check if it already exists:
                result = cur.execute('select count(*) from FAILED_URLS where url = ?', (sURL,))
                rowset = result.fetchall()
                # To avoid duplicates, add only if it does not exist:
                if rowset is not None and len(rowset) > 0 and rowset[0][0] == 0:
                    cur.execute('insert into FAILED_URLS (url, plugin_name, failedtime) values (?, ?, ?)',
                                (sURL, pluginName, failTime))
                    cur.execute('delete from pending_urls where url=? and plugin_name=?', (sURL, pluginName))
                    # delete from pending_urls where url is in failed_urls or in url_list
                    cur.execute('delete from pending_urls where url in ' +
                                '(select url from failed_urls) or url in (select url from url_list)')
                    sqlCon.commit()
        except Exception as e:
            logger.error("Error while adding URL list to pending table: %s", e)
        finally:
            if sqlCon:
                sqlCon.close()
            self.dbAccessSemaphore.release()
            logger.debug("Completed adding URL list to pending table in db: Released exclusive db access")

    def writeQueueToDB(self, results_from_queue: list) -> int:
        """ Write successfully retrieved URLs to database table - URL_LIST.
        :param results_from_queue: List of ExecutionResult objects retrieved from the completed queue.
        :return: Count of URLs saved to the database table.
        """
        sqlCon = None
        writeCount = 0
        resultObj = None
        try:
            logger.debug("Save newly retrieved URLs to history db: Waiting to get db exclusive access...")
            acqResult = self.dbAccessSemaphore.acquire(timeout=30)
            if acqResult is True:
                logger.debug("Save newly retrieved URLs to history db: Got exclusive db access")
                sqlCon = SessionHistory.openConnFromfile(self.dbFileName)
                cur = sqlCon.cursor()
                # TODO: Parallelize this to process entire list in one iteration
                for resultObj in results_from_queue:
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
                logger.debug(f"Till date, {data[0]} URLs were retrieved.")
        except Exception as e:
            logger.error(f"Error while saving newly retrieved URLs to history table: {e}, resultObj = {resultObj}")
        finally:
            if sqlCon:
                sqlCon.close()
            self.dbAccessSemaphore.release()
            logger.debug("Save newly retrieved URLs to history db: Released exclusive db access")
        return writeCount

    def addDupURLToDeleteTbl(self, sURL: str, pluginName: str, pubdate: datetime.datetime, filename: str):
        """ Add duplicate URLs To deleted table. Query this using the statement:

        `select url, plugin, pubdate, filename from deleted_duplicates;`

        :param sURL: URL to mark for deletion.
        :param pluginName: Name of the plugin.
        :param pubdate: Date when the article was published
        :param filename: Name of the file that was deleted
        """
        sqlCon = None
        try:
            logger.debug("Add URL to deleted table: Waiting to get db exclusive access...")
            acqResult = self.dbAccessSemaphore.acquire(timeout=30)
            if acqResult is True:
                logger.debug("Adding URL to deleted table for plugin %s: Got exclusive db access.", pluginName)
                sqlCon = SessionHistory.openConnFromfile(self.dbFileName)
                cur = sqlCon.cursor()
                filename = os.path.basename(filename)
                cur.execute('insert into deleted_duplicates (url, plugin, pubdate, filename)' +
                            ' values (?, ?, ?, ?)',
                            (sURL, pluginName, pubdate, filename))
                sqlCon.commit()
        except Exception as e:
            logger.error("Error while adding URL to deleted table: %s", e)
        finally:
            if sqlCon:
                sqlCon.close()
            self.dbAccessSemaphore.release()
            logger.debug("Completed adding URL to deleted table for plugin %s: Released exclusive db access.",
                         pluginName)

##########
