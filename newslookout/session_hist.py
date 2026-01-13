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
import sys
import threading
from typing import List
import time
import functools

# import internal libraries
import data_structs
from scraper_utils import deDupeList


##########

# setup logging
logger = logging.getLogger(__name__)

##########

def retry_db_op(max_retries=5, initial_delay=1):
    """Decorator to retry database operations on lock errors."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except lite.OperationalError as e:
                    if "locked" in str(e):
                        last_exception = e
                        logger.warning(f"DB Locked. Retrying {func.__name__} in {delay}s (Attempt {attempt + 1}/{max_retries})")
                        time.sleep(delay)
                        delay *= 2  # Exponential backoff
                    else:
                        raise e
                except Exception as e:
                    raise e
            logger.error(f"Failed {func.__name__} after {max_retries} attempts. Last error: {last_exception}")
            raise last_exception
        return wrapper
    return decorator


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

    def __init__(self, dataFileName: str, dbAccessSemaphore: threading.Semaphore):
        """
         Initialize the history tracking and persistence object

        :param dataFileName:
        :param dbAccessSemaphore:
        """
        self.dbFileName = dataFileName
        self.dbAccessSemaphore = dbAccessSemaphore
        self._init_db_settings()
        super().__init__()

    def _init_db_settings(self):
        """Initialize DB with WAL mode for better concurrency"""
        try:
            with lite.connect(self.dbFileName) as con:
                con.execute('PRAGMA journal_mode=WAL;')
                con.execute('PRAGMA synchronous=NORMAL;')
        except Exception as e:
            logger.error(f"Failed to set WAL mode: {e}")
            sys.exit(1)

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
        searchResult = False
        sqlCon = None
        try:
            # Read operations don't strictly need the exclusive semaphore in WAL mode,
            # but we keep it for safety if the caller logic relies on serial access.
            sqlCon = lite.connect(self.dbFileName, timeout=self.db_connect_timeout)
            cur = sqlCon.cursor()
            result = cur.execute('select url from URL_LIST where url = ? union all ' +
                                 'select url from FAILED_URLS where url = ?',
                                 (sURL, sURL))
            rowset = result.fetchall()
            if rowset and len(rowset) > 0:
                searchResult = True
        except Exception as e:
            logger.error(f"{pluginName}: Error searching url: {e}")
        finally:
            if sqlCon:
                sqlCon.close()
        return searchResult


    def removeAlreadyFetchedURLs(self, newURLsList: List[str], pluginName: str) -> list:
        """
        Remove already fetched URLs from given list by searching history database

        :param newURLsList:
        :param pluginName:
        :return:
        """
        if not newURLsList:
            return []
        sqlCon = None
        try:
            # We use the semaphore here because we are creating a TEMP table
            self.dbAccessSemaphore.acquire()
            sqlCon = lite.connect(self.dbFileName, timeout=self.db_connect_timeout)
            cur = sqlCon.cursor()
            cur.execute("CREATE TEMP TABLE IF NOT EXISTS temp_urls (url TEXT PRIMARY KEY)")
            cur.execute("DELETE FROM temp_urls") # Clear previous run
            cur.executemany("INSERT OR IGNORE INTO temp_urls VALUES (?)", [(u,) for u in newURLsList])
            result = cur.execute("""
                SELECT url FROM temp_urls
                WHERE url NOT IN (SELECT url FROM URL_LIST)
                  AND url NOT IN (SELECT url FROM FAILED_URLS)
            """)
            return [row[0] for row in result.fetchall()]
        except Exception as e:
            logger.error(f"Error filtering URLs: {e}")
            return newURLsList # Return original list on failure
        finally:
            if sqlCon:
                sqlCon.close()
            self.dbAccessSemaphore.release()


    @retry_db_op()
    def retrieveTodoURLList(self, pluginName: str) -> list:
        """ Retrieve URL list from the pending_urls table for the given plugin name

        :param pluginName: The name of the plugin for which pending URLs need to be listed
        :return: List of pending URLs
        """
        URLsFromSQLite = []
        sqlCon = None
        try:
            self.dbAccessSemaphore.acquire()
            sqlQuery = "select distinct url from pending_urls where plugin_name=? " + \
                       "and url not in (select url from failed_urls) and url not in (select url from url_list)"
            sqlCon = SessionHistory.openConnFromfile(self.dbFileName)
            cur = sqlCon.cursor()
            cur.execute(sqlQuery, (pluginName,))
            allResults = cur.fetchall()
            for urlTuple in allResults:
                URLsFromSQLite.append(urlTuple[0])
        finally:
            if sqlCon:
                sqlCon.close()
            self.dbAccessSemaphore.release()
        URLsFromSQLite = deDupeList(URLsFromSQLite)
        logger.info(f'{pluginName}: Identified {len(URLsFromSQLite)} URLs from pending table of history database.')
        return URLsFromSQLite

    @retry_db_op()
    def addURLsToPendingTable(self, urlList: list, pluginName: str, num_attempts: int = 1):
        """ Add newly identified URLs to the pending Table.

        Check duplicates using SQL:
        select count(*), url from pending_urls group by url having count(*)>1

        :param urlList: List of URLs to be added
        :param pluginName: Name of the plugin for which the URLs are to be added.
        :return:
        """
        sqlCon = None
        try:
            self.dbAccessSemaphore.acquire()
            sqlCon = SessionHistory.openConnFromfile(self.dbFileName)
            cur = sqlCon.cursor()
            urlList = deDupeList(urlList)
            # Use executemany for performance
            data = [(sURL, pluginName, num_attempts) for sURL in urlList]
            cur.executemany('insert or ignore into pending_urls (url, plugin_name, attempts) values (?, ?, ?)', data)
            sqlCon.commit()
        except Exception as e:
            logger.error(f"Error adding to pending table: {e}")
            # Re-raise to trigger retry
            raise e
        finally:
            if sqlCon:
                sqlCon.close()
            self.dbAccessSemaphore.release()

    @retry_db_op()
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
            sURL = fetchResult if isinstance(fetchResult, str) else fetchResult.URL
            self.dbAccessSemaphore.acquire()
            sqlCon = SessionHistory.openConnFromfile(self.dbFileName)
            cur = sqlCon.cursor()

            cur.execute('INSERT INTO FAILED_URLS (url, plugin_name, failedtime) ' +
                        'SELECT ?, ?, ? WHERE NOT EXISTS (SELECT 1 FROM FAILED_URLS WHERE url = ?)',
                        (sURL, pluginName, failTime, sURL))

            cur.execute('delete from pending_urls where url=? and plugin_name=?', (sURL, pluginName))
            sqlCon.commit()
        except Exception as e:
            logger.error(f"Error adding to failed table: {e}")
            raise e
        finally:
            if sqlCon:
                sqlCon.close()
            self.dbAccessSemaphore.release()

    @retry_db_op()
    def writeQueueToDB(self, results_from_queue: list) -> int:
        """ Write successfully retrieved URLs to database table - URL_LIST.
        :param results_from_queue: List of ExecutionResult objects retrieved from the completed queue.
        :return: Count of URLs saved to the database table.
        """
        if not results_from_queue:
            return 0
        sqlCon = None
        writeCount = 0
        try:
            self.dbAccessSemaphore.acquire()
            sqlCon = SessionHistory.openConnFromfile(self.dbFileName)
            cur = sqlCon.cursor()

            # Prepare data for bulk insert
            insert_data = []
            urls_to_delete = []

            for resultObj in results_from_queue:
                insert_data.append(resultObj.getAsTuple())
                urls_to_delete.append((resultObj.URL, resultObj.pluginName))

            cur.executemany('insert into URL_LIST (url, plugin, pubdate, rawsize, datasize) VALUES(?, ?, ?, ?, ?)', insert_data)
            writeCount = len(insert_data)

            # Bulk delete from pending
            cur.executemany('delete from pending_urls where url=? and plugin_name=?', urls_to_delete)
            sqlCon.commit()

        except Exception as e:
            logger.error(f"Error saving history: {e}")
            raise e
        finally:
            if sqlCon:
                sqlCon.close()
            self.dbAccessSemaphore.release()
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
