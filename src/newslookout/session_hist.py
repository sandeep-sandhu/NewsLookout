# -*- coding: utf-8 -*-

# #########################################################################################################
# File name: session_hist.py                                                                              #
# Application: The NewsLookout Web Scraping Application                                                   #
# Date: 2021-06-23                                                                                        #
# Purpose: Session History Database Class that records session history for the web scraper                #
# Copyright 2021, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu, sandeep.sandhu@gmx.com  #
#                                                                                                         #
# Provides:                                                                                               #
#    SessionHistory                                                                                       #
# Session History - Enhanced with HTTP Error Tracking                                                     #
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


import logging
import sqlite3 as lite
import functools
import time
from datetime import datetime
from typing import List, Optional

from newslookout import scraper_utils
from newslookout.scraper_utils import deDupeList

logger = logging.getLogger(__name__)


def retry_db_op(max_retries=5, initial_delay=0.5):
    """
    Decorator to retry database operations on lock errors.

    Reduced delays for faster database access:
    - Initial delay: 0.1s (was 1s)
    - Exponential backoff: 0.1s, 0.2s, 0.4s, 0.8s, 1.6s
    - Max total wait: ~3.1s (was ~31s)

    Args:
        max_retries: Maximum number of retry attempts (default: 5)
        initial_delay: Initial delay in seconds (default: 0.1)
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exception = None

            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)

                except Exception as e:
                    # Check if it's a database locked error
                    if "locked" in str(e).lower():
                        last_exception = e

                        if attempt < max_retries - 1:
                            logger.debug(
                                f"DB locked. Retrying {func.__name__} in {delay:.3f}s "
                                f"(Attempt {attempt + 1}/{max_retries})"
                            )
                            time.sleep(delay)
                            delay *= 2  # Exponential backoff
                        else:
                            logger.warning(
                                f"DB locked. Failed {func.__name__} after {max_retries} attempts"
                            )
                    else:
                        # Not a lock error, re-raise immediately
                        raise e

            # All retries exhausted
            logger.error(
                f"Failed {func.__name__} after {max_retries} attempts. "
                f"Last error: {last_exception}"
            )
            raise last_exception

        return wrapper
    return decorator


class SessionHistory:
    """
    Utility class that saves and retrieves completed URLs and tracks HTTP errors.

    New Features:
    - Tracks HTTP errors (403, 404, 410, etc.) separately from failed URLs
    - Prevents retrying URLs that returned permanent errors
    - Stores error code and timestamp for analysis
    """

    # DDL for existing tables
    ddl_url_table = str('create table if not exists URL_LIST' +
                        '(url TEXT, plugin varchar(100), pubdate DATE, rawsize long, datasize long)')
    ddl_pending_urls_table = str('create table if not exists pending_urls (url varchar(255) NOT NULL PRIMARY KEY,' +
                                 ' plugin_name varchar(100), attempts integer)')
    ddl_failed_urls_table = str('create table if not exists FAILED_URLS' +
                                '(url TEXT, plugin_name varchar(100), failedtime timestamp)')
    ddl_deleted_dups_table = """
                             CREATE TABLE IF NOT EXISTS deleted_duplicates (
                                                                               url       TEXT,
                                                                               plugin    TEXT,
                                                                               pubdate   TEXT,
                                                                               filename  TEXT
                             ) \
                             """

    # NEW: Table for HTTP errors
    ddl_http_errors_table = str('create table if not exists HTTP_ERRORS' +
                                '(url TEXT, plugin_name varchar(100), http_code integer, ' +
                                'error_time timestamp, error_message TEXT, ' +
                                'PRIMARY KEY (url, plugin_name))')

    db_connect_timeout = 180

    def __init__(self, dataFileName: str, dbAccessSemaphore):
        """
        Initialize the history tracking and persistence object.

        Args:
            dataFileName (str): Path to SQLite database file
            dbAccessSemaphore: Threading semaphore for access control
        """
        self.dbFileName = dataFileName
        self.dbAccessSemaphore = dbAccessSemaphore
        self._init_db_settings()
        self.pending_urls = []
        logger.info("Getting all pending urls from database.")
        try:
            with lite.connect(self.dbFileName) as con:
                clearPendingURLsQuery = """
                                        DELETE from pending_urls where
                                            url in (SELECT url FROM failed_urls)
                                                                    or url in (SELECT url FROM url_list)
                                                                    or url in (SELECT url FROM HTTP_ERRORS) \
                                        """
                cur = con.cursor()
                cur.execute(clearPendingURLsQuery)
                con.commit()
                selectPendingURLs = "SELECT DISTINCT plugin_name, url from pending_urls"
                cur = con.cursor()
                result = cur.execute(selectPendingURLs)
                self.pending_urls = result.fetchall()
                con.commit()
                logger.info(f"{len(self.pending_urls)} Pending urls retrieved for all plugins.")
        except Exception as e:
            logger.error(f"Failed to get pending urls: {e}")
        super().__init__()

    def _init_db_settings(self):
        """Initialize DB with WAL mode and create tables."""
        try:
            with lite.connect(self.dbFileName) as con:
                con.execute('PRAGMA journal_mode=WAL;')
                con.execute('PRAGMA synchronous=NORMAL;')
                # Create all tables including new HTTP_ERRORS table
                cur = con.cursor()
                cur.execute(self.ddl_url_table)
                cur.execute(self.ddl_pending_urls_table)
                cur.execute(self.ddl_failed_urls_table)
                cur.execute(self.ddl_deleted_dups_table)
                cur.execute(self.ddl_http_errors_table)
                con.commit()
                # Add indexes for performance
                cur.execute('CREATE INDEX IF NOT EXISTS idx_url_list_url ON URL_LIST(url)')
                cur.execute('CREATE INDEX IF NOT EXISTS idx_pending_urls_url ON pending_urls(url)')
                cur.execute('CREATE INDEX IF NOT EXISTS idx_pending_urls_plugin ON pending_urls(plugin_name)')
                cur.execute('CREATE INDEX IF NOT EXISTS idx_failed_urls_url ON FAILED_URLS(url)')
                cur.execute('CREATE INDEX IF NOT EXISTS idx_http_errors_url ON HTTP_ERRORS(url)')
                con.commit()
                logger.info("Database initialized with HTTP error tracking tables and indexes")
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            # Do not raise or sys.exit here — the constructor must complete so
            # callers can still call openConnFromfile(), which will raise
            # sqlite3.DatabaseError itself when it probes the corrupt file.
            return

    @staticmethod
    def openConnFromfile(dataFileName: str) -> lite.Connection:
        """
        Open connection to SQLite database and ensure all tables exist.

        Raises sqlite3.DatabaseError if the file is not a valid SQLite database.

        Args:
            dataFileName (str): Path to database file

        Returns:
            sqlite3.Connection: Database connection
        """
        logger.debug("Opening connection to history URLs sqlite DB file '%s'", dataFileName)
        sqlCon = lite.connect(dataFileName,
                              timeout=SessionHistory.db_connect_timeout,
                              detect_types=lite.PARSE_DECLTYPES | lite.PARSE_COLNAMES)
        # Validate the file is a real SQLite database (raises DatabaseError for corrupt files)
        sqlCon.execute("SELECT name FROM sqlite_master LIMIT 1")

        # Ensure all tables exist — this is critical for :memory: connections, where
        # each new connection is a completely separate, empty database.
        cur = sqlCon.cursor()
        cur.execute(SessionHistory.ddl_url_table)
        cur.execute(SessionHistory.ddl_pending_urls_table)
        cur.execute(SessionHistory.ddl_failed_urls_table)
        cur.execute(SessionHistory.ddl_deleted_dups_table)
        cur.execute(SessionHistory.ddl_http_errors_table)
        sqlCon.commit()
        return sqlCon

    def printDBStats(self) -> tuple:
        """Print SQLite database statistics."""
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
                completed_count = cur.fetchone()[0]

                cur.execute('SELECT count(*) from HTTP_ERRORS')
                http_errors_count = cur.fetchone()[0]

                cur.execute('SELECT count(*) from FAILED_URLS')
                failed_count = cur.fetchone()[0]

                logger.info("Total URLs retrieved = %s, HTTP errors = %s, Failed = %s, SQLite version: %s",
                            completed_count, http_errors_count, failed_count, SQLiteVersion)
                return (completed_count, http_errors_count, failed_count, SQLiteVersion)
        except Exception as e:
            logger.error(f"While showing stats: {e}")
        finally:
            if sqlCon:
                sqlCon.close()
            self.dbAccessSemaphore.release()

    @retry_db_op()
    def addHTTPError(self, url: str, plugin_name: str, http_code: int,
                     error_message: str = None):
        """
        Add HTTP error to database.

        This method records URLs that returned HTTP errors like 403, 404, 410, etc.
        These URLs will not be retried in future runs.

        Args:
            url (str): URL that returned error
            plugin_name (str): Plugin attempting to fetch
            http_code (int): HTTP status code (403, 404, etc.)
            error_message (str, optional): Additional error details
        """
        sqlCon = None
        try:
            self.dbAccessSemaphore.acquire()
            sqlCon = SessionHistory.openConnFromfile(self.dbFileName)
            cur = sqlCon.cursor()

            error_time = datetime.now()

            # Use INSERT OR REPLACE to update if URL already exists
            cur.execute(
                'INSERT OR REPLACE INTO HTTP_ERRORS ' +
                '(url, plugin_name, http_code, error_time, error_message) ' +
                'VALUES (?, ?, ?, ?, ?)',
                (url, plugin_name, http_code, error_time, error_message)
            )

            # Also remove from pending since we won't retry
            cur.execute('DELETE FROM pending_urls WHERE url=? AND plugin_name=?',
                        (url, plugin_name))

            sqlCon.commit()
            logger.debug(f"Recorded HTTP {http_code} error for URL: {url}")

        except Exception as e:
            logger.error(f"Error adding HTTP error: {e}")
            raise e

        finally:
            if sqlCon:
                sqlCon.close()
            self.dbAccessSemaphore.release()

    def url_was_attempted(self, sURL: str, pluginName: str) -> bool:
        """
        Check if URL was previously attempted (completed, failed, or HTTP error).

        Args:
            sURL (str): URL to check
            pluginName (str): Plugin name

        Returns:
            bool: True if URL was previously attempted
        """
        searchResult = False
        sqlCon = None
        try:
            sqlCon = lite.connect(self.dbFileName, timeout=self.db_connect_timeout)
            cur = sqlCon.cursor()
            # TODO: speed up slow function
            # Check URL_LIST, FAILED_URLS, and HTTP_ERRORS
            result = cur.execute(
                'SELECT url FROM URL_LIST WHERE url = ? ' +
                'UNION ALL ' +
                'SELECT url FROM FAILED_URLS WHERE url = ? ' +
                'UNION ALL ' +
                'SELECT url FROM HTTP_ERRORS WHERE url = ?',
                (sURL, sURL, sURL)
            )
            rowset = result.fetchall()
            if rowset and len(rowset) > 0:
                searchResult = True

        except Exception as e:
            logger.error(f"{pluginName}: Error searching url: {e}")

        finally:
            if sqlCon:
                sqlCon.close()

        return searchResult

    @retry_db_op(initial_delay=0.1)  # Faster retries
    def removeAlreadyFetchedURLs(self, newURLsList: list, pluginName: str) -> list:
        """
        Remove already fetched URLs (including HTTP errors) from given list.
        Optimized for large URL lists (90K+).

        Args:
            newURLsList (list): URLs to filter
            pluginName (str): Plugin name

        Returns:
            list: Filtered list of URLs not yet attempted
        """
        if not newURLsList:
            return []

        # For very large lists, process in chunks
        if len(newURLsList) > 10000:
            logger.warning(f"{pluginName}: Filtering {len(newURLsList)} URLs in chunks (this is slow!)")
            filtered_urls = []
            chunk_size = 5000

            for i in range(0, len(newURLsList), chunk_size):
                chunk = newURLsList[i:i+chunk_size]
                filtered_chunk = self._filter_urls_chunk(chunk, pluginName)
                filtered_urls.extend(filtered_chunk)
                logger.info(f"{pluginName}: Filtered chunk {i//chunk_size + 1}/{(len(newURLsList)-1)//chunk_size + 1}: "
                            f"{len(filtered_chunk)}/{len(chunk)} URLs remaining")

            return filtered_urls

        # For normal-sized lists, use existing method
        return self._filter_urls_chunk(newURLsList, pluginName)

    def _filter_urls_chunk(self, newURLsList: list, pluginName: str) -> list:
        sqlCon = None
        try:
            self.dbAccessSemaphore.acquire()
            sqlCon = SessionHistory.openConnFromfile(self.dbFileName)
            cur = sqlCon.cursor()

            # Create temporary table with smaller batches
            cur.execute('CREATE TEMP TABLE IF NOT EXISTS temp_urls (url TEXT PRIMARY KEY)')

            # Insert in batches of 1000
            batch_size = 1000
            for i in range(0, len(newURLsList), batch_size):
                batch = newURLsList[i:i+batch_size]
                cur.executemany('INSERT OR IGNORE INTO temp_urls (url) VALUES (?)',
                                [(url,) for url in batch])

            # Optimized filtering query with indexes
            result = cur.execute("""
                                 SELECT url FROM temp_urls
                                 WHERE url NOT IN (SELECT url FROM URL_LIST)
                                   AND url NOT IN (SELECT url FROM FAILED_URLS)
                                   AND url NOT IN (SELECT url FROM HTTP_ERRORS)
                                 """)

            filtered_urls = [row[0] for row in result.fetchall()]

            # Cleanup
            cur.execute('DROP TABLE temp_urls')

            return filtered_urls

        except Exception as e:
            logger.error(f"Error filtering URLs: {e}")
            return newURLsList

        finally:
            if sqlCon:
                sqlCon.close()
            self.dbAccessSemaphore.release()

    @retry_db_op()
    def retrieveTodoURLList(self, pluginName: str) -> list:
        """
        Retrieve pending URLs that haven't failed or returned HTTP errors.

        This is optimized for parallel execution - uses separate connections
        without semaphore since SQLite WAL mode allows concurrent reads.
        """
        URLsFromSQLite = []
        sqlCon = None
        try:
            #  filter from list of tuples: self.pending_urls
            URLsFromSQLite = [urlTuple[1] for urlTuple in self.pending_urls if urlTuple[0] == pluginName]

        except Exception as e:
            logger.error(f"Error retrieving pending URLs for {pluginName}: {e}")
        finally:
            if sqlCon:
                sqlCon.close()

        URLsFromSQLite = deDupeList(URLsFromSQLite)
        logger.info(f'{pluginName}: Identified {len(URLsFromSQLite)} pending URLs from history database.')
        return URLsFromSQLite

    @retry_db_op()
    def addURLsToPendingTable(self, urlList: list, pluginName: str, num_attempts: int = 1):
        """Add URLs to pending table."""
        sqlCon = None
        try:
            self.dbAccessSemaphore.acquire()
            sqlCon = SessionHistory.openConnFromfile(self.dbFileName)
            cur = sqlCon.cursor()
            urlList = deDupeList(urlList)
            data = [(sURL, pluginName, num_attempts) for sURL in urlList]
            cur.executemany('INSERT OR IGNORE INTO pending_urls (url, plugin_name, attempts) VALUES (?, ?, ?)', data)
            sqlCon.commit()
            # Keep the in-memory cache in sync so retrieveTodoURLList() sees the new URLs
            # without needing a DB round-trip.  Only append entries not already cached.
            existing_in_cache = {(t[0], t[1]) for t in self.pending_urls}
            for sURL in urlList:
                if (pluginName, sURL) not in existing_in_cache:
                    self.pending_urls.append((pluginName, sURL))
        except Exception as e:
            logger.error(f"Error adding to pending table: {e}")
            raise e
        finally:
            if sqlCon:
                sqlCon.close()
            self.dbAccessSemaphore.release()

    @retry_db_op()
    def addURLToFailedTable(self, fetchResult, pluginName: str, failTime: datetime):
        """Add URL to failed URLs table."""
        sqlCon = None
        try:
            sURL = fetchResult if isinstance(fetchResult, str) else fetchResult.URL
            self.dbAccessSemaphore.acquire()
            sqlCon = SessionHistory.openConnFromfile(self.dbFileName)
            cur = sqlCon.cursor()

            cur.execute(
                'INSERT INTO FAILED_URLS (url, plugin_name, failedtime) ' +
                'SELECT ?, ?, ? WHERE NOT EXISTS (SELECT 1 FROM FAILED_URLS WHERE url = ?)',
                (sURL, pluginName, failTime, sURL)
            )

            cur.execute('DELETE FROM pending_urls WHERE url=? AND plugin_name=?', (sURL, pluginName))
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
        """Write successfully retrieved URLs to database."""
        if not results_from_queue:
            return 0

        sqlCon = None
        writeCount = 0
        try:
            self.dbAccessSemaphore.acquire()
            sqlCon = SessionHistory.openConnFromfile(self.dbFileName)
            cur = sqlCon.cursor()

            insert_data = []
            urls_to_delete = []

            for resultObj in results_from_queue:
                insert_data.append(resultObj.getAsTuple())
                urls_to_delete.append((resultObj.URL, resultObj.pluginName))

            cur.executemany('INSERT INTO URL_LIST (url, plugin, pubdate, rawsize, datasize) VALUES(?, ?, ?, ?, ?)',
                            insert_data)
            writeCount = len(insert_data)

            cur.executemany('DELETE FROM pending_urls WHERE url=? AND plugin_name=?', urls_to_delete)
            sqlCon.commit()

        except Exception as e:
            logger.error(f"Error saving history: {e}")
            raise e
        finally:
            if sqlCon:
                sqlCon.close()
            self.dbAccessSemaphore.release()

        return writeCount

    def addDupURLToDeleteTbl(self, sURL: str, pluginName: str, pubdate: datetime, filename: str):
        """Add duplicate URL to deleted table."""
        sqlCon = None
        try:
            logger.debug("Add URL to deleted table: Waiting to get db exclusive access...")
            acqResult = self.dbAccessSemaphore.acquire(timeout=30)
            if acqResult is True:
                logger.debug("Adding URL to deleted table for plugin %s: Got exclusive db access.", pluginName)
                sqlCon = SessionHistory.openConnFromfile(self.dbFileName)
                cur = sqlCon.cursor()
                import os
                filename = os.path.basename(filename)
                cur.execute(
                    'INSERT INTO deleted_duplicates (url, plugin, pubdate, filename) VALUES (?, ?, ?, ?)',
                    (sURL, pluginName, pubdate, filename)
                )
                sqlCon.commit()
        except Exception as e:
            logger.error("Error while adding URL to deleted table: %s", e)
        finally:
            if sqlCon:
                sqlCon.close()
            self.dbAccessSemaphore.release()
            logger.debug("Completed adding URL to deleted table for plugin %s: Released exclusive db access.",
                         pluginName)

    def getHTTPErrorStats(self) -> dict:
        """
        Get statistics about HTTP errors.

        Returns:
            dict: Statistics grouped by HTTP code
        """
        sqlCon = None
        stats = {}
        try:
            sqlCon = lite.connect(self.dbFileName, timeout=self.db_connect_timeout)
            cur = sqlCon.cursor()

            result = cur.execute(
                'SELECT http_code, COUNT(*) as count FROM HTTP_ERRORS GROUP BY http_code'
            )

            for row in result.fetchall():
                stats[f"HTTP_{row[0]}"] = row[1]

        except Exception as e:
            logger.error(f"Error getting HTTP error stats: {e}")

        finally:
            if sqlCon:
                sqlCon.close()

        return stats


# End of file
