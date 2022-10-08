#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: test_session_hist.py
 Application: The NewsLookout Web Scraping Application
 Date: 2020-01-11
 Purpose: Test for the SessionHistory class for the web scraping and news text processing application
 Copyright 2021, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu, sandeep.sandhu@gmx.com


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

# ###################################


# import standard python libraries:
import datetime
import sqlite3
import re
import os
import threading

import pytest

import data_structs
from . import getAppFolders, getMockAppInstance, list_all_files, read_bz2html_file


# ###################################


def test_SessionHistory_init():
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    runDateString = '2021-06-10'
    global app_inst
    global pluginClassInst
    app_inst = getMockAppInstance(parentFolder,
                                  runDateString,
                                  config_file)
    # import application specific modules:
    import data_structs
    import session_hist
    from plugins.mod_en_in_ecotimes import mod_en_in_ecotimes
    dbAccessSemaphore = threading.Semaphore()
    pluginClassInst = mod_en_in_ecotimes()
    print(f'Instantiated plugins name: {pluginClassInst.pluginName}')
    # Initialize object that reads and writes session history of completed URLs into a database
    sessionHistoryDB = session_hist.SessionHistory(
        ":memory:",
        dbAccessSemaphore)
    (urlCount, SQLiteVersion) = sessionHistoryDB.printDBStats()
    assert urlCount == 0, 'printDBStats() is not retrieving statistics from sqlite session history database.'
    print(f'Completed URL count = {urlCount}, SQlite version = {SQLiteVersion}')
    urlList = [
        'https://economictimes.indiatimes.com/blogs/et-editorials/systemic-remedies-beyond-yes-bank/fakeurl',
        'https://economictimes.indiatimes.com/blogs/et-editorials/how-to-really-get-banks-to-lend-more/anotherfake']
    pluginClassInst.addURLsListToQueue(urlList, sessionHistoryDB)
    # check session history db has required structure:
    sqlCon = sessionHistoryDB.openConnFromfile(":memory:")
    assert type(sqlCon) == sqlite3.Connection, 'openConnFromfile() is not able to open database connections.'
    cur = sqlCon.cursor()
    cur.execute('SELECT count(*) from pending_urls')
    data = cur.fetchone()
    print(f'Count of records in table pending_urls = {data[0]}')
    assert data[0]==0, 'SessionHistory object is not able to count pending URLs'
    # check session history db has urls in pending queue:
    todoURLs = sessionHistoryDB.retrieveTodoURLList(pluginClassInst.pluginName)
    print(f'Pending URL listing from session history database = {todoURLs}')

def test_url_was_attempted():
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    dbAccessSemaphore = threading.Semaphore()
    import session_hist
    sessionHistoryDB = session_hist.SessionHistory(
        ":memory:",
        dbAccessSemaphore)
    checkResult = sessionHistoryDB.url_was_attempted('sURL', 'pluginName')
    print(f'url_was_attempted result = {checkResult}')


def test_openConnFromfile():
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()

    # Not using config file defined database since
    # it may be accessed at the same time during test runs
    # testdbFile = app_inst.app_config.completed_urls_datafile
    testdbFile = os.path.join(testdataFolder, 'test22.db')

    dbAccessSemaphore = threading.Semaphore()
    import session_hist
    sessionHistoryDB = session_hist.SessionHistory(
        testdbFile,
        dbAccessSemaphore)
    # start with a clean file:
    if os.path.isfile(testdbFile):
        os.remove(testdbFile)
    sqlConn = sessionHistoryDB.openConnFromfile(testdbFile)
    import sqlite3
    assert type(sqlConn) == sqlite3.Connection, '2. openConnFromfile() is not able to open database connection.'
    cur = sqlConn.cursor()
    cur.execute('select count(url) from url_list')
    data = cur.fetchone()
    assert data[0] == 0, '2. openConnFromfile() is not able to initialise table: url_list.'
    cur.execute('select count(url) from pending_urls')
    data = cur.fetchone()
    assert data[0] == 0, '2. openConnFromfile() is not able to initialise table: pending_urls.'
    cur.execute('select count(url) from FAILED_URLS')
    data = cur.fetchone()
    assert data[0] == 0, '2. openConnFromfile() is not able to initialise table: FAILED_URLS.'
    cur.execute('select count(url) from deleted_duplicates')
    data = cur.fetchone()
    assert data[0] == 0, '2. openConnFromfile() is not able to initialise table: deleted_duplicates.'
    sqlConn.close()
    if os.path.isfile(testdbFile):
        os.remove(testdbFile)
    # make a corrupt database file:
    with open(testdbFile, 'wt') as fp:
        fp.write('+' * 10000)
        fp.close()
    sessionDB2 = session_hist.SessionHistory(
        testdbFile,
        dbAccessSemaphore)
    # database open should fail:
    with pytest.raises(sqlite3.DatabaseError) as exc_info:
        sqlConn = sessionDB2.openConnFromfile(testdbFile)
        (urlCount, SQLiteVersion) = sessionHistoryDB.printDBStats()
    assert exc_info.value.args[0] == "file is not a database",\
        'openConnFromfile() Wrongly opened a corrupt file as a database.'
    ## make a journal file.
    # with open(testdbFile + '-journal', 'wt') as fp:
    #     fp.write('+' * 10000)
    #     fp.close()
    # if os.path.isfile(testdbFile):
    #     os.remove(testdbFile)


def test_addURLsToPendingTable():
    # Test - addURLsToPendingTable()
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    testdbFile = os.path.join(testdataFolder, 'test222.db')
    dbAccessSemaphore = threading.Semaphore()
    import session_hist
    sessionHistoryDB = session_hist.SessionHistory(
        testdbFile,
        dbAccessSemaphore)
    # start with a clean file:
    if os.path.isfile(testdbFile):
        os.remove(testdbFile)
    sqlCon = sessionHistoryDB.openConnFromfile(testdbFile)
    testURLList = ['https://plugin.site1/news1', 'https://plugin.site1/news2']
    sessionHistoryDB.addURLsToPendingTable(testURLList, 'plugin555')

    # verify count using retrieveTodoURLList():
    pendingUrlList = sessionHistoryDB.retrieveTodoURLList('plugin555')
    print(f'URL list fetched back = {pendingUrlList},\n original test list = {testURLList}')
    assert len(pendingUrlList) == len(testURLList),\
        'addURLsToPendingTable() is not able to correctly saving pending URLs.'
    assert 'https://plugin.site1/news1' in pendingUrlList, \
        'addURLsToPendingTable() is not able to correctly saving pending URLs.'
    assert 'https://plugin.site1/news2' in pendingUrlList, \
        'addURLsToPendingTable() is not able to correctly saving pending URLs.'
    assert 'https://plugin.site3/news81' not in pendingUrlList,\
        'retrieveTodoURLList() is not correctly retrieving pending URLs'

    import sqlite3
    cur = sqlCon.cursor()
    # verify count by directly querying in SQL:
    cur.execute('select count(url) from pending_urls')
    data = cur.fetchone()
    print(f'SQL result count of URLs = {data[0]}')
    assert data[0] == 2, '2. addURLsToPendingTable() is not able to save url list.'
    sqlCon.close()
    # before shutdown, clean-up:
    if os.path.isfile(testdbFile):
        os.remove(testdbFile)


def test_addURLToFailedTable():
    # Test - addURLToFailedTable()
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    testdbFile = os.path.join(testdataFolder, 'test33.db')
    dbAccessSemaphore = threading.Semaphore()
    import session_hist
    sessionHistoryDB = session_hist.SessionHistory(
        testdbFile,
        dbAccessSemaphore)
    # start with a clean file:
    if os.path.isfile(testdbFile):
        os.remove(testdbFile)
    sqlCon = sessionHistoryDB.openConnFromfile(testdbFile)
    res1 = data_structs.ExecutionResult('https://site1/failnews11', 202020, 1010, '2010-12-19',
                                        'plugin11', 'file11.json', 'file11.html.bz2', success=False)
    countWritten = sessionHistoryDB.addURLToFailedTable(res1,
                                                        'plugin11',
                                                        datetime.datetime.strptime('2010-12-19','%Y-%m-%d'))
    # verify counts:
    import sqlite3
    cur = sqlCon.cursor()
    cur.execute('select count(*) from FAILED_URLS where plugin_name = ?', ('plugin11',))
    data = cur.fetchone()
    print(f'URL count for plugin11 = {data[0]}')
    assert data[0] == 1, 'addURLToFailedTable() is not correctly saving failed URLs to history database.'
    testList = ['https://site1/failnews11', 'https://site1/news2', 'https://plugin.site1/news4']
    resultList = sessionHistoryDB.removeAlreadyFetchedURLs(testList, 'plugin11')
    print(f'result List after filtering = {resultList}')
    assert 'https://site1/failnews11' not in resultList,\
        'removeAlreadyFetchedURLs() not checking failed URL list correctly'
    sqlCon.close()
    # before shutdown, clean-up:
    if os.path.isfile(testdbFile):
        os.remove(testdbFile)


def test_writeQueueToDB():
    # Test - writeQueueToDB()
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    testdbFile = os.path.join(testdataFolder, 'test44.db')
    dbAccessSemaphore = threading.Semaphore()
    import session_hist
    sessionHistoryDB = session_hist.SessionHistory(
        testdbFile,
        dbAccessSemaphore)
    # start with a clean file:
    if os.path.isfile(testdbFile):
        os.remove(testdbFile)
    sqlCon = sessionHistoryDB.openConnFromfile(testdbFile)
    resultList = []
    res1 = data_structs.ExecutionResult('https://site1/news1', 202020, 1010, '2000-12-20',
                                        'plugin1', 'file1.json', 'file1.html.bz2', success=True)
    resultList.append(res1)
    res2 = data_structs.ExecutionResult('https://site1/news2', 302020, 3010, '2000-12-30',
                                        'plugin2', 'file2.json', 'file2.html.bz2', success=True)
    resultList.append(res2)
    countWritten = sessionHistoryDB.writeQueueToDB(resultList)
    # verify count using printDBStats:
    (countURLs, sqliteversion) = sessionHistoryDB.printDBStats()
    print(f'URL count = {countURLs}, sqlite version = {sqliteversion}')
    assert countURLs == 2, 'printDBStats() is not able to correctly count completed URLs.'
    import sqlite3
    cur = sqlCon.cursor()
    assert sessionHistoryDB.url_was_attempted('https://site1/news1', 'plugin1') == True,\
        'url_was_attempted() is not checking the history database correctly.'
    assert sessionHistoryDB.url_was_attempted('https://site1/news1', 'plugin33') == True, \
        'url_was_attempted() is not checking the history database correctly.'
    # Test - removeAlreadyFetchedURLs()
    testList = ['https://plugin.site1/news33', 'https://site1/news2', 'https://plugin.site1/news4']
    resultList = sessionHistoryDB.removeAlreadyFetchedURLs(testList, 'plugin2')
    print(f'result List after filtering = {resultList}')
    assert 'https://site1/news2' not in resultList, 'removeAlreadyFetchedURLs() not checking completed list correctly'

    # verify count by directly querying in SQL:
    cur.execute('select count(url) from url_list')
    data = cur.fetchone()
    print(f'SQL result count of URLs = {data[0]}')
    assert data[0] == 2, '2. openConnFromfile() is not able to initialise table: url_list.'
    # verify url is correct:
    cur.execute('select url from url_list where plugin = ? and pubdate = ?', ('plugin1','2000-12-20'))
    data = cur.fetchone()
    print(f'URL for plugin1 = {data[0]}')
    assert data[0] == 'https://site1/news1', 'writeQueueToDB() is not correctly saving URL.'
    # verify pubdate is correct:
    cur.execute('select pubdate from url_list where url = ? and plugin = ?', ('https://site1/news2','plugin2'))
    data = cur.fetchone()
    print(f'pubdate for url2 = {data[0]}')
    assert data[0] == datetime.date(2000, 12, 30),\
        'writeQueueToDB() is not correctly saving published date of saved article.'
    sqlCon.close()
    # before shutdown, clean-up:
    if os.path.isfile(testdbFile):
        os.remove(testdbFile)

def test_addDupURLToDeleteTbl():
    # TODO: implement this - addDupURLToDeleteTbl()
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    testdbFile = os.path.join(testdataFolder, 'test55.db')
    dbAccessSemaphore = threading.Semaphore()
    import session_hist
    sessionHistoryDB = session_hist.SessionHistory(
        testdbFile,
        dbAccessSemaphore)
    # start with a clean file:
    if os.path.isfile(testdbFile):
        os.remove(testdbFile)
    testURL = 'https://deleted.site1.com/news567'
    sessionHistoryDB.addDupURLToDeleteTbl(testURL,
                                          'plugin333',
                                          '2017-12-27',
                                          'plugin11_file.json')
    # verify saved table:
    sqlCon = sessionHistoryDB.openConnFromfile(testdbFile)
    import sqlite3
    cur = sqlCon.cursor()
    cur.execute('select url, plugin, pubdate, filename from deleted_duplicates')
    data = cur.fetchone()
    print(f'Deleted URL = {data[0]}, plugin = {data[1]}, pubdate = {data[2]}, filename = {data[3]}')
    assert data[0] == testURL, 'addDupURLToDeleteTbl() is not correctly saving URL to duplicates deleted table.'
    sqlCon.close()
    # before shutdown, clean-up:
    if os.path.isfile(testdbFile):
        os.remove(testdbFile)


if __name__ == "__main__":
    test_writeQueueToDB()

# end of file
