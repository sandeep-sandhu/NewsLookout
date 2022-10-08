#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: test_base_plugin.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-06-01
 Purpose: Test for the main class for the web scraping and news text processing application
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
import sys
import os
from datetime import datetime

import network
import queue
import threading
import logging


from . import getAppFolders, getMockAppInstance
from . import list_all_files, read_bz2html_file
from . import altfetchRawDataFromURL, get_network_substitute_fun

# ###################################

global pluginClassInst
global app_inst

logger = logging.getLogger(__name__)


def test_getFullFilePathsInDir():
    # TODO: implement this - getFullFilePathsInDir()
    pass


def test_identifyFilesForDate():
    # TODO: implement this
    pass


def test_filterInvalidURLs():
    # TODO: implement this
    pass


def test_extractArchiveURLLinksForDate():
    # TODO: implement this
    pass


def test_getLinksRecursively():
    # TODO: implement this
    pass


def test_extractArticleListFromMainURL():
    # TODO: implement this
    pass


def test_extractLinksFromURLList():
    # TODO: implement this
    pass

def test_downloadDataArchive():
    # TODO: implement this
    pass


def test_clearQueue():
    pass




def testPluginSubClass():
    """Test case Base Plugin Class
    """
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    runDateString = '2021-06-10'
    global app_inst
    global pluginClassInst
    app_inst = getMockAppInstance(parentFolder,
                                  runDateString,
                                  config_file)
    # import application specific modules:
    from plugins.mod_en_in_ecotimes import mod_en_in_ecotimes
    import data_structs
    import session_hist

    pluginClassInst = mod_en_in_ecotimes()
    print(f'Instantiated plugins name: {pluginClassInst.pluginName}')
    assert type(pluginClassInst).__name__ == "mod_en_in_ecotimes",\
        "mod_en_in_ecotimes Plugin was not initialising correctly"
    pluginClassInst.config(app_inst.app_config)
    print(f'Base data directory configured as {pluginClassInst.baseDirName}')
    assert len(pluginClassInst.baseDirName) > 0, "mod_en_in_ecotimes Plugin not configured: baseDirName!"
    assert pluginClassInst.configReader is not None, "mod_en_in_ecotimes Plugin not configured: configReader!"
    assert len(pluginClassInst.urlMatchPatterns) > 0, "mod_en_in_ecotimes Plugin not configured: urlMatchPatterns!"
    assert len(pluginClassInst.authorMatchPatterns) > 0, "mod_en_in_ecotimes not configured: authorMatchPatterns!"
    assert len(pluginClassInst.dateMatchPatterns) > 0, "mod_en_in_ecotimes Plugin not configured: dateMatchPatterns!"
    print(f'mod_en_in_ecotimes plugin {pluginClassInst.getStatusString()}')
    assert pluginClassInst.getStatusString() == 'State = STATE_GET_URL_LIST',\
        "mod_en_in_ecotimes Plugin status not set correctly!"
    pluginClassInst.initNetworkHelper()
    assert type(pluginClassInst.networkHelper) == network.NetworkFetcher, "mod_en_in_ecotimes network fetcher not init!"
    pluginClassInst.setURLQueue(queue.Queue())
    assert type(pluginClassInst.urlQueue) == queue.Queue, "mod_en_in_ecotimes queue not set!"
    dbAccessSemaphore = threading.Semaphore()
    # Initialize object that reads and writes session history of completed URLs into a database
    sessionHistoryDB = session_hist.SessionHistory(
        ":memory:",
        dbAccessSemaphore)
    (urlCount, SQLiteVersion) = sessionHistoryDB.printDBStats()
    print(f'Completed URL count = {urlCount}, SQlite version = {SQLiteVersion}')
    # try getting html from test data directory:
    _ = read_bz2html_file("file1.html.bz2")
    urlList = [
        'https://economictimes.indiatimes.com/blogs/et-editorials/systemic-remedies-beyond-yes-bank/fakeurl',
        'https://economictimes.indiatimes.com/blogs/et-editorials/how-to-really-get-banks-to-lend-more/anotherfake']
    pluginClassInst.addURLsListToQueue(urlList, sessionHistoryDB)
    # check session history db has required structure:
    sqlCon = sessionHistoryDB.openConnFromfile(":memory:")
    cur = sqlCon.cursor()
    cur.execute('SELECT count(*) from pending_urls')
    data = cur.fetchone()
    print(f'Count of records in table pending_urls = {data[0]}')
    # check session history db has urls in pending queue:
    todoURLs = sessionHistoryDB.retrieveTodoURLList(pluginClassInst.pluginName)
    print(f'Pending URL listing from session history database = {todoURLs}')
    print(f'Plugin queue size = {pluginClassInst.getQueueSize()}')
    assert pluginClassInst.urlQueue.qsize() == 2, "mod_en_in_ecotimes - Cannot add to queue!"
    assert pluginClassInst.getQueueSize() == 1, "mod_en_in_ecotimes - Cannot get proper queue size!"
    retrievedItem = pluginClassInst.getNextItemFromFetchQueue()
    assert retrievedItem == urlList[0], "mod_en_in_ecotimes - Cannot retrieve from queue!"
    assert pluginClassInst.urlQueue.qsize() == 1, "mod_en_in_ecotimes - Cannot get queue size!"
    pluginClassInst.putQueueEndMarker()
    assert pluginClassInst.getNextItemFromFetchQueue() == urlList[1], "mod_en_in_ecotimes - Cannot retrieve item 2!"
    assert pluginClassInst.getNextItemFromFetchQueue() == None, "mod_en_in_ecotimes - Cannot retrieve queue sentinel!"
    assert pluginClassInst.pluginState == data_structs.PluginTypes.STATE_FETCH_CONTENT,\
        "mod_en_in_ecotimes - Queue sentinel marker did not set the correct state"
    assert pluginClassInst.isQueueEmpty() is True, "mod_en_in_ecotimes - Queue is not empty!"
    datePath = pluginClassInst.identifyDataPathForRunDate(pluginClassInst.baseDirName, runDateString)
    print(f'Path for date {runDateString} calculated as: {datePath}')
    assert datePath == os.path.join(pluginClassInst.baseDirName, runDateString),\
        "mod_en_in_ecotimes - path for date not computed correctly!"


def test_filterNonContentURLs():
    # TODO: implement this
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    sys.path.append(sourceFolder)
    app_inst = getMockAppInstance(parentFolder,
                                  '2021-06-10',
                                  config_file)
    from plugins.mod_en_in_ecotimes import mod_en_in_ecotimes
    pluginClassInst = mod_en_in_ecotimes()
    longURL1 = "https://economictimes.indiatimes.com/industry/banking/finance/pnb-housing-finance-carlyle-deal-" +\
               "psbs-told-to-tick-all-boxes-before-stake-sale-in-units/articleshow/84356047.cms"
    longURL2 = "https://economictimes.indiatimes.com/news/politics-and-nation/Earth-Sciences-Ministry-plans-major-" +\
               "social-media-outreach/articleshow/52923597.cms"
    urlList = ['https://economictimes.indiatimes.com/etlatestnews.cms?track900=1234&abcd=defg',
               'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=b',
               longURL1,
               longURL2]
    print('Input list:\n', urlList)
    filteredURLList = pluginClassInst.filterNonContentURLs(urlList)
    print('Output list:\n', filteredURLList)
    assert longURL1 in filteredURLList, "filterNonContentURLs() is not filtering non content URL correctly."
    assert longURL2 in filteredURLList, "filterNonContentURLs() is not filtering non content URL correctly."
    assert 'https://economictimes.indiatimes.com/etlatestnews.cms?track900=1234&abcd=defg' not in filteredURLList,\
        "filterNonContentURLs() is not filtering non content URL correctly."
    assert 'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=b' not in filteredURLList, \
        "filterNonContentURLs() is not filtering non content URL correctly."


def test_getArticlesListFromRSS():
    # Test getArticlesListFromRSS()
    global pluginClassInst
    print(f'Instantiated plugin name: {pluginClassInst.pluginName}')
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    listofFiles = [i for i in list_all_files(testdataFolder) if i.find('mod_en_in_ecotimes') >= 0 and i.find('.xml')> 0]
    rssFileName = listofFiles[0]
    # monkey patch to prevent network fetch!
    pluginClassInst.networkHelper.fetchRawDataFromURL = altfetchRawDataFromURL
    pluginClassInst.all_rss_feeds = [rssFileName]
    resultList = pluginClassInst.getArticlesListFromRSS(pluginClassInst.all_rss_feeds)
    url1 = 'https://economictimes.indiatimes.com/news/science/covid-19-delta-variant-may-breach-vaccine-shield/' +\
           'articleshow/83889378.cms'
    url47 = 'https://economictimes.indiatimes.com/jobs/epfo-adds-1-27-million-subscribers-in-april/' +\
            'articleshow/83844142.cms'
    print(f'Extracted {len(resultList)} URLs from RSS file.')
    assert resultList[0] == url1, 'getArticlesListFromRSS() could not extract first news links from RSS file.'
    assert resultList[46] == url47, 'getArticlesListFromRSS() could not extract last news links from RSS file.'


def test_loadDocument():
    global pluginClassInst
    print(f'Instantiated plugins name: {pluginClassInst.pluginName}')
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    jsonFileName = os.path.join(testdataFolder, 'test_readFromJSON.json')
    document = pluginClassInst.loadDocument(jsonFileName)
    assert document.getFileName() == jsonFileName, "loadDocument() could not set the proper file name."



def test_extractUniqueIDFromURL():
    global pluginClassInst
    print(f'Instantiated plugins name: {pluginClassInst.pluginName}')
    uRLtoFetch = "https://economictimes.indiatimes.com/markets/expert-view/a-reasonable-budget-but-still-unclear-on-" + \
                 "fiscal-deficit-front-swaminathan-aiyar/articleshow/73837853.cms"
    uniqueID = pluginClassInst.extractUniqueIDFromURL(uRLtoFetch)
    assert uniqueID == '73837853', "extractUniqueIDFromURL() is not correctly identifying article unique ID"



def test_fetchDataFromURL():
    """  Test fetchDataFromURL()
    :return:
    """
    global pluginClassInst
    global app_inst
    print(f'Instantiated plugins name: {pluginClassInst.pluginName}')
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    import data_structs
    # monkey patch to substitute network fetch.
    pluginClassInst.networkHelper.fetchRawDataFromURL = get_network_substitute_fun(
        pluginClassInst.pluginName,
        testdataFolder,
        file_no=0
        )
    uRLtoFetch = "https://economictimes.indiatimes.com/markets/expert-view/a-reasonable-budget-but-still-unclear-on-" +\
                 "fiscal-deficit-front-swaminathan-aiyar/articleshow/73837853.cms"
    logging.getLogger().setLevel(logging.DEBUG)
    resultVal = pluginClassInst.fetchDataFromURL(uRLtoFetch, '1')
    print(f'Fetched data successfully? {resultVal.wasSuccessful}')
    print(f'HTML Data Size: {resultVal.rawDataSize}, textSize: {resultVal.textSize}, URL: {resultVal.URL}')
    print(f'Saved File Name: {resultVal.savedDataFileName}, ID: {resultVal.articleID}, plugin: {resultVal.pluginName}')
    print(f'Count of additional links: {len(resultVal.additionalLinks)}, Publish Date: {resultVal.publishDate}')
    print('Additional links:')
    for j, addl_url in enumerate(resultVal.additionalLinks):
        print(f'{j+1}:\t{addl_url}')
    assert type(resultVal) == data_structs.ExecutionResult, 'fetchDataFromURL() not returning exec result correctly.'
    assert resultVal.wasSuccessful is True, 'fetchDataFromURL() did not complete successfully'
    assert resultVal.pluginName == pluginClassInst.pluginName, 'fetchDataFromURL() not parsing text body correctly.'
    assert resultVal.publishDate == datetime.strptime('2020-02-01','%Y-%m-%d'), 'fetchDataFromURL() not parsing published date correctly.'
    assert resultVal.articleID == '73837853', 'fetchDataFromURL() not identifying unique ID correctly.'
    assert resultVal.textSize == 2687, 'fetchDataFromURL() not parsing text body correctly.'
    assert resultVal.savedDataFileName == os.path.join(app_inst.app_config.data_dir, '2020-02-01', 'mod_en_in_ecotimes_73837853'),\
        'fetchDataFromURL() not saving parsed data correctly.'
    assert len(resultVal.additionalLinks) == 40, 'fetchDataFromURL() not extracting additional links correctly.'
    if os.path.isfile(resultVal.savedDataFileName + ".json"):
        os.remove(resultVal.savedDataFileName + ".json")
        print(f'Deleted temp JSON file {resultVal.savedDataFileName + ".json"} successfully.')
    if os.path.isfile(resultVal.savedDataFileName + ".html.bz2"):
        os.remove(resultVal.savedDataFileName + ".html.bz2")
        print(f'Deleted temp raw-data file {resultVal.savedDataFileName + ".html.bz2"} successfully.')


if __name__ == "__main__":
    testPluginSubClass()

# end of file
