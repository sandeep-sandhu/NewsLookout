#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: test_mod_en_in_timesofindia.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-06-01
 Purpose: Test for the mod_en_in_timesofindia plugin for the web scraping and news text processing application
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

def testPluginSubClass():
    """Test case Base Plugin Class
    """
    (parentFolder, sourceFolder, testdataFolder) = getAppFolders()
    runDateString = '2021-06-10'
    global app_inst
    global pluginClassInst
    app_inst = getMockAppInstance(parentFolder,
                                  runDateString,
                                  os.path.join(parentFolder, 'conf', 'newslookout.conf'))
    # import application specific modules:
    from plugins.mod_en_in_timesofindia import mod_en_in_timesofindia
    import data_structs
    import session_hist

    pluginClassInst = mod_en_in_timesofindia()
    print(f'Instantiated plugins name: {pluginClassInst.pluginName}')
    assert type(pluginClassInst).__name__ == "mod_en_in_timesofindia", \
        "mod_en_in_timesofindia Plugin was not initialising correctly"
    pluginClassInst.config(app_inst.app_config)
    print(f'Base data directory configured as {pluginClassInst.baseDirName}')
    assert len(pluginClassInst.baseDirName) > 0, "mod_en_in_timesofindia Plugin not configured: baseDirName!"
    assert pluginClassInst.configReader is not None, "mod_en_in_timesofindia Plugin not configured: configReader!"
    assert len(pluginClassInst.urlMatchPatterns) > 0, "mod_en_in_timesofindia Plugin not configured: urlMatchPatterns!"
    # assert len(pluginClassInst.authorMatchPatterns) > 0, "mod_en_in_timesofindia not configured: authorMatchPatterns!"
    assert len(pluginClassInst.dateMatchPatterns) > 0, "mod_en_in_timesofindia Plugin not configured: dateMatchPatterns!"
    print(f'mod_en_in_timesofindia plugin {pluginClassInst.getStatusString()}')
    assert pluginClassInst.getStatusString() == 'State = STATE_GET_URL_LIST', \
        "mod_en_in_timesofindia Plugin status not set correctly!"
    pluginClassInst.initNetworkHelper()
    assert type(pluginClassInst.networkHelper) == network.NetworkFetcher, "mod_en_in_timesofindia network fetcher not init!"
    pluginClassInst.setURLQueue(queue.Queue())
    assert type(pluginClassInst.urlQueue) == queue.Queue, "mod_en_in_timesofindia queue not set!"


def test_fetchDataFromURL():
    """  Test fetchDataFromURL()
    :return:
    """
    global pluginClassInst
    print(f'Instantiated plugins name: {pluginClassInst.pluginName}')
    (parentFolder, sourceFolder, testdataFolder) = getAppFolders()
    import data_structs
    # monkey patch to substitute network fetch.
    pluginClassInst.networkHelper.fetchRawDataFromURL = get_network_substitute_fun(
        pluginClassInst.pluginName,
        testdataFolder,
        file_no=0
    )
    uRLtoFetch = "https://timesofindia.indiatimes.com/blogs/toi-edit-page/as-communal-riots-exploded-in-delhi-elected-representatives-were-missing-on-the-ground-when-residents-needed-them-most/"
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
    assert resultVal.publishDate == '2020-03-14', 'fetchDataFromURL() not parsing published date correctly.'
    assert resultVal.articleID == '134129', 'fetchDataFromURL() not identifying unique ID correctly.'
    assert resultVal.textSize == 1212, 'fetchDataFromURL() not parsing text body correctly.'
    assert resultVal.savedDataFileName == os.path.join('./data', '2020-03-14', 'mod_en_in_timesofindia_134129'), \
        'fetchDataFromURL() not saving parsed data correctly.'
    assert len(resultVal.additionalLinks) == 17, 'fetchDataFromURL() not extracting additional links correctly.'
    if os.path.isfile(resultVal.savedDataFileName + ".json"):
        os.remove(resultVal.savedDataFileName + ".json")
        print(f'Deleted temp JSON file {resultVal.savedDataFileName + ".json"} successfully.')
    if os.path.isfile(resultVal.savedDataFileName + ".html.bz2"):
        os.remove(resultVal.savedDataFileName + ".html.bz2")
        print(f'Deleted temp raw-data file {resultVal.savedDataFileName + ".html.bz2"} successfully.')
    # test alternate logic to extract article body content:
    htmlContent = pluginClassInst.networkHelper.fetchRawDataFromURL(uRLtoFetch, pluginClassInst.pluginName)
    bodytext = pluginClassInst.extractArticleBody(htmlContent)
    print(f'Alternate method extracted body text of size = {len(bodytext)}:\n{bodytext}')
    assert len(bodytext) == 1210, \
        "extractArticleBody() unable to extract article text using alternate (non-newspaper library) method."
    uniqueID = pluginClassInst.extractUniqueIDFromContent(htmlContent, uRLtoFetch)
    print(f'Unique ID from content = {uniqueID}')


def test_extractArchiveURLLinksForDate():
    # TODO: implement this - extractArchiveURLLinksForDate(self, runDate)
    global pluginClassInst
    print(f'Instantiated plugins name: {pluginClassInst.pluginName}')


def extractAuthors():
    # TODO: implement this
    pass


def test_extractIndustries():
    # TODO: implement this
    pass


if __name__ == "__main__":
    testPluginSubClass()

# end of file