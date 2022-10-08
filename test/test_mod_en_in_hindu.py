#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: test_mod_en_in_hindu.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-06-01
 Purpose: Test for the mod_en_in_hindu plugin for the web scraping and news text processing application
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
    from plugins.mod_en_in_hindu import mod_en_in_hindu
    import data_structs
    import session_hist

    pluginClassInst = mod_en_in_hindu()
    print(f'Instantiated plugins name: {pluginClassInst.pluginName}')
    assert type(pluginClassInst).__name__ == "mod_en_in_hindu", \
        "mod_en_in_hindu Plugin was not initialising correctly"
    pluginClassInst.config(app_inst.app_config)
    print(f'Base data directory configured as {pluginClassInst.baseDirName}')
    assert len(pluginClassInst.baseDirName) > 0, "mod_en_in_hindu Plugin not configured: baseDirName!"
    assert pluginClassInst.configReader is not None, "mod_en_in_hindu Plugin not configured: configReader!"
    assert len(pluginClassInst.urlMatchPatterns) > 0, "mod_en_in_hindu Plugin not configured: urlMatchPatterns!"
    assert len(pluginClassInst.authorMatchPatterns) > 0, "mod_en_in_hindu not configured: authorMatchPatterns!"
    assert len(pluginClassInst.dateMatchPatterns) > 0, "mod_en_in_hindu Plugin not configured: dateMatchPatterns!"
    print(f'mod_en_in_hindu plugin {pluginClassInst.getStatusString()}')
    assert pluginClassInst.getStatusString() == 'State = STATE_GET_URL_LIST', \
        "mod_en_in_hindu Plugin status not set correctly!"
    pluginClassInst.initNetworkHelper()
    assert type(pluginClassInst.networkHelper) == network.NetworkFetcher, "mod_en_in_hindu network fetcher not init!"
    pluginClassInst.setURLQueue(queue.Queue())
    assert type(pluginClassInst.urlQueue) == queue.Queue, "mod_en_in_hindu queue not set!"


def test_fetchDataFromURL():
    """  Test fetchDataFromURL()
    :return:
    """
    global pluginClassInst
    print(f'Instantiated plugins name: {pluginClassInst.pluginName}')
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    import data_structs
    # monkey patch to substitute network fetch.
    pluginClassInst.networkHelper.fetchRawDataFromURL = get_network_substitute_fun(
        pluginClassInst.pluginName,
        testdataFolder,
        file_no=0
    )
    uRLtoFetch = "https://www.thehindu.com/business/budget/16-point-action-plan-for-agriculture-evokes-mixed-response/article30713792.ece"
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
    assert resultVal.articleID == '30713792', 'fetchDataFromURL() not identifying unique ID correctly.'
    assert resultVal.textSize == 2791, 'fetchDataFromURL() not parsing text body correctly.'
    assert resultVal.savedDataFileName == os.path.join(app_inst.app_config.data_dir, '2020-02-01', 'mod_en_in_hindu_30713792'), \
        'fetchDataFromURL() not saving parsed data correctly.'
    assert len(resultVal.additionalLinks) == 45, 'fetchDataFromURL() not extracting additional links correctly.'
    if os.path.isfile(resultVal.savedDataFileName + ".json"):
        os.remove(resultVal.savedDataFileName + ".json")
        print(f'Deleted temp JSON file {resultVal.savedDataFileName + ".json"} successfully.')
    if os.path.isfile(resultVal.savedDataFileName + ".html.bz2"):
        os.remove(resultVal.savedDataFileName + ".html.bz2")
        print(f'Deleted temp raw-data file {resultVal.savedDataFileName + ".html.bz2"} successfully.')
    # test alternate logic to extract article body content:
    htmlContent = pluginClassInst.networkHelper.fetchRawDataFromURL(uRLtoFetch, pluginClassInst.pluginName)
    bodytext = pluginClassInst.extractArticleBody(htmlContent)
    print(f'Alternate method extracted body text of size = {len(bodytext)}')
    assert len(bodytext) == 0, \
        "extractArticleBody() unable to extract article text using alternate (non-newspaper library) method."


def test_extractArchiveURLLinksForDate():
    # TODO: implement this - extractArchiveURLLinksForDate(self, runDate)
    global pluginClassInst
    print(f'Instantiated plugins name: {pluginClassInst.pluginName}')


def test_extractUniqueIDFromURL():
    global pluginClassInst
    print(f'Instantiated plugins name: {pluginClassInst.pluginName}')
    uRLtoFetch = "https://www.thehindu.com/business/budget/16-point-action-plan-for-agriculture-evokes-mixed-response/article30713792.ece"
    uniqueID = pluginClassInst.extractUniqueIDFromURL(uRLtoFetch)
    assert uniqueID == '30713792', "extractUniqueIDFromURL() is not correctly identifying article unique ID"


def extractAuthors():
    # TODO: implement this
    pass


def test_extractIndustries():
    # TODO: implement this
    pass


if __name__ == "__main__":
    testPluginSubClass()

# end of file
