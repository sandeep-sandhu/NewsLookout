#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: test_mod_in_gdelt.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-06-01
 Purpose: Test for the mod_in_gdelt plugin for the web scraping and news text processing application
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
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    runDateString = '2021-06-10'
    global app_inst
    global pluginClassInst
    app_inst = getMockAppInstance(parentFolder,
                                  runDateString,
                                  config_file)
    # import application specific modules:
    from plugins.mod_in_gdelt import mod_in_gdelt
    import data_structs
    import session_hist

    pluginClassInst = mod_in_gdelt()
    print(f'Instantiated plugins name: {pluginClassInst.pluginName}')
    assert type(pluginClassInst).__name__ == "mod_in_gdelt", \
        "mod_in_gdelt Plugin was not initialising correctly"
    pluginClassInst.config(app_inst.app_config)
    print(f'Base data directory configured as {pluginClassInst.baseDirName}')
    assert len(pluginClassInst.baseDirName) > 0, "mod_in_gdelt Plugin not configured: baseDirName!"
    assert pluginClassInst.configReader is not None, "mod_in_gdelt Plugin not configured: configReader!"
    print(f'mod_in_gdelt plugin {pluginClassInst.getStatusString()}')
    assert pluginClassInst.getStatusString() == 'State = STATE_GET_URL_LIST', \
        "mod_in_gdelt Plugin status not set correctly!"
    pluginClassInst.initNetworkHelper()
    assert type(pluginClassInst.networkHelper) == network.NetworkFetcher, "mod_in_gdelt network fetcher not init!"
    pluginClassInst.setURLQueue(queue.Queue())
    assert type(pluginClassInst.urlQueue) == queue.Queue, "mod_in_gdelt queue not set!"


def test_prepare_url_datadir_for_date():
    # Test prepare_url_datadir_for_date(rundate_obj)
    global pluginClassInst
    print(f'Instantiated plugins name: {pluginClassInst.pluginName}')
    date_obj_1 = datetime.datetime.strptime('2021-03-02', '%Y-%m-%d')
    resultURL1, resultDir1 = pluginClassInst.prepare_url_datadir_for_date(date_obj_1)
    print(f'resultURL1 = {resultURL1} and resultDir1 = {resultDir1}')
    expectedURL1 = 'http://data.gdeltproject.org/events/20210228.export.CSV.zip'
    assert resultURL1 == expectedURL1, 'prepare_url_datadir_for_date() not preparing GDELT URL1 correctly.'
    assert resultDir1 == os.path.join(app_inst.app_config.data_dir, '2021-02-28'),\
        'prepare_url_datadir_for_date() not calculating data directory correctly.'
    date_obj_2 = datetime.datetime.strptime('2020-03-02', '%Y-%m-%d')
    resultURL2, resultDir2 = pluginClassInst.prepare_url_datadir_for_date(date_obj_2)
    print(f'resultURL2 = {resultURL2} and resultDir2 = {resultDir2}')
    expectedURL2 = 'http://data.gdeltproject.org/events/20200229.export.CSV.zip'
    assert resultURL2 == expectedURL2, 'prepare_url_datadir_for_date() not preparing GDELT URL2 correctly.'
    date_obj_3 = datetime.datetime.strptime('2020-01-02', '%Y-%m-%d')
    resultURL3, resultDir3 = pluginClassInst.prepare_url_datadir_for_date(date_obj_3)
    print(f'resultURL3 = {resultURL3} and resultDir3 = {resultDir3}')
    expectedURL3 = 'http://data.gdeltproject.org/events/20191231.export.CSV.zip'
    assert resultURL3 == expectedURL3, 'prepare_url_datadir_for_date() not preparing GDELT URL3 correctly.'


def test_extract_csvlist_from_archive():
    # Test extract_csvlist_from_archive(archive_bytes, dataDirForDate)
    global pluginClassInst
    print(f'Instantiated plugins name: {pluginClassInst.pluginName}')
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    test_files_list = [i for i in list_all_files(testdataFolder)
                       if i.find(pluginClassInst.pluginName) > -1 and i.endswith('.zip')]
    print(f'Test file: {test_files_list[0]}')
    date_obj = datetime.datetime.strptime('2021-02-03', '%Y-%m-%d')
    resultURL, dataDirForDate = pluginClassInst.prepare_url_datadir_for_date(date_obj)
    with open(test_files_list[0], 'rb') as fp:
        zipcontent = fp.read()
    csv_filenames = pluginClassInst.extract_csvlist_from_archive(zipcontent, dataDirForDate)
    print(f'Extracted data files from archive: {csv_filenames}')
    assert len(csv_filenames) == 1, 'extract_csvlist_from_archive() extracted incorrect number of data files'
    assert os.path.join(app_inst.app_config.data_dir, '2021-02-01','mod_in_gdelt_20210203.txt') == csv_filenames[0],\
        'extract_csvlist_from_archive() incorrectly extracting data files from archive'
    run_extract_urls_from_csv(csv_filenames[0])
    if os.path.isfile(csv_filenames[0]):
        os.remove(csv_filenames[0])


def run_extract_urls_from_csv(csv_filename):
    # Test extract_urls_from_csv(csv_filename, country_code='IN')
    global pluginClassInst
    print(f'Instantiated plugins name: {pluginClassInst.pluginName}')
    urlList = pluginClassInst.extract_urls_from_csv(csv_filename, country_code='IN')
    print(f'Extracted URLS:\n{urlList}')
    assert len(urlList) == 215, 'extract_urls_from_csv() incorrectly extracting URLs from GDELT decompressed file.'
    expectedURL = 'https://timesofindia.indiatimes.com/city/surat/textile-traders-claim-rs-2000-cr-loss-in-75-days/' +\
                  'articleshow/80655048.cms'
    assert expectedURL in urlList, 'extract_urls_from_csv() did not extract expected URL from GDELT decompressed file.'


if __name__ == "__main__":
    testPluginSubClass()

# end of file
