#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: test_mod_en_in_ecotimes.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-06-01
 Purpose: Test for the mod_en_in_ecotimes plugin for the web scraping and news text processing application
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
    """Test instantiation of this Plugin object
    """
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    runDateString = '2021-06-10'
    global app_inst
    global pluginClassInst
    app_inst = getMockAppInstance(parentFolder,
                                  runDateString,
                                  config_file)
    # import application specific modules:
    from plugins.mod_dataprep import mod_dataprep
    import data_structs
    import session_hist
    # instantiate the plugin object:
    pluginClassInst = mod_dataprep()
    print(f'Instantiated plugin name: {pluginClassInst.pluginName}')
    assert type(pluginClassInst).__name__ == "mod_dataprep", \
        "mod_dataprep Plugin was not initialising correctly"
    pluginClassInst.config(app_inst.app_config)
    dbAccessSemaphore = threading.Semaphore()
    sessionHistoryDB = session_hist.SessionHistory(
        ":memory:",
        dbAccessSemaphore)
    pluginClassInst.additionalConfig(sessionHistoryDB)


def test_processDataObj():
    """  Test processDataObj()
    :return:
    """
    global pluginClassInst
    print(f'Instantiated plugins name: {pluginClassInst.pluginName}')
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()


if __name__ == "__main__":
    testPluginSubClass()

# end of file
