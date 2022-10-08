#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: test_data_structs.py
 Application: The NewsLookout Web Scraping Application
 Date: 2020-01-11
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

from data_structs import QueueStatus
from . import getAppFolders, getMockAppInstance, list_all_files, read_bz2html_file


# ###################################


def test_decodeNameFromIntVal():
    from data_structs import PluginTypes
    assert PluginTypes.decodeNameFromIntVal(10) == 'STATE_GET_URL_LIST',\
        'test_decodeNameFromIntVal() is not decoding types into names correctly'
    assert PluginTypes.decodeNameFromIntVal(20) == 'STATE_FETCH_CONTENT', \
        'test_decodeNameFromIntVal() is not decoding types into names correctly'
    assert PluginTypes.decodeNameFromIntVal(80) == 'STATE_STOPPED', \
        'test_decodeNameFromIntVal() is not decoding types into names correctly'


def test_ExecutionResult_init():
    from data_structs import ExecutionResult
    testObject = ExecutionResult('https://www.site.com/',
                                 41410,
                                 2120,
                                 '2020-02-27',
                                 'some_plugin',
                                 dataFileName='some_json',
                                 rawDataFile='some.html.bz2',
                                 success=True,
                                 additionalLinks=['URL/one', 'URL/two'])
    expectedTuple = ('https://www.site.com/', 'some_plugin', '2020-02-27', 41410, 2120)
    assert expectedTuple == testObject.getAsTuple(), 'ExecutionResult is not initialising correctly'
    assert 'some_json' == testObject.savedDataFileName, 'ExecutionResult is not initialising correctly'
    assert testObject.wasSuccessful == True, 'ExecutionResult is not initialising correctly'
    assert testObject.additionalLinks == ['URL/one', 'URL/two'], 'ExecutionResult is not initialising correctly'


def test_QueueStatus_init():
    # TODO: implement this - instantiation of object of type QueueStatus()
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    runDateString = '2021-06-10'
    global app_inst
    global pluginClassInst
    app_inst = getMockAppInstance(parentFolder,
                                  runDateString,
                                  config_file)
    app_inst.app_queue_manager.config(app_inst.app_config)
    qstatusobj = QueueStatus(app_inst.app_queue_manager)
    qstatusobj.updateStatus()
    assert type(qstatusobj) == QueueStatus, 'QueueStatus is not initialising correctly'


if __name__ == "__main__":
    test_decodeNameFromIntVal()

# end of file
