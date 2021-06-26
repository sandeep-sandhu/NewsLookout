#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: test_scraper_app.py
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

####################################

# import standard python libraries:
import sys
import os
import logging
from datetime import datetime

# ###################################

def getMockAppInstance():
    testfolder = os.path.dirname(os.path.realpath(__file__))
    parentFolder = os.path.dirname(testfolder)
    sourceFolder = os.path.join(parentFolder, 'src')
    sys.path.append(sourceFolder)

    from scraper_app import NewsLookout
    mock_sys_argv = ['python.exe', '-c', os.path.join(parentFolder, 'conf', 'newslookout.conf'),
                     '-d', '2021-06-10']
    # instantiate the main application class
    app_inst = NewsLookout()
    app_inst.config(mock_sys_argv)
    return(app_inst)

def test_appInitRunDate():
    app_inst = getMockAppInstance()
    print('Initialised application config =', app_inst.app_config)
    assert app_inst.app_config.rundate == datetime.strptime('2021-06-10', '%Y-%m-%d'), 'The rundate not set correctly'

def test_appInitConfigFile():
    app_inst = getMockAppInstance()
    print('Initialised application config file =', app_inst.app_config.config_file)
    assert app_inst.app_config.config_file.endswith('newslookout.conf'), 'The config file was not setup correctly'

def test_isqueuemanager_initialised():
    from queue_manager import QueueManager
    app_inst = getMockAppInstance()
    assert isinstance(app_inst.app_queue_manager, QueueManager), 'The queue manager was not initialised correctly'

def test_isqueuemanager_config():
    app_inst = getMockAppInstance()
    app_inst.app_queue_manager.config(app_inst.app_config)
    from session_hist import SessionHistory
    assert isinstance(app_inst.app_queue_manager.sessionHistoryDB, SessionHistory),\
        'The session history was not initialised correctly'

def test_fetchCycleTime_config():
    app_inst = getMockAppInstance()
    app_inst.app_queue_manager.config(app_inst.app_config)
    assert app_inst.app_queue_manager.fetchCycleTime > 60, 'Queue manager: fetchCycleTime was not configured correctly.'

def test_pidfile_add():
    app_inst = getMockAppInstance()
    app_inst.remove_pid_file()
    app_inst.set_pid_file()
    assert os.path.isfile(app_inst.app_config.pid_file)==True, 'The PID file was not created.'
    app_inst.remove_pid_file()

def test_pidfile_remove():
    app_inst = getMockAppInstance()
    app_inst.set_pid_file()
    app_inst.remove_pid_file()
    assert os.path.isfile(app_inst.app_config.pid_file)==False, 'The PID file was not removed.'

def checkModLoaded():
    # TODO: implement this
    pass
    # app_inst.app_queue_manager.initPlugins()
    # isinstance(app_inst.app_queue_manager.pluginNameToObjMap, dict)
    #testMod = app_inst.app_queue_manager.pluginNameToObjMap['mod_in_gdelt']

if __name__ == "__main__":
    # run all tests
    test_appInitRunDate()
    test_appInitConfigFile()

# end of file
